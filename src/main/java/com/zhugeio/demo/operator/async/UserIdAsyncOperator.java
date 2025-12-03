package com.zhugeio.demo.operator.async;

import com.zhugeio.demo.KvrocksClient;
import com.zhugeio.demo.model.IdOutput;
import com.zhugeio.demo.utils.SnowflakeIdGenerator;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 用户ID映射算子 - 优雅关闭版本
 *
 * 修复点:
 * 1. ✅ 使用ConcurrentHashMap追踪未完成的写入
 * 2. ✅ O(1)时间复杂度的add/remove操作
 * 3. ✅ close()时等待所有未完成的写入
 * 4. ✅ 防止连接过早关闭导致数据丢失
 */
public class UserIdAsyncOperator extends RichAsyncFunction<IdOutput, IdOutput> {

    private static final Logger LOG = LoggerFactory.getLogger(UserIdAsyncOperator.class);

    private transient KvrocksClient kvrocks;
    private transient SnowflakeIdGenerator idGenerator;

    // ✅ 使用ConcurrentHashMap追踪未完成的写入
    private transient ConcurrentHashMap<Long, CompletableFuture<Void>> pendingWrites;
    private transient AtomicLong futureIdGenerator;

    private final String kvrocksHost;
    private final int kvrocksPort;
    private final boolean kvrocksCluster;

    public UserIdAsyncOperator() {
        this(null, 0, true);
    }

    public UserIdAsyncOperator(String kvrocksHost, int kvrocksPort) {
        this(kvrocksHost, kvrocksPort, true);
    }

    public UserIdAsyncOperator(String kvrocksHost, int kvrocksPort, boolean kvrocksCluster) {
        this.kvrocksHost = kvrocksHost;
        this.kvrocksPort = kvrocksPort;
        this.kvrocksCluster = kvrocksCluster;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        kvrocks = new KvrocksClient(kvrocksHost, kvrocksPort, kvrocksCluster);
        kvrocks.init();

        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        int totalSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();

        if (totalSubtasks > 256) {
            throw new RuntimeException("UserId算子最多支持256个并行度,当前: " + totalSubtasks);
        }

        int workerId = 256 + subtaskIndex;
        idGenerator = new SnowflakeIdGenerator(workerId);

        // ✅ 初始化ConcurrentHashMap
        pendingWrites = new ConcurrentHashMap<>(1024);
        futureIdGenerator = new AtomicLong(0);

        LOG.info("[UserId算子-{}] 雪花算法初始化成功, workerId={}", subtaskIndex, workerId);
    }

    @Override
    public void asyncInvoke(IdOutput input, ResultFuture<IdOutput> resultFuture) {
        if (StringUtils.isBlank(input.getCuid())) {
            input.setZgUserId(null);
            input.setNewUser(false);
            resultFuture.complete(Collections.singleton(input));
            return;
        }

        String cacheKey = "u:" + input.getAppId() + ":" + input.getCuid();

        kvrocks.asyncGet(cacheKey)
                .thenCompose(zgUidStr -> {
                    if (zgUidStr != null) {
                        Long zgUserId = Long.parseLong(zgUidStr);
                        return CompletableFuture.completedFuture(new UserIdResult(zgUserId, false));
                    } else {
                        Long newUserId = idGenerator.nextId();

                        // ✅ 生成唯一ID作为key
                        Long futureId = futureIdGenerator.incrementAndGet();

                        CompletableFuture<Void> writeFuture = kvrocks.asyncSet(cacheKey, String.valueOf(newUserId))
                                .whenComplete((v, throwable) -> {
                                    // ✅ O(1)时间复杂度删除
                                    pendingWrites.remove(futureId);

                                    if (throwable != null) {
                                        LOG.error("[UserId算子-{}] UserId写入失败: {}",
                                                getRuntimeContext().getIndexOfThisSubtask(),
                                                cacheKey, throwable);
                                    }
                                });

                        // ✅ O(1)时间复杂度添加
                        pendingWrites.put(futureId, writeFuture);

                        return CompletableFuture.completedFuture(new UserIdResult(newUserId, true));
                    }
                })
                .whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        resultFuture.completeExceptionally(throwable);
                    } else {
                        input.setZgUserId(result.userId);
                        input.setNewUser(result.isNew);
                        resultFuture.complete(Collections.singleton(input));
                    }
                });
    }

    @Override
    public void close() throws Exception {
        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();

        LOG.info("[UserId算子-{}] 开始关闭，等待未完成的写入...", subtaskIndex);

        // ✅ 等待所有未完成的写入
        if (pendingWrites != null && !pendingWrites.isEmpty()) {
            long startWait = System.currentTimeMillis();
            int pendingCount = pendingWrites.size();

            LOG.info("[UserId算子-{}] 发现 {} 个未完成的写入操作，开始等待...",
                    subtaskIndex, pendingCount);

            try {
                CompletableFuture<Void> allWrites = CompletableFuture.allOf(
                        pendingWrites.values().toArray(new CompletableFuture[0])
                );
                allWrites.get(30, TimeUnit.SECONDS);

                long waitTime = System.currentTimeMillis() - startWait;
                LOG.info("[UserId算子-{}] ✅ 所有未完成的写入已完成，等待时间: {} ms",
                        subtaskIndex, waitTime);

            } catch (Exception e) {
                long waitTime = System.currentTimeMillis() - startWait;
                LOG.error("[UserId算子-{}] ⚠️  等待写入完成超时或失败，等待时间: {} ms, 剩余未完成: {}",
                        subtaskIndex, waitTime, pendingWrites.size(), e);
            }
        }

        if (kvrocks != null) {
            kvrocks.shutdown();
            LOG.info("[UserId算子-{}] KVRocks连接已关闭", subtaskIndex);
        }
    }

    private static class UserIdResult {
        final Long userId;
        final boolean isNew;

        UserIdResult(Long userId, boolean isNew) {
            this.userId = userId;
            this.isNew = isNew;
        }
    }
}