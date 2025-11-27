package com.zhugeio.demo.operator.async;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.zhugeio.demo.KvrocksClient;
import com.zhugeio.demo.model.IdOutput;
import com.zhugeio.demo.utils.SnowflakeIdGenerator;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * 用户ID映射算子 (雪花算法版本)
 *
 * 优化点:
 * 1. ✅ 使用雪花算法生成ID,无需同步调用KVRocks incr
 * 2. ✅ 所有操作都是异步的
 * 3. ✅ workerId范围: 256-511 (与其他算子隔离)
 */
public class UserIdAsyncOperator extends RichAsyncFunction<IdOutput, IdOutput> {

    private transient KvrocksClient kvrocks;
    private transient Cache<String, Long> localCache;
    private transient SnowflakeIdGenerator idGenerator;  // ✅ 新增

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

        // ✅ 初始化雪花算法生成器
        // workerId范围: 256 + subtaskIndex (256-511)
        // 避免与DeviceId(0-255)和Zgid(512-767)冲突
        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        int totalSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();

        if (totalSubtasks > 256) {
            throw new RuntimeException(
                    "UserId算子最多支持256个并行度,当前: " + totalSubtasks);
        }

        int workerId = 256 + subtaskIndex;
        idGenerator = new SnowflakeIdGenerator(workerId);

        localCache = Caffeine.newBuilder()
                .maximumSize(10000)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .build();

        System.out.println(String.format(
                "[UserId算子-%d] 雪花算法初始化成功, workerId=%d",
                subtaskIndex, workerId
        ));
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

        Long cachedZgUid = localCache.getIfPresent(cacheKey);
        if (cachedZgUid != null) {
            input.setZgUserId(cachedZgUid);
            input.setNewUser(false);
            resultFuture.complete(Collections.singleton(input));
            return;
        }

        kvrocks.asyncGet(cacheKey)
                .thenCompose(zgUidStr -> {

                    if (zgUidStr != null) {
                        Long zgUserId = Long.parseLong(zgUidStr);
                        localCache.put(cacheKey, zgUserId);
                        return CompletableFuture.completedFuture(
                                new UserIdResult(zgUserId, false));

                    } else {
                        // ✅ 雪花算法生成
                        Long newUserId = idGenerator.nextId();

                        // ✅ Fire-and-Forget: 后台异步写入,不等待
                        kvrocks.asyncSet(cacheKey, String.valueOf(newUserId))
                                .whenComplete((v, throwable) -> {
                                    if (throwable != null) {
                                        System.err.println("UserId写入失败: " + cacheKey);
                                    }
                                });

                        // 立即返回
                        localCache.put(cacheKey, newUserId);
                        return CompletableFuture.completedFuture(
                                new UserIdResult(newUserId, true));
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
        if (kvrocks != null) {
            kvrocks.shutdown();
        }

        if (localCache != null) {
            System.out.println(String.format(
                    "[UserId算子-%d] 缓存统计: %s",
                    getRuntimeContext().getIndexOfThisSubtask(),
                    localCache.stats()
            ));
        }
    }

    // 内部结果类
    private static class UserIdResult {
        final Long userId;
        final boolean isNew;

        UserIdResult(Long userId, boolean isNew) {
            this.userId = userId;
            this.isNew = isNew;
        }
    }
}