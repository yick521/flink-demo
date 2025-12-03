package com.zhugeio.demo.operator.async;

import com.zhugeio.demo.KvrocksClient;
import com.zhugeio.demo.model.IdOutput;
import com.zhugeio.demo.utils.SnowflakeIdGenerator;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
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
 * Zgid异步映射算子 - 优雅关闭版本
 *
 * 修复点:
 * 1. ✅ 使用ConcurrentHashMap追踪未完成的写入
 * 2. ✅ O(1)时间复杂度的add/remove操作
 * 3. ✅ close()时等待所有未完成的写入
 * 4. ✅ 防止连接过早关闭导致数据丢失
 */
public class ZgidAsyncOperator extends RichAsyncFunction<IdOutput, IdOutput> {

    private static final Logger LOG = LoggerFactory.getLogger(ZgidAsyncOperator.class);

    private transient KvrocksClient kvrocks;
    private transient SnowflakeIdGenerator idGenerator;

    // ✅ 使用ConcurrentHashMap追踪未完成的写入操作
    private transient ConcurrentHashMap<Long, CompletableFuture<Void>> pendingWrites;
    private transient AtomicLong futureIdGenerator;

    // 统计指标
    private transient Counter totalWrites;
    private transient Counter totalWriteFailures;
    private transient AtomicLong writeFailureCount;

    private final String kvrocksHost;
    private final int kvrocksPort;
    private final boolean kvrocksCluster;

    private transient int subtaskIndex;

    public ZgidAsyncOperator() {
        this(null, 0, true);
    }

    public ZgidAsyncOperator(String kvrocksHost, int kvrocksPort) {
        this(kvrocksHost, kvrocksPort, true);
    }

    public ZgidAsyncOperator(String kvrocksHost, int kvrocksPort, boolean kvrocksCluster) {
        this.kvrocksHost = kvrocksHost;
        this.kvrocksPort = kvrocksPort;
        this.kvrocksCluster = kvrocksCluster;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        kvrocks = new KvrocksClient(kvrocksHost, kvrocksPort, kvrocksCluster);
        kvrocks.init();

        subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        int totalSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();

        if (totalSubtasks > 256) {
            throw new RuntimeException("Zgid算子最多支持256个并行度,当前: " + totalSubtasks);
        }

        int workerId = 512 + subtaskIndex;
        idGenerator = new SnowflakeIdGenerator(workerId);

        // ✅ 初始化ConcurrentHashMap
        pendingWrites = new ConcurrentHashMap<>(1024);
        futureIdGenerator = new AtomicLong(0);

        writeFailureCount = new AtomicLong(0);

        totalWrites = getRuntimeContext()
                .getMetricGroup()
                .counter("zgid_total_writes");
        totalWriteFailures = getRuntimeContext()
                .getMetricGroup()
                .counter("zgid_write_failures");

        LOG.info("[Zgid算子-{}] 初始化成功, workerId={}", subtaskIndex, workerId);
    }

    @Override
    public void asyncInvoke(IdOutput input, ResultFuture<IdOutput> resultFuture) {
        if (StringUtils.isBlank(input.getCuid()) || input.getZgDeviceId() == null) {
            input.setZgid(null);
            resultFuture.complete(Collections.singleton(input));
            return;
        }

        String cacheKey = "z:" + input.getAppId() + ":" + input.getCuid() + ":" + input.getZgDeviceId();

        kvrocks.asyncGet(cacheKey)
                .thenCompose(zgidStr -> {
                    if (zgidStr != null) {
                        Long zgid = Long.parseLong(zgidStr);
                        return CompletableFuture.completedFuture(new ZgidResult(zgid, false));
                    } else {
                        Long newZgid = idGenerator.nextId();
                        totalWrites.inc();

                        // ✅ Fire-and-Forget 异步写入，但要追踪
                        writeToKVRocksAsync(cacheKey, newZgid, input);

                        return CompletableFuture.completedFuture(new ZgidResult(newZgid, true));
                    }
                })
                .whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        resultFuture.completeExceptionally(throwable);
                    } else {
                        input.setZgid(result.zgid);
                        input.setNewZgid(result.isNew);
                        input.setLatency(System.currentTimeMillis()-input.getIngestTime());
                        resultFuture.complete(Collections.singleton(input));
                    }
                });
    }

    /**
     * 异步写入KVRocks（主键 + 设备映射）
     */
    private void writeToKVRocksAsync(String cacheKey, Long newZgid, IdOutput input) {
        CompletableFuture<Void> writeMain = kvrocks.asyncSet(cacheKey, String.valueOf(newZgid));
        CompletableFuture<Void> writeDevice = kvrocks.asyncHSet(
                "zgid_device:" + input.getAppId(),
                String.valueOf(input.getZgDeviceId()),
                String.valueOf(newZgid)
        );

        // ✅ 生成唯一ID作为key
        Long futureId = futureIdGenerator.incrementAndGet();

        CompletableFuture<Void> combinedFuture = CompletableFuture.allOf(writeMain, writeDevice)
                .whenComplete((v, throwable) -> {
                    // ✅ O(1)时间复杂度删除
                    pendingWrites.remove(futureId);

                    if (throwable != null) {
                        long failCount = writeFailureCount.incrementAndGet();
                        totalWriteFailures.inc();
                        LOG.error("[Zgid算子-{}] KVRocks写入失败 (总失败={}): key={}, zgid={}, error={}",
                                subtaskIndex, failCount, cacheKey, newZgid, throwable.getMessage());

                        // 重试一次
                        retryWrite(cacheKey, newZgid, input, 1, 3);
                    }
                });

        // ✅ O(1)时间复杂度添加
        pendingWrites.put(futureId, combinedFuture);
    }

    /**
     * 重试写入（最多3次）
     */
    private void retryWrite(String cacheKey, Long zgid, IdOutput input, int attempt, int maxAttempts) {
        if (attempt > maxAttempts) {
            LOG.error("[Zgid算子-{}] 重试{}次后仍失败，放弃: key={}, zgid={}",
                    subtaskIndex, maxAttempts, cacheKey, zgid);
            return;
        }

        LOG.info("[Zgid算子-{}] 开始第{}次重试: key={}, zgid={}",
                subtaskIndex, attempt, cacheKey, zgid);

        CompletableFuture<Void> writeMain = kvrocks.asyncSet(cacheKey, String.valueOf(zgid));
        CompletableFuture<Void> writeDevice = kvrocks.asyncHSet(
                "zgid_device:" + input.getAppId(),
                String.valueOf(input.getZgDeviceId()),
                String.valueOf(zgid)
        );

        // ✅ 生成唯一ID作为key
        Long futureId = futureIdGenerator.incrementAndGet();

        CompletableFuture<Void> retryFuture = CompletableFuture.allOf(writeMain, writeDevice)
                .whenComplete((v, throwable) -> {
                    // ✅ O(1)时间复杂度删除
                    pendingWrites.remove(futureId);

                    if (throwable != null) {
                        LOG.error("[Zgid算子-{}] 第{}次重试失败: key={}",
                                subtaskIndex, attempt, cacheKey, throwable);

                        // 继续重试
                        retryWrite(cacheKey, zgid, input, attempt + 1, maxAttempts);
                    } else {
                        LOG.info("[Zgid算子-{}] 第{}次重试成功: key={}, zgid={}",
                                subtaskIndex, attempt, cacheKey, zgid);
                    }
                });

        // ✅ O(1)时间复杂度添加
        pendingWrites.put(futureId, retryFuture);
    }

    @Override
    public void close() throws Exception {
        LOG.info("[Zgid算子-{}] 开始关闭，等待未完成的写入...", subtaskIndex);

        // ✅ 等待所有未完成的写入
        if (pendingWrites != null && !pendingWrites.isEmpty()) {
            long startWait = System.currentTimeMillis();
            int pendingCount = pendingWrites.size();

            LOG.info("[Zgid算子-{}] 发现 {} 个未完成的写入操作，开始等待...",
                    subtaskIndex, pendingCount);

            try {
                CompletableFuture<Void> allWrites = CompletableFuture.allOf(
                        pendingWrites.values().toArray(new CompletableFuture[0])
                );
                allWrites.get(30, TimeUnit.SECONDS);

                long waitTime = System.currentTimeMillis() - startWait;
                LOG.info("[Zgid算子-{}] ✅ 所有未完成的写入已完成，等待时间: {} ms",
                        subtaskIndex, waitTime);

            } catch (Exception e) {
                long waitTime = System.currentTimeMillis() - startWait;
                LOG.error("[Zgid算子-{}] ⚠️  等待写入完成超时或失败，等待时间: {} ms, 剩余未完成: {}",
                        subtaskIndex, waitTime, pendingWrites.size(), e);
            }
        }

        if (kvrocks != null) {
            kvrocks.shutdown();
            LOG.info("[Zgid算子-{}] KVRocks连接已关闭", subtaskIndex);
        }

        long totalFailures = writeFailureCount.get();
        if (totalFailures > 0) {
            LOG.warn("[Zgid算子-{}] 关闭统计: 总失败={}", subtaskIndex, totalFailures);
        }
    }

    private static class ZgidResult {
        final Long zgid;
        final boolean isNew;

        ZgidResult(Long zgid, boolean isNew) {
            this.zgid = zgid;
            this.isNew = isNew;
        }
    }
}