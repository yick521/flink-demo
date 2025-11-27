package com.zhugeio.demo.operator.async;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.zhugeio.demo.KvrocksClient;
import com.zhugeio.demo.model.IdOutput;
import com.zhugeio.demo.utils.SnowflakeIdGenerator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 诸葛ID映射算子 (雪花算法版本 + Fire-and-Forget优化)
 *
 * 优化点:
 * 1. ✅ 使用雪花算法生成ID,无需同步调用KVRocks incr
 * 2. ✅ Fire-and-Forget模式: 先返回结果,后台异步写入
 * 3. ✅ 写入失败重试机制
 * 4. ✅ 监控写入失败率
 */
public class ZgidAsyncOperator extends RichAsyncFunction<IdOutput, IdOutput> {

    private transient KvrocksClient kvrocks;
    private transient Cache<String, Long> localCache;
    private transient SnowflakeIdGenerator idGenerator;

    // ✅ 新增: 监控指标
    private transient AtomicLong writeFailureCount;
    private transient AtomicLong totalWriteCount;

    private final String kvrocksHost;
    private final int kvrocksPort;
    private final boolean kvrocksCluster;

    // ✅ 新增: 是否等待写入完成 (默认false,使用Fire-and-Forget)
    private final boolean waitForWrite;

    public ZgidAsyncOperator() {
        this(null, 0, true, false);
    }

    public ZgidAsyncOperator(String kvrocksHost, int kvrocksPort) {
        this(kvrocksHost, kvrocksPort, true, false);
    }

    public ZgidAsyncOperator(String kvrocksHost, int kvrocksPort, boolean kvrocksCluster) {
        this(kvrocksHost, kvrocksPort, kvrocksCluster, false);
    }

    public ZgidAsyncOperator(String kvrocksHost, int kvrocksPort, boolean kvrocksCluster, boolean waitForWrite) {
        this.kvrocksHost = kvrocksHost;
        this.kvrocksPort = kvrocksPort;
        this.kvrocksCluster = kvrocksCluster;
        this.waitForWrite = waitForWrite;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        kvrocks = new KvrocksClient(kvrocksHost, kvrocksPort, kvrocksCluster);
        kvrocks.init();

        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        int totalSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();

        if (totalSubtasks > 256) {
            throw new RuntimeException(
                    "Zgid算子最多支持256个并行度,当前: " + totalSubtasks);
        }

        int workerId = 512 + subtaskIndex;
        idGenerator = new SnowflakeIdGenerator(workerId);

        localCache = Caffeine.newBuilder()
                .maximumSize(10000)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .build();

        // ✅ 初始化监控指标
        writeFailureCount = new AtomicLong(0);
        totalWriteCount = new AtomicLong(0);

        System.out.println(String.format(
                "[Zgid算子-%d] 雪花算法初始化成功, workerId=%d, waitForWrite=%s",
                subtaskIndex, workerId, waitForWrite
        ));
    }

    @Override
    public void asyncInvoke(IdOutput input, ResultFuture<IdOutput> resultFuture) {

        String cacheKey;

        // 优先用用户维度
        if (input.getZgUserId() != null) {
            cacheKey = "uz:" + input.getAppId() + ":" + input.getZgUserId();
        } else {
            cacheKey = "dz:" + input.getAppId() + ":" + input.getZgDeviceId();
        }

        // 1. 本地缓存查询
        Long cachedZgid = localCache.getIfPresent(cacheKey);
        if (cachedZgid != null) {
            completeWithZgid(input, cachedZgid, false, resultFuture);
            return;
        }

        // 2. 异步查询KVRocks
        kvrocks.asyncGet(cacheKey)
                .thenCompose(zgidStr -> {

                    if (zgidStr != null) {
                        // KVRocks中已存在
                        Long zgid = Long.parseLong(zgidStr);
                        localCache.put(cacheKey, zgid);
                        return CompletableFuture.completedFuture(
                                new ZgidResult(zgid, false));

                    } else {
                        // ✅ 使用雪花算法生成新ID
                        Long newZgid = idGenerator.nextId();

                        // ✅ 关键优化: Fire-and-Forget模式
                        if (waitForWrite) {
                            // 模式1: 等待写入完成 (数据一致性优先)
                            return writeToKVRocksAndWait(cacheKey, newZgid, input);
                        } else {
                            // 模式2: 后台异步写入 (性能优先,推荐)
                            writeToKVRocksAsync(cacheKey, newZgid, input);

                            // 立即返回结果,不等待写入
                            localCache.put(cacheKey, newZgid);
                            return CompletableFuture.completedFuture(
                                    new ZgidResult(newZgid, true));
                        }
                    }
                })
                .whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        resultFuture.completeExceptionally(throwable);
                    } else {
                        completeWithZgid(input, result.zgid, result.isNew, resultFuture);
                    }
                });
    }

    /**
     * ✅ Fire-and-Forget异步写入 (性能优先,推荐)
     */
    private void writeToKVRocksAsync(String cacheKey, Long newZgid, IdOutput input) {
        totalWriteCount.incrementAndGet();

        // 构建两个写入任务
        CompletableFuture<Void> writeMain =
                kvrocks.asyncSet(cacheKey, String.valueOf(newZgid));

        CompletableFuture<Void> writeDevice =
                kvrocks.asyncSet(
                        "dz:" + input.getAppId() + ":" + input.getZgDeviceId(),
                        String.valueOf(newZgid)
                );

        // ✅ 后台异步执行,不阻塞主流程
        CompletableFuture.allOf(writeMain, writeDevice)
                .whenComplete((v, throwable) -> {
                    if (throwable != null) {
                        long failCount = writeFailureCount.incrementAndGet();

                        // 记录错误日志
                        System.err.println(String.format(
                                "[Zgid算子-%d] KVRocks写入失败 (总失败=%d): key=%s, zgid=%d, error=%s",
                                getRuntimeContext().getIndexOfThisSubtask(),
                                failCount, cacheKey, newZgid, throwable.getMessage()
                        ));

                        // ✅ 可选: 重试机制 (最多3次)
                        retryWrite(cacheKey, newZgid, input, 1, 3);
                    }
                });
    }

    /**
     * ✅ 写入重试机制
     */
    private void retryWrite(String cacheKey, Long zgid, IdOutput input, int attempt, int maxAttempts) {
        if (attempt > maxAttempts) {
            System.err.println(String.format(
                    "[Zgid算子-%d] KVRocks写入重试失败,已达最大次数: key=%s, zgid=%d",
                    getRuntimeContext().getIndexOfThisSubtask(), cacheKey, zgid
            ));
            return;
        }

        CompletableFuture<Void> writeMain =
                kvrocks.asyncSet(cacheKey, String.valueOf(zgid));

        CompletableFuture<Void> writeDevice =
                kvrocks.asyncSet(
                        "dz:" + input.getAppId() + ":" + input.getZgDeviceId(),
                        String.valueOf(zgid)
                );

        CompletableFuture.allOf(writeMain, writeDevice)
                .whenComplete((v, throwable) -> {
                    if (throwable != null) {
                        System.err.println(String.format(
                                "[Zgid算子-%d] 重试第%d次失败: key=%s",
                                getRuntimeContext().getIndexOfThisSubtask(), attempt, cacheKey
                        ));

//                        // 指数退避重试
//                        try {
//                            Thread.sleep(100 * attempt);
//                        } catch (InterruptedException e) {
//                            Thread.currentThread().interrupt();
//                        }

                        retryWrite(cacheKey, zgid, input, attempt + 1, maxAttempts);
                    } else {
                        System.out.println(String.format(
                                "[Zgid算子-%d] 重试第%d次成功: key=%s",
                                getRuntimeContext().getIndexOfThisSubtask(), attempt, cacheKey
                        ));
                    }
                });
    }

    /**
     * 等待写入完成 (数据一致性优先,不推荐)
     */
    private CompletableFuture<ZgidResult> writeToKVRocksAndWait(String cacheKey, Long newZgid, IdOutput input) {
        totalWriteCount.incrementAndGet();

        CompletableFuture<Void> writeMain =
                kvrocks.asyncSet(cacheKey, String.valueOf(newZgid));

        CompletableFuture<Void> writeDevice =
                kvrocks.asyncSet(
                        "dz:" + input.getAppId() + ":" + input.getZgDeviceId(),
                        String.valueOf(newZgid)
                );

        return CompletableFuture.allOf(writeMain, writeDevice)
                .thenApply(v -> {
                    localCache.put(cacheKey, newZgid);
                    return new ZgidResult(newZgid, true);
                })
                .exceptionally(throwable -> {
                    writeFailureCount.incrementAndGet();
                    throw new RuntimeException("KVRocks写入失败: " + cacheKey, throwable);
                });
    }

    private void completeWithZgid(IdOutput input, Long zgid, boolean isNew,
                                  ResultFuture<IdOutput> resultFuture) {
        input.setZgid(zgid);
        input.setNewZgid(isNew);
        input.setProcessTime(System.currentTimeMillis());
        input.setLatency(input.getProcessTime() - input.getIngestTime());
        resultFuture.complete(Collections.singleton(input));
    }

    @Override
    public void close() throws Exception {
        if (kvrocks != null) {
            kvrocks.shutdown();
        }

        // ✅ 输出监控统计
        if (totalWriteCount != null && writeFailureCount != null) {
            long total = totalWriteCount.get();
            long failed = writeFailureCount.get();
            double failureRate = total > 0 ? (failed * 100.0 / total) : 0;

            System.out.println(String.format(
                    "[Zgid算子-%d] 关闭统计: 总写入=%d, 失败=%d, 失败率=%.2f%%",
                    getRuntimeContext().getIndexOfThisSubtask(),
                    total, failed, failureRate
            ));
        }

        if (localCache != null) {
            System.out.println(String.format(
                    "[Zgid算子-%d] 缓存统计: %s",
                    getRuntimeContext().getIndexOfThisSubtask(),
                    localCache.stats()
            ));
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