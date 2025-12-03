package com.zhugeio.demo.operator.async;

import com.zhugeio.demo.KvrocksClient;
import com.zhugeio.demo.model.IdOutput;
import com.zhugeio.demo.model.RawEvent;
import com.zhugeio.demo.utils.SnowflakeIdGenerator;
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
 * 设备ID异步映射算子 - 优雅关闭版本
 *
 * 修复点:
 * 1. ✅ 使用ConcurrentHashMap追踪未完成的写入
 * 2. ✅ O(1)时间复杂度的add/remove操作
 * 3. ✅ close()时等待所有未完成的写入
 * 4. ✅ 防止连接过早关闭导致数据丢失
 */
public class DeviceIdAsyncOperator extends RichAsyncFunction<RawEvent, IdOutput> {

    private static final Logger LOG = LoggerFactory.getLogger(DeviceIdAsyncOperator.class);

    private transient KvrocksClient kvrocks;
    private transient SnowflakeIdGenerator idGenerator;

    // ✅ 使用ConcurrentHashMap追踪未完成的写入
    private transient ConcurrentHashMap<Long, CompletableFuture<Void>> pendingWrites;
    private transient AtomicLong futureIdGenerator;

    private final String kvrocksHost;
    private final int kvrocksPort;
    private final boolean kvrocksCluster;

    public DeviceIdAsyncOperator(String kvrocksHost, int kvrocksPort, boolean kvrocksCluster) {
        this.kvrocksHost = kvrocksHost;
        this.kvrocksPort = kvrocksPort;
        this.kvrocksCluster = kvrocksCluster;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        kvrocks = new KvrocksClient(kvrocksHost, kvrocksPort, kvrocksCluster);
        kvrocks.init();

        if (!kvrocks.testConnection()) {
            throw new RuntimeException("KVRocks连接失败!");
        }

        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        int totalSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();

        if (totalSubtasks > 256) {
            throw new RuntimeException("DeviceId算子最多支持256个并行度,当前: " + totalSubtasks);
        }

        idGenerator = new SnowflakeIdGenerator(subtaskIndex);

        // ✅ 初始化ConcurrentHashMap
        pendingWrites = new ConcurrentHashMap<>(1024);
        futureIdGenerator = new AtomicLong(0);

        LOG.info("[DeviceId算子-{}] 初始化成功, KVRocks: {}:{}, workerId={}",
                subtaskIndex, kvrocksHost, kvrocksPort, subtaskIndex);
    }

    @Override
    public void asyncInvoke(RawEvent input, ResultFuture<IdOutput> resultFuture) throws Exception {
        String hashKey = "d:" + input.getAppId();
        String field = input.getDid();

        kvrocks.asyncHGet(hashKey, field)
                .thenCompose(value -> {
                    if (value != null) {
                        Long zgDeviceId = Long.parseLong(value);
                        IdOutput output = createOutput(input, zgDeviceId, false);
                        return CompletableFuture.completedFuture(output);
                    } else {
                        Long newDeviceId = idGenerator.nextId();

                        // ✅ 生成唯一ID作为key
                        Long futureId = futureIdGenerator.incrementAndGet();

                        CompletableFuture<Void> writeFuture = kvrocks.asyncHSet(hashKey, field, String.valueOf(newDeviceId))
                                .whenComplete((v, throwable) -> {
                                    // ✅ O(1)时间复杂度删除
                                    pendingWrites.remove(futureId);

                                    if (throwable != null) {
                                        LOG.error("[DeviceId算子-{}] DeviceId写入失败: {}:{}",
                                                getRuntimeContext().getIndexOfThisSubtask(),
                                                hashKey, field, throwable);
                                    }
                                });

                        // ✅ O(1)时间复杂度添加
                        pendingWrites.put(futureId, writeFuture);

                        return CompletableFuture.completedFuture(createOutput(input, newDeviceId, true));
                    }
                })
                .whenComplete((output, throwable) -> {
                    if (throwable != null) {
                        resultFuture.completeExceptionally(throwable);
                    } else {
                        resultFuture.complete(Collections.singleton(output));
                    }
                });
    }

    private IdOutput createOutput(RawEvent input, Long zgDeviceId, boolean isNew) {
        IdOutput output = new IdOutput();
        output.setAppKey(input.getAppKey());
        output.setAppId(input.getAppId());
        output.setDid(input.getDid());
        output.setCuid(input.getCuid());
        output.setSid(input.getSid());
        output.setZgDeviceId(zgDeviceId);
        output.setNewDevice(isNew);
        output.setEventName(input.getEventName());
        output.setTimestamp(input.getTimestamp());
        output.setIngestTime(input.getIngestTime());
        return output;
    }

    @Override
    public void close() throws Exception {
        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();

        LOG.info("[DeviceId算子-{}] 开始关闭，等待未完成的写入...", subtaskIndex);

        // ✅ 等待所有未完成的写入
        if (pendingWrites != null && !pendingWrites.isEmpty()) {
            long startWait = System.currentTimeMillis();
            int pendingCount = pendingWrites.size();

            LOG.info("[DeviceId算子-{}] 发现 {} 个未完成的写入操作，开始等待...",
                    subtaskIndex, pendingCount);

            try {
                CompletableFuture<Void> allWrites = CompletableFuture.allOf(
                        pendingWrites.values().toArray(new CompletableFuture[0])
                );
                allWrites.get(30, TimeUnit.SECONDS);

                long waitTime = System.currentTimeMillis() - startWait;
                LOG.info("[DeviceId算子-{}] ✅ 所有未完成的写入已完成，等待时间: {} ms",
                        subtaskIndex, waitTime);

            } catch (Exception e) {
                long waitTime = System.currentTimeMillis() - startWait;
                LOG.error("[DeviceId算子-{}] ⚠️  等待写入完成超时或失败，等待时间: {} ms, 剩余未完成: {}",
                        subtaskIndex, waitTime, pendingWrites.size(), e);
            }
        }

        if (kvrocks != null) {
            kvrocks.shutdown();
            LOG.info("[DeviceId算子-{}] KVRocks连接已关闭", subtaskIndex);
        }
    }
}