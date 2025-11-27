package com.zhugeio.demo.operator.async;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.zhugeio.demo.KvrocksClient;
import com.zhugeio.demo.model.IdOutput;
import com.zhugeio.demo.model.RawEvent;
import com.zhugeio.demo.utils.SnowflakeIdGenerator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * 设备ID异步映射算子 (雪花算法版本)
 *
 * 优化点:
 * 1. ✅ 使用雪花算法生成ID,无需同步调用KVRocks incr
 * 2. ✅ 所有操作都是异步的
 * 3. ✅ workerId范围: 0-255 (与其他算子隔离)
 */
public class DeviceIdAsyncOperator extends RichAsyncFunction<RawEvent, IdOutput> {

    private transient KvrocksClient kvrocks;
    private transient Cache<String, Long> deviceCache;
    private transient SnowflakeIdGenerator idGenerator;  // ✅ 新增

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
        // 初始化KVRocks客户端
        kvrocks = new KvrocksClient(kvrocksHost, kvrocksPort, kvrocksCluster);
        kvrocks.init();

        // 测试连接
        if (!kvrocks.testConnection()) {
            throw new RuntimeException("KVRocks连接失败!");
        }

        // ✅ 初始化雪花算法生成器
        // workerId范围: 0-255 (subtaskIndex直接使用)
        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        int totalSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();

        if (totalSubtasks > 256) {
            throw new RuntimeException(
                    "DeviceId算子最多支持256个并行度,当前: " + totalSubtasks);
        }

        idGenerator = new SnowflakeIdGenerator(subtaskIndex);

        // 初始化Caffeine缓存
        deviceCache = Caffeine.newBuilder()
                .maximumSize(100000)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .recordStats()
                .build();

        System.out.println(String.format(
                "[DeviceId算子-%d] 初始化成功, KVRocks: %s:%d, workerId=%d",
                subtaskIndex, kvrocksHost, kvrocksPort, subtaskIndex
        ));
    }

    @Override
    public void asyncInvoke(RawEvent input, ResultFuture<IdOutput> resultFuture) throws Exception {

        String cacheKey = input.getAppId() + ":" + input.getDid();

        Long cachedDeviceId = deviceCache.getIfPresent(cacheKey);

        if (cachedDeviceId != null) {
            IdOutput output = createOutput(input, cachedDeviceId, false);
            resultFuture.complete(Collections.singleton(output));
            return;
        }

        String hashKey = "d:" + input.getAppId();
        String field = input.getDid();

        kvrocks.asyncHGet(hashKey, field)
                .thenCompose(value -> {

                    if (value != null) {
                        Long zgDeviceId = Long.parseLong(value);
                        deviceCache.put(cacheKey, zgDeviceId);

                        IdOutput output = createOutput(input, zgDeviceId, false);
                        return CompletableFuture.completedFuture(output);

                    } else {
                        // ✅ 雪花算法生成
                        Long newDeviceId = idGenerator.nextId();

                        // ✅ Fire-and-Forget: 后台异步写入
                        kvrocks.asyncHSet(hashKey, field, String.valueOf(newDeviceId))
                                .whenComplete((v, throwable) -> {
                                    if (throwable != null) {
                                        System.err.println("DeviceId写入失败: " + hashKey + ":" + field);
                                    }
                                });

                        // 立即返回
                        deviceCache.put(cacheKey, newDeviceId);
                        return CompletableFuture.completedFuture(
                                createOutput(input, newDeviceId, true));
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
        output.setNewDevice(isNew);  // ✅ 设置是否新设备
        output.setEventName(input.getEventName());
        output.setTimestamp(input.getTimestamp());
        output.setIngestTime(input.getIngestTime());
        return output;
    }

    @Override
    public void close() throws Exception {
        if (kvrocks != null) {
            kvrocks.shutdown();
        }

        if (deviceCache != null) {
            System.out.println(String.format(
                    "[DeviceId算子-%d] 缓存统计: %s",
                    getRuntimeContext().getIndexOfThisSubtask(),
                    deviceCache.stats()
            ));
        }
    }
}