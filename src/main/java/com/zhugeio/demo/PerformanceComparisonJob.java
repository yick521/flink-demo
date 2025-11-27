package com.zhugeio.demo;

import com.zhugeio.demo.generator.EventDataGenerator;
import com.zhugeio.demo.model.RawEvent;
import com.zhugeio.demo.model.IdOutput;
import com.zhugeio.demo.operator.async.*;
import com.zhugeio.demo.operator.sync.SessionIdProcessOperator;
import com.zhugeio.demo.operator.window.IdWindowedBatchOperator;
import com.zhugeio.demo.sink.PerformanceMetricsSink;
import com.zhugeio.demo.sink.KafkaSinkWithCheckpoint;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;


/**
 * ETL ID模块性能对比主程序（完整版）
 *
 * 新增功能：
 * 1. Kafka Sink（支持Checkpoint）
 * 2. Exactly-Once语义
 * 3. 可选择是否启用Checkpoint
 *
 * 运行参数：
 * --mode async                    # 流式异步模式
 * --mode window                   # 开窗批量模式
 * --qps 3000                      # 每个并行度的QPS
 * --parallelism 16                # 并行度
 * --duration 10800                # 测试时长（秒）
 * --window 5                      # 窗口大小（秒）
 * --capacity 100                  # AsyncIO容量
 * --checkpoint-enabled true       # 是否启用Checkpoint
 * --checkpoint-interval 60000     # Checkpoint间隔（毫秒）
 * --kafka-enabled true            # 是否启用Kafka输出
 * --kafka-topic id-output         # Kafka Topic
 * --kafka-brokers localhost:9092  # Kafka地址
 * --exactly-once true             # 是否启用Exactly-Once
 */
public class PerformanceComparisonJob {

    public static void main(String[] args) throws Exception {

        // ========== 解析参数 ==========
        String mode = getParameter(args, "--mode", "async");
        int qps = Integer.parseInt(getParameter(args, "--qps", "3000"));
        int parallelism = Integer.parseInt(getParameter(args, "--parallelism", "16"));
        int windowSeconds = Integer.parseInt(getParameter(args, "--window", "5"));
        int asyncCapacity = Integer.parseInt(getParameter(args, "--capacity", "100"));
        int durationSeconds = Integer.parseInt(getParameter(args, "--duration", "10800"));

        // KVRocks配置
        String kvrocksHost = getParameter(args, "--kvrocks-host", "10.10.0.115");
        int kvrocksPort = Integer.parseInt(getParameter(args, "--kvrocks-port", "7001"));


        // Checkpoint配置
        boolean checkpointEnabled = Boolean.parseBoolean(
                getParameter(args, "--checkpoint-enabled", "true"));
        long checkpointInterval = Long.parseLong(
                getParameter(args, "--checkpoint-interval", "60000"));

        // Kafka配置
        boolean kafkaEnabled = Boolean.parseBoolean(
                getParameter(args, "--kafka-enabled", "true"));
        String kafkaTopic = getParameter(args, "--kafka-topic", "id-output");
        String kafkaBrokers = getParameter(args, "--kafka-brokers", "localhost:9092");
        boolean exactlyOnce = Boolean.parseBoolean(
                getParameter(args, "--exactly-once", "true"));

        // 计算数据量
        long maxRecordsPerSubtask = (long) qps * durationSeconds;
        long totalRecords = maxRecordsPerSubtask * parallelism;

        // ========== 打印配置 ==========
        System.out.println("========== ETL ID模块性能测试（完整版） ==========");
        System.out.println("模式: " + mode);
        System.out.println("QPS(每并行度): " + qps);
        System.out.println("并行度: " + parallelism);
        System.out.println("总QPS: " + (qps * parallelism));
        System.out.println("测试时长: " + durationSeconds + " 秒 (" +
                String.format("%.2f", durationSeconds / 3600.0) + " 小时)");
        System.out.println("预计总数据量: " +
                String.format("%.2f", totalRecords / 100000000.0) + " 亿条");
        System.out.println("每条记录字段数: 150+");

        if ("window".equals(mode)) {
            System.out.println("窗口大小: " + windowSeconds + "秒");
        } else {
            System.out.println("AsyncIO容量: " + asyncCapacity);
        }

        System.out.println("\n---------- Checkpoint配置 ----------");
        System.out.println("Checkpoint启用: " + checkpointEnabled);
        if (checkpointEnabled) {
            System.out.println("Checkpoint间隔: " + checkpointInterval + " ms");
            System.out.println("Checkpoint模式: EXACTLY_ONCE");
        }

        System.out.println("\n---------- Kafka配置 ----------");
        System.out.println("Kafka输出启用: " + kafkaEnabled);
        if (kafkaEnabled) {
            System.out.println("Kafka Topic: " + kafkaTopic);
            System.out.println("Kafka Brokers: " + kafkaBrokers);
            System.out.println("Exactly-Once语义: " + exactlyOnce);
        }
        System.out.println("===================================================\n");

        // ========== 创建执行环境 ==========
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.disableOperatorChaining();
        env.setParallelism(parallelism);

        // ========== 配置Checkpoint ==========
        if (checkpointEnabled) {
            env.enableCheckpointing(checkpointInterval);
            env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(checkpointInterval / 2);
            env.getCheckpointConfig().setCheckpointTimeout(600000);  // 10分钟超时
            env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

            System.out.println("✅ Checkpoint已启用（Exactly-Once模式）\n");
        } else {
            System.out.println("⚠️  Checkpoint未启用（无容错保证）\n");
        }

        // ========== 数据源 ==========
        DataStream<RawEvent> source = env
                .addSource(new EventDataGenerator(qps, maxRecordsPerSubtask))
                .name("EventGenerator")
                .uid("event-generator")
                .setParallelism(parallelism);

        // ========== 处理逻辑 ==========
        DataStream<IdOutput> result;

        if ("async".equals(mode)) {
            result = processWithAsyncIO(source, asyncCapacity, kvrocksHost, kvrocksPort);
        } else if ("window".equals(mode)) {
            result = processWithWindow(source, windowSeconds, kvrocksHost, kvrocksPort);
        } else {
            throw new IllegalArgumentException("Unknown mode: " + mode);
        }


        // ========== Kafka Sink（可选） ==========
        if (kafkaEnabled) {
            KafkaSinkWithCheckpoint.addKafkaSink(
                    result,
                    kafkaTopic,
                    kafkaBrokers,
                    exactlyOnce && checkpointEnabled  // Exactly-Once需要Checkpoint
            );

            System.out.println("✅ Kafka Sink已添加（Topic: " + kafkaTopic + "）\n");
        }

        // ========== 性能指标收集 ==========
        result.addSink(new PerformanceMetricsSink(mode.toUpperCase()))
                .name("PerformanceMetricsSink")
                .uid("performance-metrics-sink")
                .setParallelism(1);

        // ========== 执行 ==========
        String jobName = String.format("ETL-ID-%s-%s-Checkpoint%s",
                mode.toUpperCase(),
                String.format("%.1fM", totalRecords / 1000000.0),
                checkpointEnabled ? "-ON" : "-OFF"
        );

        System.out.println("🚀 开始执行：" + jobName + "\n");

        env.execute(jobName);
    }

    private static DataStream<IdOutput> processWithAsyncIO(
            DataStream<RawEvent> source,
            int capacity,
            String kvrocksHost,
            int kvrocksPort) {

        System.out.println("📊 使用流式异步处理（AsyncIO + 真实KVRocks）\n");

        // 1. 设备ID映射
        SingleOutputStreamOperator<IdOutput> withDeviceId = AsyncDataStream.unorderedWait(
                        source,
                        new DeviceIdAsyncOperator(kvrocksHost, kvrocksPort, true),  // ← 传入KVRocks地址
                        5000, TimeUnit.MILLISECONDS, capacity
                ).name("DeviceId-AsyncIO")
                .uid("device-id-async")
                .setParallelism(4);

        // 2. 会话ID处理
        DataStream<IdOutput> withSessionId = withDeviceId
                .process(new SessionIdProcessOperator())
                .name("SessionId-Process")
                .uid("session-id-process")
                .setParallelism(4);

        // 3. 用户ID映射
        SingleOutputStreamOperator<IdOutput> withUserId = AsyncDataStream.unorderedWait(
                        withSessionId,
                        new UserIdAsyncOperator(kvrocksHost, kvrocksPort),  // ← 传入KVRocks地址
                        5000, TimeUnit.MILLISECONDS, capacity
                ).name("UserId-AsyncIO")
                .uid("user-id-async")
                .setParallelism(4);

        // 4. 诸葛ID映射
        SingleOutputStreamOperator<IdOutput> withZgid = AsyncDataStream.unorderedWait(
                        withUserId,
                        new ZgidAsyncOperator(kvrocksHost, kvrocksPort),  // ← 传入KVRocks地址
                        5000, TimeUnit.MILLISECONDS, capacity
                ).name("Zgid-AsyncIO")
                .uid("zgid-async")
                .setParallelism(4);

        return withZgid;
    }

    /**
     * 方案B：开窗批量处理
     */
    private static DataStream<IdOutput> processWithWindow(
            DataStream<RawEvent> source, int windowSeconds, String kvrocksHost, int kvrocksPort) {

        System.out.println("📊 使用开窗批量处理（Window + 真实KVRocks）\n");
        
        int parallelism = source.getParallelism();

        DataStream<IdOutput> result = source
                .keyBy(event -> event.getDeviceId())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(windowSeconds)))
                .process(new IdWindowedBatchOperator(kvrocksHost, kvrocksPort, true))  // ← 传入KVRocks地址
                .name("ID-WindowedBatch")
                .uid("id-windowed-batch");

        return result;
    }

    private static String getParameter(String[] args, String key, String defaultValue) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(key)) {
                return args[i + 1];
            }
        }
        return defaultValue;
    }
}