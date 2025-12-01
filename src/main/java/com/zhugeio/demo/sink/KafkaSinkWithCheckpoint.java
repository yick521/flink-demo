package com.zhugeio.demo.sink;

import com.zhugeio.demo.model.IdOutput;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Sink 工厂类（使用自定义Sink避免JMX冲突）
 */
public class KafkaSinkWithCheckpoint {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSinkWithCheckpoint.class);

    /**
     * 添加Kafka Sink到数据流（使用自定义实现）
     *
     * 注意：这个方法现在使用 CustomKafkaSink 替代 FlinkKafkaProducer
     * 原因：FlinkKafkaProducer 生成的 client-id 包含冒号，导致 Kafka Broker 的 JMX 冲突
     */
    public static void addKafkaSink(
            DataStream<IdOutput> stream,
            String topic,
            String brokers,
            boolean enableExactlyOnce) {

        LOG.info("\n========================================");
        LOG.info("配置Kafka Sink:");
        LOG.info("  - 实现: CustomKafkaSink (避免JMX冲突)");
        LOG.info("  - Topic: {}", topic);
        LOG.info("  - Brokers: {}", brokers);
        LOG.info("  - Exactly-Once: {}", enableExactlyOnce);
        LOG.info("========================================\n");

        // 使用自定义 Kafka Sink
        CustomKafkaSink.addCustomKafkaSink(stream, topic, brokers, enableExactlyOnce);
    }
}