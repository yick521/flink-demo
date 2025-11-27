package com.zhugeio.demo.sink;

import com.alibaba.fastjson.JSON;
import com.zhugeio.demo.model.IdOutput;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.UUID;

/**
 * Kafka Sink（Flink 1.14完整适配版 - 修复client-id问题）
 */
public class KafkaSinkWithCheckpoint {

    /**
     * 自定义Kafka序列化Schema
     */
    private static class StringKafkaSerializationSchema implements KafkaSerializationSchema<String> {

        private final String topic;

        public StringKafkaSerializationSchema(String topic) {
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
            return new ProducerRecord<>(
                    topic,
                    element.getBytes(StandardCharsets.UTF_8)
            );
        }
    }

    /**
     * 创建Kafka Producer（支持Exactly-Once）
     * Flink 1.14版本 - 修复client-id问题
     */
    public static FlinkKafkaProducer<String> createKafkaProducer(
            String topic,
            String brokers,
            boolean enableExactlyOnce) {

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", brokers);

        // 🔧 关键修复：设置简短合法的client.id（避免JMX错误）
        // 只使用字母、数字、下划线、连字符
        String clientId = "flink-producer-" + UUID.randomUUID().toString().substring(0, 8);
        kafkaProps.setProperty("client.id", clientId);

        // 创建序列化Schema
        KafkaSerializationSchema<String> serializationSchema =
                new StringKafkaSerializationSchema(topic);

        if (enableExactlyOnce) {
            // Exactly-Once语义配置
            kafkaProps.setProperty("transaction.timeout.ms", "600000");  // 15分钟

            // 设置事务ID前缀（也要简短合法）
            kafkaProps.setProperty("transactional.id", "flink-txn-" +
                    UUID.randomUUID().toString().substring(0, 8));

            return new FlinkKafkaProducer<>(
                    topic,                                        // default topic
                    serializationSchema,                          // serialization schema
                    kafkaProps,                                   // producer config
                    FlinkKafkaProducer.Semantic.EXACTLY_ONCE     // exactly-once mode
            );
        } else {
            // At-Least-Once语义配置
            return new FlinkKafkaProducer<>(
                    topic,
                    serializationSchema,
                    kafkaProps,
                    FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
            );
        }
    }

    /**
     * 添加Kafka Sink到数据流
     */
    public static void addKafkaSink(
            DataStream<IdOutput> stream,
            String topic,
            String brokers,
            boolean enableExactlyOnce) {

        // 转换为JSON
        DataStream<String> jsonStream = stream.map(new MapFunction<IdOutput, String>() {
            @Override
            public String map(IdOutput value) throws Exception {
                return JSON.toJSONString(value);
            }
        }).name("To-JSON-String").uid("to-json-string");

        // 添加Kafka Sink
        FlinkKafkaProducer<String> kafkaProducer =
                createKafkaProducer(topic, brokers, enableExactlyOnce);

        jsonStream.addSink(kafkaProducer)
                .name("Kafka-Sink")
                .uid("kafka-sink");
    }
}