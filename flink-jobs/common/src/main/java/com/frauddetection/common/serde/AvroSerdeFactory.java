package com.frauddetection.common.serde;

import com.frauddetection.common.config.PipelineConfig;
import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;

/**
 * Factory for Kafka sources and sinks with Confluent Schema Registry Avro serde.
 */
public final class AvroSerdeFactory {
    
    private AvroSerdeFactory() {}

    /**
     * Creates a KafkaSource that deserializes Avro SpecificRecords using the Confluent Schema Registry.
     */
    public static <T extends SpecificRecord> KafkaSource<T> kafkaSource(
            String topic, String groupId, Class<T> clazz) {

        return KafkaSource.<T>builder()
            .setBootstrapServers(PipelineConfig.kafkaBootstrapServers())
            .setTopics(topic)
            .setGroupId(groupId)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(
                ConfluentRegistryAvroDeserializationSchema.
                    forSpecific(clazz, PipelineConfig.schemaRegistryUrl()))
            .build();
    }

    /**
     * Creates a KafkaSink that serializes Avro SpecificRecords using the Confluent Schema Registry.
     * Subject naming follows the TopicNameStrategy: {topic}-value.
     */
    public static <T extends SpecificRecord> KafkaSink<T> kafkaSink(
            String topic, Class<T> clazz) {

        return KafkaSink.<T>builder()
            .setBootstrapServers(PipelineConfig.kafkaBootstrapServers())
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic(topic)
                    .setValueSerializationSchema(
                        ConfluentRegistryAvroSerializationSchema
                            .forSpecific(
                                clazz,
                                topic + "-value",
                                PipelineConfig.schemaRegistryUrl()))
                    .build())
            .build();
    }
}