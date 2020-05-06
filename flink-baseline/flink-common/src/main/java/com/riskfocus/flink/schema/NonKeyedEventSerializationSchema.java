package com.riskfocus.flink.schema;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.function.Supplier;

/**
 * @author Khokhlov Pavel
 */
@AllArgsConstructor
@Slf4j
public class NonKeyedEventSerializationSchema<T> implements KafkaSerializationSchema<T> {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final long serialVersionUID = -7630400380854325462L;

    static {
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    private final String topic;
    private final Supplier<byte[]> keySupplier;

    @Override
    public ProducerRecord<byte[], byte[]> serialize(T element, @Nullable Long timestamp) {
        try {
            final byte[] key = keySupplier.get();
            ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(topic, key, objectMapper.writeValueAsBytes(element));
            log.debug("Create producer record for topic: {}, key: {}, value: {}", topic, key, element);
            return producerRecord;
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Could not serialize record: " + element, e);
        }
    }
}