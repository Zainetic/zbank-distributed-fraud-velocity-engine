package com.zbank.fraud.config;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);
    public static final String DLT_TOPIC = "fraud-alerts-dlt";

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(@org.springframework.beans.factory.annotation.Qualifier("kafkaTemplate") KafkaOperations<?, ?> kafkaOperations) {
        // Destination resolver routing exhausted failed messages to "fraud-alerts-dlt"
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaOperations,
                (record, ex) -> {
                    log.error("🚨 Exceeded retries for fraud alert on topic '{}', partition {}. Routing to DLT topic '{}'. Cause: {}",
                            record.topic(), record.partition(), DLT_TOPIC, ex.getMessage());
                    return new TopicPartition(DLT_TOPIC, -1);
                });

        // Fixed backoff: 3 retry attempts with a 1000ms delay between attempts
        FixedBackOff backOff = new FixedBackOff(1000L, 3L);
        return new DefaultErrorHandler(recoverer, backOff);
    }
}
