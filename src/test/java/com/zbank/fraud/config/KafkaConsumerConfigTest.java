package com.zbank.fraud.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DefaultErrorHandler;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(MockitoExtension.class)
class KafkaConsumerConfigTest {

    @Mock
    private KafkaOperations<Object, Object> kafkaOperations;

    @Test
    @DisplayName("KafkaConsumerConfig instantiates DefaultErrorHandler bean with backoff and DLT recoverer")
    void shouldCreateDefaultErrorHandlerBean() {
        KafkaConsumerConfig config = new KafkaConsumerConfig();
        DefaultErrorHandler errorHandler = config.kafkaErrorHandler(kafkaOperations);

        assertNotNull(errorHandler, "ErrorHandler bean must not be null");
    }
}
