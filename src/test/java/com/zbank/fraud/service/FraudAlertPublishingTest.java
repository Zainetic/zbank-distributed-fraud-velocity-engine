package com.zbank.fraud.service;

import com.zbank.fraud.controller.FraudCheckController;
import com.zbank.fraud.entity.FraudAuditRecord;
import com.zbank.fraud.event.FraudAlertEvent;
import com.zbank.fraud.model.TransactionRequest;
import com.zbank.fraud.repository.BlacklistedMerchantRepository;
import com.zbank.fraud.repository.FraudAuditRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class FraudAlertPublishingTest {

    @Mock
    private VelocityFraudDetector velocityFraudDetector;

    @Mock
    private BlacklistedMerchantRepository merchantRepository;

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Mock
    private FraudAuditRepository auditRepository;

    @InjectMocks
    private FraudCheckController fraudCheckController;

    @Test
    @DisplayName("Declined transaction due to velocity limit publishes FraudAlertEvent to Kafka")
    void shouldPublishFraudAlertEventWhenVelocityExceeded() {
        TransactionRequest request = new TransactionRequest(
                UUID.randomUUID().toString(),
                "card_velocity_breach",
                "acc_123",
                new BigDecimal("150.00"),
                "USD",
                "5411",
                Instant.now()
        );

        when(merchantRepository.existsByMerchantCategoryCode("5411")).thenReturn(false);
        when(velocityFraudDetector.isFraudulentVelocity(any(TransactionRequest.class))).thenReturn(true);

        ResponseEntity<FraudCheckController.FraudCheckResponse> response = fraudCheckController.checkVelocity(request);

        assertNotNull(response.getBody());
        assertTrue(response.getBody().isFraudulent());
        assertEquals("DECLINED_VELOCITY_LIMIT_EXCEEDED", response.getBody().statusReason());

        ArgumentCaptor<FraudAlertEvent> eventCaptor = ArgumentCaptor.forClass(FraudAlertEvent.class);
        verify(kafkaTemplate, times(1)).send(eq("fraud-alerts"), eventCaptor.capture());

        FraudAlertEvent publishedEvent = eventCaptor.getValue();
        assertEquals(request.transactionId(), publishedEvent.transactionId());
        assertEquals(request.cardId(), publishedEvent.cardId());
        assertEquals(request.amount(), publishedEvent.attemptedAmount());
        assertEquals("DECLINED_VELOCITY_LIMIT_EXCEEDED", publishedEvent.declineReason());
        assertNotNull(publishedEvent.timestamp());
    }

    @Test
    @DisplayName("Declined transaction due to blacklisted merchant publishes FraudAlertEvent to Kafka")
    void shouldPublishFraudAlertEventWhenMerchantBlacklisted() {
        TransactionRequest request = new TransactionRequest(
                UUID.randomUUID().toString(),
                "card_blacklisted_mcc",
                "acc_456",
                new BigDecimal("200.00"),
                "USD",
                "7995", // Gambling MCC
                Instant.now()
        );

        when(merchantRepository.existsByMerchantCategoryCode("7995")).thenReturn(true);

        ResponseEntity<FraudCheckController.FraudCheckResponse> response = fraudCheckController.checkVelocity(request);

        assertNotNull(response.getBody());
        assertTrue(response.getBody().isFraudulent());
        assertEquals("DECLINED_BLACKLISTED_MERCHANT", response.getBody().statusReason());

        ArgumentCaptor<FraudAlertEvent> eventCaptor = ArgumentCaptor.forClass(FraudAlertEvent.class);
        verify(kafkaTemplate, times(1)).send(eq("fraud-alerts"), eventCaptor.capture());

        FraudAlertEvent publishedEvent = eventCaptor.getValue();
        assertEquals(request.transactionId(), publishedEvent.transactionId());
        assertEquals(request.cardId(), publishedEvent.cardId());
        assertEquals(request.amount(), publishedEvent.attemptedAmount());
        assertEquals("DECLINED_BLACKLISTED_MERCHANT", publishedEvent.declineReason());
    }

    @Test
    @DisplayName("Approved transaction does not publish any FraudAlertEvent to Kafka")
    void shouldNotPublishEventWhenTransactionApproved() {
        TransactionRequest request = new TransactionRequest(
                UUID.randomUUID().toString(),
                "card_clean",
                "acc_789",
                new BigDecimal("35.00"),
                "USD",
                "5411",
                Instant.now()
        );

        when(merchantRepository.existsByMerchantCategoryCode("5411")).thenReturn(false);
        when(velocityFraudDetector.isFraudulentVelocity(any(TransactionRequest.class))).thenReturn(false);

        ResponseEntity<FraudCheckController.FraudCheckResponse> response = fraudCheckController.checkVelocity(request);

        assertNotNull(response.getBody());
        assertFalse(response.getBody().isFraudulent());
        assertEquals("APPROVED", response.getBody().statusReason());

        verify(kafkaTemplate, never()).send(anyString(), any());
    }

    @Test
    @DisplayName("FraudAlertListener consumes FraudAlertEvent and persists FraudAuditRecord")
    void shouldPersistAuditRecordWhenEventConsumed() {
        when(auditRepository.existsByTransactionId("tx_123")).thenReturn(false);

        FraudAlertListener listener = new FraudAlertListener(auditRepository);
        FraudAlertEvent event = new FraudAlertEvent(
                "tx_123",
                "card_alert",
                new BigDecimal("99.99"),
                "DECLINED_VELOCITY_LIMIT_EXCEEDED",
                Instant.now()
        );

        listener.consumeFraudAlert(event);

        ArgumentCaptor<FraudAuditRecord> auditCaptor = ArgumentCaptor.forClass(FraudAuditRecord.class);
        verify(auditRepository, times(1)).save(auditCaptor.capture());

        FraudAuditRecord savedRecord = auditCaptor.getValue();
        assertEquals("tx_123", savedRecord.getTransactionId());
        assertEquals("card_alert", savedRecord.getCardId());
        assertEquals(new BigDecimal("99.99"), savedRecord.getAttemptedAmount());
        assertEquals("DECLINED_VELOCITY_LIMIT_EXCEEDED", savedRecord.getDeclineReason());
        assertNotNull(savedRecord.getAlertTimestamp());
    }

    @Test
    @DisplayName("FraudAlertListener skips duplicate alerts idempotently without writing to DB")
    void shouldSkipPersistenceWhenDuplicateAlertConsumed() {
        when(auditRepository.existsByTransactionId("tx_duplicate")).thenReturn(true);

        FraudAlertListener listener = new FraudAlertListener(auditRepository);
        FraudAlertEvent duplicateEvent = new FraudAlertEvent(
                "tx_duplicate",
                "card_alert",
                new BigDecimal("99.99"),
                "DECLINED_VELOCITY_LIMIT_EXCEEDED",
                Instant.now()
        );

        listener.consumeFraudAlert(duplicateEvent);

        verify(auditRepository, never()).save(any(FraudAuditRecord.class));
    }

    @Test
    @DisplayName("FraudAlertListener catches DataIntegrityViolationException without throwing")
    void shouldHandleDataIntegrityViolationGracefully() {
        when(auditRepository.existsByTransactionId("tx_concurrent")).thenReturn(false);
        when(auditRepository.save(any(FraudAuditRecord.class)))
                .thenThrow(new org.springframework.dao.DataIntegrityViolationException("Duplicate key violation"));

        FraudAlertListener listener = new FraudAlertListener(auditRepository);
        FraudAlertEvent event = new FraudAlertEvent(
                "tx_concurrent",
                "card_alert",
                new BigDecimal("50.00"),
                "DECLINED_VELOCITY_LIMIT_EXCEEDED",
                Instant.now()
        );

        assertDoesNotThrow(() -> listener.consumeFraudAlert(event));
        verify(auditRepository, times(1)).save(any(FraudAuditRecord.class));
    }
}
