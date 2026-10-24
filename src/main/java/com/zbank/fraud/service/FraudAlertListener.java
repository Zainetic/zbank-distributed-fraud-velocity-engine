package com.zbank.fraud.service;

import com.zbank.fraud.entity.FraudAuditRecord;
import com.zbank.fraud.event.FraudAlertEvent;
import com.zbank.fraud.repository.FraudAuditRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class FraudAlertListener {

    private static final Logger log = LoggerFactory.getLogger(FraudAlertListener.class);
    
    private final FraudAuditRepository auditRepository;

    public FraudAlertListener(FraudAuditRepository auditRepository) {
        this.auditRepository = auditRepository;
    }

    @KafkaListener(topics = "fraud-alerts", groupId = "zbank-fraud-group")
    public void consumeFraudAlert(FraudAlertEvent event) {
        
        log.warn("🚨 ASYNC KAFKA ALERT RECEIVED: Processing decline for Card {} due to: {}", 
                 event.cardId(), event.declineReason());

        // Idempotency check: prevent duplicate audit logs on redelivered events
        if (auditRepository.existsByTransactionId(event.transactionId())) {
            log.warn("⚠️ Duplicate alert received for transactionId: {}. Skipping audit persistence (idempotent).", 
                     event.transactionId());
            return;
        }

        // Map the event payload to our database entity
        FraudAuditRecord auditLog = new FraudAuditRecord(
                event.transactionId(),
                event.cardId(),
                event.attemptedAmount(),
                event.declineReason(),
                event.timestamp()
        );

        // Save it to PostgreSQL asynchronously with integrity protection
        try {
            auditRepository.save(auditLog);
            log.info("💾 Successfully persisted audit log for transaction: {}", event.transactionId());
        } catch (org.springframework.dao.DataIntegrityViolationException e) {
            log.warn("⚠️ Duplicate audit entry detected via DB constraint for transaction: {}. Handled idempotently.", 
                     event.transactionId());
        }
    }
}