package com.zbank.fraud.entity;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "fraud_audit_logs")
public class FraudAuditRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "transaction_id", nullable = false, unique = true)
    private String transactionId;
    private String cardId;
    private BigDecimal attemptedAmount;
    private String declineReason;
    private Instant alertTimestamp;

    // JPA Requires a no-args constructor
    protected FraudAuditRecord() {}

    public FraudAuditRecord(String transactionId, String cardId, BigDecimal attemptedAmount, String declineReason, Instant alertTimestamp) {
        this.transactionId = transactionId;
        this.cardId = cardId;
        this.attemptedAmount = attemptedAmount;
        this.declineReason = declineReason;
        this.alertTimestamp = alertTimestamp;
    }

    // Getters
    public UUID getId() { return id; }
    public String getTransactionId() { return transactionId; }
    public String getCardId() { return cardId; }
    public BigDecimal getAttemptedAmount() { return attemptedAmount; }
    public String getDeclineReason() { return declineReason; }
    public Instant getAlertTimestamp() { return alertTimestamp; }
}