package com.zbank.fraud.repository;

import com.zbank.fraud.entity.FraudAuditRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface FraudAuditRepository extends JpaRepository<FraudAuditRecord, UUID> {
    boolean existsByTransactionId(String transactionId);
    Optional<FraudAuditRecord> findByTransactionId(String transactionId);
}