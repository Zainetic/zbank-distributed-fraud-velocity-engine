package com.zbank.fraud.service;

import com.zbank.fraud.model.TransactionRequest;
import com.zbank.fraud.repository.BlacklistedMerchantRepository;
import com.zbank.fraud.repository.FraudAuditRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
class VelocityFraudDetectorTest {

    @Autowired
    private VelocityFraudDetector velocityFraudDetector;

    @Autowired
    private StringRedisTemplate redisTemplate;

    // Mocking external infrastructure to keep Redis tests isolated and fast
    @MockBean private BlacklistedMerchantRepository merchantRepository;
    @MockBean private FraudAuditRepository auditRepository;
    @MockBean private KafkaTemplate<String, Object> kafkaTemplate;

    private final String TEST_CARD_A = "card_111111";
    private final String TEST_CARD_B = "card_222222";
    private final String TEST_MCC = "5411"; // Grocery

    @AfterEach
    void cleanUp() {
        redisTemplate.delete("velocity:card:" + TEST_CARD_A);
        redisTemplate.delete("velocity:card:" + TEST_CARD_B);
    }

    // --- TEST 1: The Smoke Test ---
    @Test
    void shouldApproveSingleValidTransaction() {
        TransactionRequest req = createRequest(TEST_CARD_A, Instant.now());
        assertFalse(velocityFraudDetector.isFraudulentVelocity(req), "A single transaction should be approved");
    }

    // --- TEST 2: The Upper Boundary Edge Case ---
    @Test
    void shouldApproveExactlyThreeTransactionsWithinSixtySeconds() {
        Instant now = Instant.now();
        
        assertFalse(velocityFraudDetector.isFraudulentVelocity(createRequest(TEST_CARD_A, now)));
        assertFalse(velocityFraudDetector.isFraudulentVelocity(createRequest(TEST_CARD_A, now.plusSeconds(1))));
        assertFalse(velocityFraudDetector.isFraudulentVelocity(createRequest(TEST_CARD_A, now.plusSeconds(2))));
        // Total: 3 transactions. All should pass.
    }

    // --- TEST 3: The Velocity Breach ---
    @Test
    void shouldDeclineFourthTransactionWithinSixtySeconds() {
        Instant now = Instant.now();

        // 3 valid transactions
        velocityFraudDetector.isFraudulentVelocity(createRequest(TEST_CARD_A, now));
        velocityFraudDetector.isFraudulentVelocity(createRequest(TEST_CARD_A, now.plusSeconds(1)));
        velocityFraudDetector.isFraudulentVelocity(createRequest(TEST_CARD_A, now.plusSeconds(2)));

        // The 4th transaction hits the sliding window limit
        TransactionRequest fraudulentReq = createRequest(TEST_CARD_A, now.plusSeconds(5));
        assertTrue(velocityFraudDetector.isFraudulentVelocity(fraudulentReq), "4th transaction MUST be declined");
    }

    // --- TEST 4: The Sliding Window Drop (Time Travel) ---
    @Test
    void shouldApproveFourthTransactionIfOldestTransactionFellOutsideWindow() {
        Instant now = Instant.now();

        // Transaction 1: Happened 65 seconds ago (Outside the 60s window)
        velocityFraudDetector.isFraudulentVelocity(createRequest(TEST_CARD_A, now.minusSeconds(65)));
        
        // Transaction 2 & 3: Happened recently
        velocityFraudDetector.isFraudulentVelocity(createRequest(TEST_CARD_A, now.minusSeconds(30)));
        velocityFraudDetector.isFraudulentVelocity(createRequest(TEST_CARD_A, now.minusSeconds(10)));

        // Transaction 4: Happening right now
        // Because Tx 1 is > 60 seconds old, Redis zRemRangeByScore will delete it.
        // The window now only contains Tx 2, Tx 3, and this new Tx 4. 
        // Total inside window = 3. It should be APPROVED.
        TransactionRequest boundaryReq = createRequest(TEST_CARD_A, now);
        assertFalse(velocityFraudDetector.isFraudulentVelocity(boundaryReq), "Should approve because old transactions expired");
    }

    // --- TEST 5: Data Isolation / Cross-Pollination ---
    @Test
    void shouldTrackVelocityIndependentlyForDifferentCards() {
        Instant now = Instant.now();

        // Card A maxes out its velocity limit
        velocityFraudDetector.isFraudulentVelocity(createRequest(TEST_CARD_A, now));
        velocityFraudDetector.isFraudulentVelocity(createRequest(TEST_CARD_A, now.plusSeconds(1)));
        velocityFraudDetector.isFraudulentVelocity(createRequest(TEST_CARD_A, now.plusSeconds(2)));
        assertTrue(velocityFraudDetector.isFraudulentVelocity(createRequest(TEST_CARD_A, now.plusSeconds(3))), "Card A should be blocked");

        // Card B makes a transaction at the exact same time Card A was blocked
        TransactionRequest cardBReq = createRequest(TEST_CARD_B, now.plusSeconds(3));
        
        // Card B must NOT be blocked by Card A's velocity
        assertFalse(velocityFraudDetector.isFraudulentVelocity(cardBReq), "Card B should be approved independently of Card A");
    }

    @Test
    void shouldEnforceVelocityLimitEquallyInNonPipelinedMode() {
        String testCardNonPipelined = "card_non_pipelined_99";
        try {
            velocityFraudDetector.setPipeliningEnabled(false);
            Instant now = Instant.now();

            assertFalse(velocityFraudDetector.isFraudulentVelocity(createRequest(testCardNonPipelined, now)));
            assertFalse(velocityFraudDetector.isFraudulentVelocity(createRequest(testCardNonPipelined, now.plusSeconds(1))));
            assertFalse(velocityFraudDetector.isFraudulentVelocity(createRequest(testCardNonPipelined, now.plusSeconds(2))));
            assertTrue(velocityFraudDetector.isFraudulentVelocity(createRequest(testCardNonPipelined, now.plusSeconds(3))),
                    "4th transaction in non-pipelined mode must also be declined");
        } finally {
            velocityFraudDetector.setPipeliningEnabled(true);
            redisTemplate.delete("velocity:card:" + testCardNonPipelined);
        }
    }

    // Helper method to keep test code clean and readable
    private TransactionRequest createRequest(String cardId, Instant timestamp) {
        return new TransactionRequest(
                UUID.randomUUID().toString(),
                cardId,
                "acc_123",
                new BigDecimal("25.00"),
                "EUR",
                TEST_MCC,
                timestamp
        );
    }
}