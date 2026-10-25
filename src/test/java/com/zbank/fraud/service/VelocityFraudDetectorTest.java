package com.zbank.fraud.service;

import com.zbank.fraud.model.TransactionRequest;
import com.zbank.fraud.repository.BlacklistedMerchantRepository;
import com.zbank.fraud.repository.FraudAuditRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VelocityFraudDetectorTest extends AbstractRedisIntegrationTest {

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
        velocityFraudDetector.setMode(VelocityFraudDetector.VelocityMode.LUA);
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

    // --- TEST 6: Non-Pipelined Mode Parity ---
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
            velocityFraudDetector.setMode(VelocityFraudDetector.VelocityMode.LUA);
            redisTemplate.delete("velocity:card:" + testCardNonPipelined);
        }
    }

    // --- TEST 7: Reproduces & Resolves Problem (b) - Declined tx must not stay in sorted set ---
    @Test
    @DisplayName("Bug (b): Declined transaction must not remain in Redis sorted set or count against subsequent requests")
    void shouldNotRetainDeclinedTransactionInSortedSetOrCountAgainstLaterRequests() {
        String testCard = "card_declined_retention_test";
        try {
            Instant now = Instant.now();

            // 1. Fill 3 allowed slots in Lua mode (all approved)
            assertFalse(velocityFraudDetector.isFraudulentVelocity(createRequest(testCard, now.minusSeconds(50))));
            assertFalse(velocityFraudDetector.isFraudulentVelocity(createRequest(testCard, now.minusSeconds(40))));
            assertFalse(velocityFraudDetector.isFraudulentVelocity(createRequest(testCard, now.minusSeconds(30))));

            // 2. 4th transaction at now - 20s exceeds limit -> DECLINED
            assertTrue(velocityFraudDetector.isFraudulentVelocity(createRequest(testCard, now.minusSeconds(20))),
                    "4th transaction must be declined");

            // Assert: Declined transaction must NOT be retained in the Redis Sorted Set
            Long countInSet = redisTemplate.opsForZSet().zCard("velocity:card:" + testCard);
            assertEquals(3L, countInSet,
                    "Declined transaction must NOT remain in Redis sorted set (expected 3 approved, but was " + countInSet + ")");

            // 3. At now + 15s: Tx 1 (at now - 50s, so 65s ago) has expired outside 60s window.
            // Active approved transactions inside window: Tx 2 (55s ago) and Tx 3 (45s ago). Total = 2.
            // If declined Tx 4 was retained, total count would be 3 + 1 = 4 -> falsely declined.
            // Since Tx 4 was declined, it must not count -> Tx 5 arriving at now + 15s must be APPROVED!
            assertFalse(velocityFraudDetector.isFraudulentVelocity(createRequest(testCard, now.plusSeconds(15))),
                    "Transaction must be approved after 1st expires, because declined 4th transaction should not count");
        } finally {
            redisTemplate.delete("velocity:card:" + testCard);
        }
    }

    // --- TEST 8: Reproduces & Resolves Problem (a) - Concurrent requests on last slot ---
    @Test
    @DisplayName("Bug (a): Two concurrent requests hitting the last available slot must not both be declined")
    void shouldApproveOneAndDeclineOneWhenTwoConcurrentRequestsHitLastAvailableSlot() throws InterruptedException {
        String testCard = "card_concurrent_race_test";
        try {
            Instant now = Instant.now();

            // 1. Occupy 2 of 3 allowed slots in the 60s window
            assertFalse(velocityFraudDetector.isFraudulentVelocity(createRequest(testCard, now.minusSeconds(20))));
            assertFalse(velocityFraudDetector.isFraudulentVelocity(createRequest(testCard, now.minusSeconds(10))));

            // 2. Exactly 1 slot remains before limit (3) is breached.
            // Launch 2 concurrent requests hitting Redis simultaneously.
            int concurrentRequests = 2;
            ExecutorService executor = Executors.newFixedThreadPool(concurrentRequests);
            CountDownLatch readyLatch = new CountDownLatch(concurrentRequests);
            CountDownLatch startLatch = new CountDownLatch(1);
            AtomicInteger approvedCount = new AtomicInteger(0);
            AtomicInteger declinedCount = new AtomicInteger(0);

            for (int i = 0; i < concurrentRequests; i++) {
                executor.submit(() -> {
                    readyLatch.countDown();
                    try {
                        startLatch.await();
                        boolean fraudulent = velocityFraudDetector.isFraudulentVelocity(createRequest(testCard, Instant.now()));
                        if (fraudulent) {
                            declinedCount.incrementAndGet();
                        } else {
                            approvedCount.incrementAndGet();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }

            readyLatch.await(5, TimeUnit.SECONDS);
            startLatch.countDown();
            executor.shutdown();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));

            assertEquals(1, approvedCount.get(),
                    "Exactly one of the two concurrent requests must be approved when 1 slot remains (race condition caused both to be declined)");
            assertEquals(1, declinedCount.get(),
                    "Exactly one of the two concurrent requests must be declined when 1 slot remains");
        } finally {
            redisTemplate.delete("velocity:card:" + testCard);
        }
    }

    // --- TEST 9: Detailed decision and count evaluation ---
    @Test
    @DisplayName("evaluateVelocity returns allowed status and accurate count")
    void shouldReturnAllowedStatusAndAccurateCountViaEvaluateVelocity() {
        String testCard = "card_evaluate_details";
        try {
            Instant now = Instant.now();
            VelocityFraudDetector.VelocityResult res1 = velocityFraudDetector.evaluateVelocity(createRequest(testCard, now));
            assertTrue(res1.allowed());
            assertEquals(1L, res1.count());

            VelocityFraudDetector.VelocityResult res2 = velocityFraudDetector.evaluateVelocity(createRequest(testCard, now.plusSeconds(1)));
            assertTrue(res2.allowed());
            assertEquals(2L, res2.count());

            VelocityFraudDetector.VelocityResult res3 = velocityFraudDetector.evaluateVelocity(createRequest(testCard, now.plusSeconds(2)));
            assertTrue(res3.allowed());
            assertEquals(3L, res3.count());

            // 4th is declined: count remains 3 (not incremented)
            VelocityFraudDetector.VelocityResult res4 = velocityFraudDetector.evaluateVelocity(createRequest(testCard, now.plusSeconds(3)));
            assertFalse(res4.allowed());
            assertEquals(3L, res4.count());
        } finally {
            redisTemplate.delete("velocity:card:" + testCard);
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