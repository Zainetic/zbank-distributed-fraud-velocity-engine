package com.zbank.fraud.service;

import com.zbank.fraud.model.TransactionRequest;
import com.zbank.fraud.repository.BlacklistedMerchantRepository;
import com.zbank.fraud.repository.FraudAuditRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@Testcontainers(disabledWithoutDocker = true)
class RedisVelocitySlidingWindowTestcontainersTest {

    @Container
    static final GenericContainer<?> redisContainer = new GenericContainer<>(DockerImageName.parse("redis:7.2-alpine"))
            .withExposedPorts(6379);

    static {
        try {
            redisContainer.start();
        } catch (Exception ignored) {
            // Docker daemon inactive in local environment; test will be skipped by disabledWithoutDocker
        }
    }

    @DynamicPropertySource
    static void configureRedis(DynamicPropertyRegistry registry) {
        if (redisContainer.isRunning()) {
            registry.add("spring.data.redis.host", redisContainer::getHost);
            registry.add("spring.data.redis.port", () -> redisContainer.getMappedPort(6379));
        }
    }

    @Autowired
    private VelocityFraudDetector velocityFraudDetector;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @MockBean
    private BlacklistedMerchantRepository merchantRepository;

    @MockBean
    private FraudAuditRepository auditRepository;

    @MockBean
    private KafkaTemplate<String, Object> kafkaTemplate;

    private static final String TEST_CARD = "card_testcontainers_99";
    private static final String TEST_MCC = "5411";

    @AfterEach
    void cleanUp() {
        redisTemplate.delete("velocity:card:" + TEST_CARD);
    }

    @Test
    @DisplayName("Requests under limit pass, next is declined, and requests pass after window expires")
    void shouldEnforceSlidingWindowLimitAndAllowRequestsAfterWindowExpires() {
        Instant baseTime = Instant.now();

        // 1. Requests under the limit pass (up to 3 within 60s)
        TransactionRequest req1 = createRequest(TEST_CARD, baseTime);
        TransactionRequest req2 = createRequest(TEST_CARD, baseTime.plusSeconds(5));
        TransactionRequest req3 = createRequest(TEST_CARD, baseTime.plusSeconds(10));

        assertFalse(velocityFraudDetector.isFraudulentVelocity(req1), "1st transaction under limit must be approved");
        assertFalse(velocityFraudDetector.isFraudulentVelocity(req2), "2nd transaction under limit must be approved");
        assertFalse(velocityFraudDetector.isFraudulentVelocity(req3), "3rd transaction under limit must be approved");

        // 2. The next one is declined (4th within 60s window)
        TransactionRequest req4 = createRequest(TEST_CARD, baseTime.plusSeconds(15));
        assertTrue(velocityFraudDetector.isFraudulentVelocity(req4), "4th transaction exceeding limit must be declined");

        // 3. The window expires: after 76 seconds, earlier transactions fall outside the 60s sliding window
        TransactionRequest reqAfterWindow = createRequest(TEST_CARD, baseTime.plusSeconds(76));
        assertFalse(velocityFraudDetector.isFraudulentVelocity(reqAfterWindow),
                "Transaction arriving after older transactions expired outside the 60s window must be approved");
    }

    @Test
    @DisplayName("Under concurrent load for the same card, at most 3 transactions are approved")
    void shouldEnforceVelocityLimitUnderConcurrentRequests() throws InterruptedException {
        String concurrentCard = "card_concurrent_test_88";
        int totalRequests = 10;
        java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newFixedThreadPool(totalRequests);
        java.util.concurrent.CountDownLatch readyLatch = new java.util.concurrent.CountDownLatch(totalRequests);
        java.util.concurrent.CountDownLatch startLatch = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.atomic.AtomicInteger approvedCount = new java.util.concurrent.atomic.AtomicInteger(0);
        java.util.concurrent.atomic.AtomicInteger declinedCount = new java.util.concurrent.atomic.AtomicInteger(0);

        try {
            for (int i = 0; i < totalRequests; i++) {
                executor.submit(() -> {
                    readyLatch.countDown();
                    try {
                        startLatch.await();
                        TransactionRequest req = createRequest(concurrentCard, Instant.now());
                        boolean fraudulent = velocityFraudDetector.isFraudulentVelocity(req);
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

            readyLatch.await(5, java.util.concurrent.TimeUnit.SECONDS);
            startLatch.countDown(); // release all threads simultaneously
            executor.shutdown();
            assertTrue(executor.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS));

            assertEquals(totalRequests, approvedCount.get() + declinedCount.get(),
                    "All concurrent transactions must complete");
            assertTrue(approvedCount.get() <= 3,
                    "Never more than 3 transactions can be approved in a 60s window (approved: " + approvedCount.get() + ")");
            assertTrue(declinedCount.get() >= 7,
                    "At least 7 transactions must be declined when 10 hit concurrently (declined: " + declinedCount.get() + ")");
        } finally {
            redisTemplate.delete("velocity:card:" + concurrentCard);
        }
    }

    @Test
    @DisplayName("Transaction at exact 60s boundary is pruned while 59s transaction is retained")
    void shouldPruneTransactionAtExactSixtySecondsBoundary() {
        String boundaryCard = "card_boundary_test_77";
        try {
            Instant now = Instant.now();

            // 1. Transaction at exactly 60.000s ago is pruned by ZREMRANGEBYSCORE (0 to windowStartMillis)
            // Tx 1: exactly 60 seconds ago
            velocityFraudDetector.isFraudulentVelocity(createRequest(boundaryCard, now.minusSeconds(60)));
            // Tx 2 & 3: recent transactions
            velocityFraudDetector.isFraudulentVelocity(createRequest(boundaryCard, now.minusSeconds(30)));
            velocityFraudDetector.isFraudulentVelocity(createRequest(boundaryCard, now.minusSeconds(10)));

            // 4th transaction arrives at 'now'. The one from 60s ago is evicted, so active count is 3 -> APPROVED
            TransactionRequest reqAtBoundary = createRequest(boundaryCard, now);
            assertFalse(velocityFraudDetector.isFraudulentVelocity(reqAtBoundary),
                    "Transaction at exactly 60s boundary must be evicted, allowing 4th transaction to be approved");

            // Clean up boundaryCard for the next sub-case
            redisTemplate.delete("velocity:card:" + boundaryCard);

            // 2. In contrast, transaction at 59s ago is RETAINED (inside the window)
            velocityFraudDetector.isFraudulentVelocity(createRequest(boundaryCard, now.minusSeconds(59)));
            velocityFraudDetector.isFraudulentVelocity(createRequest(boundaryCard, now.minusSeconds(30)));
            velocityFraudDetector.isFraudulentVelocity(createRequest(boundaryCard, now.minusSeconds(10)));

            // 4th transaction arrives at 'now'. The 59s transaction is still valid -> DECLINED
            TransactionRequest reqInsideWindow = createRequest(boundaryCard, now);
            assertTrue(velocityFraudDetector.isFraudulentVelocity(reqInsideWindow),
                    "Transaction at 59s is inside the 60s window, so 4th transaction must be declined");
        } finally {
            redisTemplate.delete("velocity:card:" + boundaryCard);
        }
    }

    private TransactionRequest createRequest(String cardId, Instant timestamp) {
        return new TransactionRequest(
                UUID.randomUUID().toString(),
                cardId,
                "acc_test",
                new BigDecimal("50.00"),
                "USD",
                TEST_MCC,
                timestamp
        );
    }
}
