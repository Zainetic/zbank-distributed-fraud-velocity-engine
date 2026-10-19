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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@Testcontainers
class RedisVelocitySlidingWindowTestcontainersTest {

    @Container
    static GenericContainer<?> redisContainer = new GenericContainer<>(DockerImageName.parse("redis:7.2-alpine"))
            .withExposedPorts(6379);

    @DynamicPropertySource
    static void configureRedis(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", redisContainer::getHost);
        registry.add("spring.data.redis.port", () -> redisContainer.getMappedPort(6379));
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
