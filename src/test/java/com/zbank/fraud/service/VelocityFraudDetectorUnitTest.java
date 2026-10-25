package com.zbank.fraud.service;

import com.zbank.fraud.model.TransactionRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisKeyCommands;
import org.springframework.data.redis.connection.RedisZSetCommands;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.serializer.RedisSerializer;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class VelocityFraudDetectorUnitTest {

    @Mock
    private StringRedisTemplate redisTemplate;

    @Mock
    private ZSetOperations<String, String> zSetOperations;

    private VelocityFraudDetector detector;

    @BeforeEach
    void setUp() {
        detector = new VelocityFraudDetector(redisTemplate);
        detector.init();
    }

    @Test
    @DisplayName("Lua mode: approved when script returns allowed=1")
    void shouldApproveInLuaModeWhenScriptAllows() {
        detector.setMode(VelocityFraudDetector.VelocityMode.LUA);
        when(redisTemplate.execute(any(RedisScript.class), anyList(), any(), any(), any(), any(), any()))
                .thenReturn(List.of(1L, 2L));

        TransactionRequest req = createRequest("card_unit_1", Instant.now());
        VelocityFraudDetector.VelocityResult result = detector.evaluateVelocity(req);

        assertTrue(result.allowed());
        assertEquals(2L, result.count());
        assertFalse(detector.isFraudulentVelocity(req));
    }

    @Test
    @DisplayName("Lua mode: declined when script returns allowed=0")
    void shouldDeclineInLuaModeWhenScriptDeclines() {
        detector.setMode(VelocityFraudDetector.VelocityMode.LUA);
        when(redisTemplate.execute(any(RedisScript.class), anyList(), any(), any(), any(), any(), any()))
                .thenReturn(List.of(0L, 3L));

        TransactionRequest req = createRequest("card_unit_2", Instant.now());
        VelocityFraudDetector.VelocityResult result = detector.evaluateVelocity(req);

        assertFalse(result.allowed());
        assertEquals(3L, result.count());
        assertTrue(detector.isFraudulentVelocity(req));
    }

    @Test
    @DisplayName("Lua mode: fallback when script returns empty result")
    void shouldHandleEmptyScriptResultGracefully() {
        detector.setMode(VelocityFraudDetector.VelocityMode.LUA);
        when(redisTemplate.execute(any(RedisScript.class), anyList(), any(), any(), any(), any(), any()))
                .thenReturn(Collections.emptyList());

        TransactionRequest req = createRequest("card_unit_fallback", Instant.now());
        VelocityFraudDetector.VelocityResult result = detector.evaluateVelocity(req);

        assertTrue(result.allowed());
        assertFalse(detector.isFraudulentVelocity(req));
    }

    @Test
    @DisplayName("Pipelined mode: evaluates correctly via executePipelined callback")
    void shouldEvaluateInPipelinedMode() {
        detector.setMode(VelocityFraudDetector.VelocityMode.PIPELINED);
        assertTrue(detector.isPipeliningEnabled());

        when(redisTemplate.executePipelined(any(RedisCallback.class)))
                .thenAnswer(invocation -> {
                    RedisCallback<?> callback = invocation.getArgument(0);
                    RedisConnection connection = mock(RedisConnection.class);
                    RedisZSetCommands zSetCommands = mock(RedisZSetCommands.class);
                    RedisKeyCommands keyCommands = mock(RedisKeyCommands.class);
                    when(connection.zSetCommands()).thenReturn(zSetCommands);
                    when(connection.keyCommands()).thenReturn(keyCommands);
                    when(redisTemplate.getStringSerializer()).thenReturn(RedisSerializer.string());

                    callback.doInRedis(connection);
                    return List.of(true, 0L, 2L, true); // index 2 is zCard count
                });

        TransactionRequest req = createRequest("card_piped", Instant.now());
        VelocityFraudDetector.VelocityResult result = detector.evaluateVelocity(req);

        assertTrue(result.allowed());
        assertEquals(2L, result.count());
    }

    @Test
    @DisplayName("Non-Pipelined mode: executes sequential redis operations")
    void shouldEvaluateInNonPipelinedMode() {
        detector.setPipeliningEnabled(false);
        assertEquals(VelocityFraudDetector.VelocityMode.NON_PIPELINED, detector.getMode());
        assertFalse(detector.isPipeliningEnabled());

        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(zSetOperations.zCard(anyString())).thenReturn(4L);

        TransactionRequest req = createRequest("card_seq", Instant.now());
        VelocityFraudDetector.VelocityResult result = detector.evaluateVelocity(req);

        assertFalse(result.allowed());
        assertEquals(4L, result.count());
        assertTrue(detector.isFraudulentVelocity(req));
        verify(redisTemplate, org.mockito.Mockito.times(2)).expire(eq("velocity:card:card_seq"), any(Duration.class));
    }

    @Test
    @DisplayName("setPipeliningEnabled(true) restores PIPELINED mode")
    void shouldRestorePipelinedModeWhenEnabled() {
        detector.setPipeliningEnabled(false);
        assertEquals(VelocityFraudDetector.VelocityMode.NON_PIPELINED, detector.getMode());

        detector.setPipeliningEnabled(true);
        assertEquals(VelocityFraudDetector.VelocityMode.PIPELINED, detector.getMode());
    }

    @Test
    @DisplayName("init() initializes PIPELINED mode when configuredMode is 'pipelined'")
    void shouldInitPipelinedMode() {
        detector.setConfiguredMode("pipelined");
        detector.init();
        assertEquals(VelocityFraudDetector.VelocityMode.PIPELINED, detector.getMode());
    }

    @Test
    @DisplayName("init() initializes NON_PIPELINED mode when configuredMode is 'non_pipelined'")
    void shouldInitNonPipelinedMode() {
        detector.setConfiguredMode("non_pipelined");
        detector.init();
        assertEquals(VelocityFraudDetector.VelocityMode.NON_PIPELINED, detector.getMode());
    }

    @Test
    @DisplayName("init() initializes NON_PIPELINED mode when configuredMode is 'sequential'")
    void shouldInitSequentialMode() {
        detector.setConfiguredMode("sequential");
        detector.init();
        assertEquals(VelocityFraudDetector.VelocityMode.NON_PIPELINED, detector.getMode());
    }

    @Test
    @DisplayName("init() initializes NON_PIPELINED mode when pipeliningEnabled is false")
    void shouldInitNonPipelinedModeWhenPipeliningDisabled() {
        detector.setConfiguredMode("other");
        detector.setPipeliningEnabled(false);
        detector.init();
        assertEquals(VelocityFraudDetector.VelocityMode.NON_PIPELINED, detector.getMode());
    }

    @Test
    @DisplayName("Pipelined mode handles null count gracefully")
    void shouldHandleNullCountInPipelinedMode() {
        detector.setMode(VelocityFraudDetector.VelocityMode.PIPELINED);
        when(redisTemplate.executePipelined(any(RedisCallback.class)))
                .thenReturn(List.of(true, 0L, 0L)); // null-like / 0
        TransactionRequest req = createRequest("card_null_pipe", Instant.now());
        VelocityFraudDetector.VelocityResult result = detector.evaluateVelocity(req);
        assertTrue(result.allowed());
    }

    @Test
    @DisplayName("Non-pipelined mode handles null count gracefully")
    void shouldHandleNullCountInNonPipelinedMode() {
        detector.setPipeliningEnabled(false);
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(zSetOperations.zCard(anyString())).thenReturn(null);

        TransactionRequest req = createRequest("card_null_seq", Instant.now());
        VelocityFraudDetector.VelocityResult result = detector.evaluateVelocity(req);
        assertTrue(result.allowed());
        assertEquals(0L, result.count());
    }

    private TransactionRequest createRequest(String cardId, Instant timestamp) {
        return new TransactionRequest(
                UUID.randomUUID().toString(),
                cardId,
                "acc_unit",
                new BigDecimal("25.00"),
                "EUR",
                "5411",
                timestamp
        );
    }
}
