package com.zbank.fraud.service;

import com.zbank.fraud.model.TransactionRequest;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

@Service
public class VelocityFraudDetector {

    public enum VelocityMode {
        LUA,
        PIPELINED,
        NON_PIPELINED
    }

    public record VelocityResult(boolean allowed, long count) {}

    private final StringRedisTemplate redisTemplate;

    @Value("${zbank.redis.velocity.mode:LUA}")
    private String configuredMode = "LUA";

    @Value("${zbank.redis.pipelining.enabled:true}")
    private boolean pipeliningEnabled = true;

    private VelocityMode mode = VelocityMode.LUA;

    // Core financial rule constraints
    private static final int MAX_TRANSACTIONS_PER_WINDOW = 3;
    private static final long TIME_WINDOW_SECONDS = 60;

    /**
     * Atomic Lua script:
     * 1. Prunes expired transactions older than windowStartMillis.
     * 2. Counts remaining active approved transactions.
     * 3. Only if currentCount < maxLimit, runs ZADD and EXPIRE (approves transaction).
     * 4. If currentCount >= maxLimit, refreshes EXPIRE without adding the transaction (declines transaction).
     * Returns a 2-element array: {allowed (1 or 0), currentCount}.
     */
    private static final String LUA_VELOCITY_SCRIPT =
            "redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[2])\n" +
            "local currentCount = redis.call('ZCARD', KEYS[1])\n" +
            "if currentCount < tonumber(ARGV[3]) then\n" +
            "    redis.call('ZADD', KEYS[1], ARGV[1], ARGV[4])\n" +
            "    redis.call('EXPIRE', KEYS[1], ARGV[5])\n" +
            "    return {1, currentCount + 1}\n" +
            "else\n" +
            "    redis.call('EXPIRE', KEYS[1], ARGV[5])\n" +
            "    return {0, currentCount}\n" +
            "end\n";

    private final RedisScript<List> redisScript = new DefaultRedisScript<>(LUA_VELOCITY_SCRIPT, List.class);

    public VelocityFraudDetector(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @PostConstruct
    public void init() {
        if ("pipelined".equalsIgnoreCase(configuredMode)) {
            this.mode = VelocityMode.PIPELINED;
        } else if ("non_pipelined".equalsIgnoreCase(configuredMode) || "sequential".equalsIgnoreCase(configuredMode)) {
            this.mode = VelocityMode.NON_PIPELINED;
        } else if (!pipeliningEnabled) {
            this.mode = VelocityMode.NON_PIPELINED;
        } else {
            this.mode = VelocityMode.LUA;
        }
    }

    public void setMode(VelocityMode mode) {
        this.mode = mode;
    }

    void setConfiguredMode(String configuredMode) {
        this.configuredMode = configuredMode;
    }

    public VelocityMode getMode() {
        return this.mode;
    }

    public void setPipeliningEnabled(boolean pipeliningEnabled) {
        this.pipeliningEnabled = pipeliningEnabled;
        if (!pipeliningEnabled) {
            this.mode = VelocityMode.NON_PIPELINED;
        } else if (this.mode == VelocityMode.NON_PIPELINED) {
            this.mode = VelocityMode.PIPELINED;
        }
    }

    public boolean isPipeliningEnabled() {
        return this.mode == VelocityMode.PIPELINED;
    }

    /**
     * Evaluates a transaction request against historical velocity rules.
     * @return true if fraudulent (declined), false if allowed (approved).
     */
    public boolean isFraudulentVelocity(TransactionRequest request) {
        return !evaluateVelocity(request).allowed();
    }

    /**
     * Evaluates velocity returning both the decision (allowed) and the transaction count.
     */
    public VelocityResult evaluateVelocity(TransactionRequest request) {
        String redisKey = "velocity:card:" + request.cardId();
        long currentTimeMillis = request.timestamp().toEpochMilli();
        long windowStartMillis = currentTimeMillis - (TIME_WINDOW_SECONDS * 1000);
        String transactionId = request.transactionId();

        if (mode == VelocityMode.LUA) {
            List<?> scriptResult = redisTemplate.execute(
                    redisScript,
                    Collections.singletonList(redisKey),
                    String.valueOf(currentTimeMillis),
                    String.valueOf(windowStartMillis),
                    String.valueOf(MAX_TRANSACTIONS_PER_WINDOW),
                    transactionId,
                    String.valueOf(TIME_WINDOW_SECONDS)
            );

            if (scriptResult != null && scriptResult.size() >= 2) {
                long allowedFlag = ((Number) scriptResult.get(0)).longValue();
                long count = ((Number) scriptResult.get(1)).longValue();
                return new VelocityResult(allowedFlag == 1L, count);
            }
            // Fallback if script execution returned empty
            return new VelocityResult(true, 1L);
        } else if (mode == VelocityMode.PIPELINED) {
            // Legacy pipelined path (kept for benchmark comparison)
            List<Object> pipelineResults = redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
                byte[] rawKey = redisTemplate.getStringSerializer().serialize(redisKey);
                byte[] rawMember = redisTemplate.getStringSerializer().serialize(transactionId);

                connection.zSetCommands().zAdd(rawKey, currentTimeMillis, rawMember);
                connection.zSetCommands().zRemRangeByScore(rawKey, 0, windowStartMillis);
                connection.zSetCommands().zCard(rawKey);
                connection.keyCommands().expire(rawKey, TIME_WINDOW_SECONDS);

                return null;
            });

            Long count = (Long) pipelineResults.get(2);
            long total = count != null ? count : 0L;
            boolean allowed = total <= MAX_TRANSACTIONS_PER_WINDOW;
            return new VelocityResult(allowed, total);
        } else {
            // Legacy non-pipelined path (4 sequential round-trips for benchmark comparison)
            redisTemplate.opsForZSet().add(redisKey, transactionId, currentTimeMillis);
            redisTemplate.opsForZSet().removeRangeByScore(redisKey, 0, windowStartMillis);
            Long count = redisTemplate.opsForZSet().zCard(redisKey);
            redisTemplate.expire(redisKey, Duration.ofSeconds(TIME_WINDOW_SECONDS));

            long total = count != null ? count : 0L;
            boolean allowed = total <= MAX_TRANSACTIONS_PER_WINDOW;
            return new VelocityResult(allowed, total);
        }
    }
}