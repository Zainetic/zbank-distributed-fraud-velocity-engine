package com.zbank.fraud.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

@Component
public class FraudMetrics {

    private final MeterRegistry meterRegistry;
    private final Timer velocityCheckTimer;
    private final Counter kafkaPublishFailures;

    public FraudMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        this.velocityCheckTimer = Timer.builder("zbank.velocity.check.time")
                .description("Latency of Redis sliding-window velocity checks")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(meterRegistry);
        this.kafkaPublishFailures = Counter.builder("zbank.kafka.publish.failures")
                .description("Total number of Kafka alert publishing failures")
                .register(meterRegistry);
    }

    /**
     * Records a fraud evaluation decision with decision and reason tags.
     *
     * @param decision "APPROVED" or "DECLINED"
     * @param reason   e.g. "NONE", "VELOCITY_LIMIT_EXCEEDED", "BLACKLISTED_MERCHANT"
     */
    public void recordDecision(String decision, String reason) {
        Counter.builder("zbank.fraud.decisions")
                .description("Total transaction fraud check decisions")
                .tag("decision", decision)
                .tag("reason", reason)
                .register(meterRegistry)
                .increment();
    }

    /**
     * Measures the execution time of the Redis sliding-window velocity check.
     */
    public <T> T recordVelocityCheckTime(Supplier<T> supplier) {
        long start = System.nanoTime();
        try {
            return supplier.get();
        } finally {
            velocityCheckTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    /**
     * Increments the Kafka publishing failure counter.
     */
    public void incrementKafkaPublishFailures() {
        kafkaPublishFailures.increment();
    }

    public Timer getVelocityCheckTimer() {
        return velocityCheckTimer;
    }

    public Counter getKafkaPublishFailures() {
        return kafkaPublishFailures;
    }
}
