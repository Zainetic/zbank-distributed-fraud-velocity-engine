package com.zbank.fraud.controller;

import com.zbank.fraud.service.FraudMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.web.servlet.MockMvc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class ActuatorAndMetricsIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private FraudMetrics fraudMetrics;

    @Autowired
    private MeterRegistry meterRegistry;

    @MockBean
    private com.zbank.fraud.repository.BlacklistedMerchantRepository merchantRepository;

    @MockBean
    private com.zbank.fraud.repository.FraudAuditRepository auditRepository;

    @MockBean
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Test
    @DisplayName("Health endpoint returns UP status")
    void shouldReturnHealthUp() throws Exception {
        mockMvc.perform(get("/actuator/health"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("UP"));
    }

    @Test
    @DisplayName("Prometheus actuator endpoint is exposed and scrapeable")
    void shouldExposePrometheusEndpoint() throws Exception {
        mockMvc.perform(get("/actuator/prometheus"))
                .andExpect(status().isOk());
    }

    @Test
    @DisplayName("FraudMetrics records decisions, latency timer, and publish failure counters")
    void shouldRecordCustomMetrics() {
        // Record test metrics
        fraudMetrics.recordDecision("APPROVED", "NONE");
        fraudMetrics.recordDecision("DECLINED", "VELOCITY_LIMIT_EXCEEDED");
        fraudMetrics.incrementKafkaPublishFailures();

        String timedResult = fraudMetrics.recordVelocityCheckTime(() -> "timed_success");
        assertEquals("timed_success", timedResult);

        // Verify metrics inside MeterRegistry
        double approvedCount = meterRegistry.get("zbank.fraud.decisions")
                .tag("decision", "APPROVED")
                .tag("reason", "NONE")
                .counter().count();
        assertTrue(approvedCount >= 1.0, "Approved decision counter should be >= 1");

        double velocityCheckCount = fraudMetrics.getVelocityCheckTimer().count();
        assertTrue(velocityCheckCount >= 1, "Velocity timer execution count should be >= 1");

        double failuresCount = fraudMetrics.getKafkaPublishFailures().count();
        assertTrue(failuresCount >= 1.0, "Kafka failure counter should be >= 1");
    }
}
