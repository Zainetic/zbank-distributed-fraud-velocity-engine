package com.zbank.fraud.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zbank.fraud.model.TransactionRequest;
import com.zbank.fraud.repository.BlacklistedMerchantRepository;
import com.zbank.fraud.service.VelocityFraudDetector;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(FraudCheckController.class)
class FraudCheckControllerValidationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private VelocityFraudDetector velocityFraudDetector;

    @MockBean
    private BlacklistedMerchantRepository merchantRepository;

    @MockBean
    private KafkaTemplate<String, Object> kafkaTemplate;

    @MockBean
    private com.zbank.fraud.service.FraudMetrics fraudMetrics;

    @org.junit.jupiter.api.BeforeEach
    void setUp() {
        org.mockito.Mockito.when(fraudMetrics.recordVelocityCheckTime(org.mockito.ArgumentMatchers.any()))
                .thenAnswer(inv -> ((java.util.function.Supplier<?>) inv.getArgument(0)).get());
    }

    @Test
    @DisplayName("Should return 400 Bad Request when transaction amount is negative")
    void shouldReturn400WhenAmountIsNegative() throws Exception {
        TransactionRequest request = new TransactionRequest(
                UUID.randomUUID().toString(),
                "card_valid",
                "acc_valid",
                new BigDecimal("-25.00"),
                "EUR",
                "5411",
                Instant.now()
        );

        mockMvc.perform(post("/api/v1/fraud/velocity-check")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.message").value("Validation Failed"))
                .andExpect(jsonPath("$.details.amount").value("Transaction amount must be strictly positive"));
    }

    @Test
    @DisplayName("Should return 400 Bad Request when transaction amount is zero")
    void shouldReturn400WhenAmountIsZero() throws Exception {
        TransactionRequest request = new TransactionRequest(
                UUID.randomUUID().toString(),
                "card_valid",
                "acc_valid",
                BigDecimal.ZERO,
                "EUR",
                "5411",
                Instant.now()
        );

        mockMvc.perform(post("/api/v1/fraud/velocity-check")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.message").value("Validation Failed"))
                .andExpect(jsonPath("$.details.amount").value("Transaction amount must be strictly positive"));
    }

    @Test
    @DisplayName("Should return 200 OK when transaction amount is positive")
    void shouldReturn200WhenAmountIsPositive() throws Exception {
        TransactionRequest request = new TransactionRequest(
                UUID.randomUUID().toString(),
                "card_valid",
                "acc_valid",
                new BigDecimal("50.00"),
                "EUR",
                "5411",
                Instant.now()
        );

        when(merchantRepository.existsByMerchantCategoryCode(any())).thenReturn(false);
        when(velocityFraudDetector.isFraudulentVelocity(any())).thenReturn(false);

        mockMvc.perform(post("/api/v1/fraud/velocity-check")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.isFraudulent").value(false))
                .andExpect(jsonPath("$.statusReason").value("APPROVED"));
    }
}
