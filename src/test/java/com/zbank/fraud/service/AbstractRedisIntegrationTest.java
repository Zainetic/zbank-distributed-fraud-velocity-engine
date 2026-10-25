package com.zbank.fraud.service;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest
@Testcontainers(disabledWithoutDocker = true)
public abstract class AbstractRedisIntegrationTest {

    @Container
    public static final GenericContainer<?> redisContainer = new GenericContainer<>(DockerImageName.parse("redis:7.2-alpine"))
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
}
