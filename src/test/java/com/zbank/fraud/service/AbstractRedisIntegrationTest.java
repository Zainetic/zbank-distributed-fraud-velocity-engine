package com.zbank.fraud.service;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest
public abstract class AbstractRedisIntegrationTest {

    public static final GenericContainer<?> REDIS_CONTAINER;

    static {
        GenericContainer<?> container = null;
        try {
            container = new GenericContainer<>(DockerImageName.parse("redis:7.2-alpine"))
                    .withExposedPorts(6379);
            container.start();
        } catch (Exception ignored) {
            container = null;
        }
        REDIS_CONTAINER = container;
    }

    @BeforeAll
    static void ensureDockerRunning() {
        Assumptions.assumeTrue(REDIS_CONTAINER != null && REDIS_CONTAINER.isRunning(),
                "Docker is not running or redis container failed to start; skipping container test.");
    }

    @DynamicPropertySource
    static void configureRedis(DynamicPropertyRegistry registry) {
        if (REDIS_CONTAINER != null && REDIS_CONTAINER.isRunning()) {
            registry.add("spring.data.redis.host", REDIS_CONTAINER::getHost);
            registry.add("spring.data.redis.port", () -> REDIS_CONTAINER.getMappedPort(6379));
        }
    }
}
