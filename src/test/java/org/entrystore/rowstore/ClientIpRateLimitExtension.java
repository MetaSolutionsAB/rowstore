/*
 * Copyright (c) 2011-2026 MetaSolutions AB <info@metasolutions.se>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.entrystore.rowstore;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;

import java.util.List;

/**
 * JUnit 5 extension that starts three RowStore test environments with a per-client-IP rate limit
 * of {@value #CLIENT_IP_LIMIT} requests per {@value #TIME_RANGE_SECONDS} seconds. Global and per-dataset
 * limits are set high so that only the per-client-IP limit trips.
 *
 * - {@link #getUntrustedBaseUrl()}: {@code trustedproxies: []}, the test client on loopback is not a trusted proxy,
 *   so X-Forwarded-For must be ignored.
 * - {@link #getTrustedBaseUrl()}: {@code trustedproxies} contains the loopback addresses, so the test client acts
 *   as a trusted reverse proxy and X-Forwarded-For determines the client address.
 * - {@link #getDefaultBaseUrl()}: no {@code trustedproxies}, the connecting test client is trusted as the reverse
 *   proxy and the last X-Forwarded-For entry determines the client address.
 */
public class ClientIpRateLimitExtension implements BeforeAllCallback, ExecutionCondition {

    private static final Logger log = LoggerFactory.getLogger(ClientIpRateLimitExtension.class);

    public static final int CLIENT_IP_LIMIT = 5;
    public static final int TIME_RANGE_SECONDS = 30;
    private static final int HIGH_LIMIT = 1000;

    private static ConfigurableTestEnvironment untrustedEnvironment;
    private static ConfigurableTestEnvironment trustedEnvironment;
    private static ConfigurableTestEnvironment defaultEnvironment;
    private static final Object LOCK = new Object();

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        String externalUrl = System.getProperty("rowstore.baseUrl");
        if (externalUrl != null && !externalUrl.isEmpty()) {
            return ConditionEvaluationResult.disabled(
                    "Rate limit tests require managed test environment (cannot use external server)");
        }
        if (!isDockerAvailable()) {
            return ConditionEvaluationResult.disabled(
                    "Docker is not available. ClientIpRateLimitExtension tests require Docker.");
        }
        return ConditionEvaluationResult.enabled("Docker is available");
    }

    @Override
    public void beforeAll(ExtensionContext context) {
        synchronized (LOCK) {
            if (untrustedEnvironment == null) {
                untrustedEnvironment = start(List.of());
                trustedEnvironment = start(List.of("127.0.0.1", "::1"));
                defaultEnvironment = start(null);

                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    untrustedEnvironment.stop();
                    trustedEnvironment.stop();
                    defaultEnvironment.stop();
                }));
            }
        }
    }

    private static ConfigurableTestEnvironment start(List<String> trustedProxies) {
        ConfigurableTestEnvironment env = ConfigurableTestEnvironment.builder()
                .rateLimitEnabled(true)
                .rateLimitTimeRange(TIME_RANGE_SECONDS)
                .rateLimitRequestsGlobal(HIGH_LIMIT)
                .rateLimitRequestsDataset(HIGH_LIMIT)
                .rateLimitRequestsClientIp(CLIENT_IP_LIMIT)
                .trustedProxies(trustedProxies)
                .buildEnvironment();
        env.start();
        return env;
    }

    public static String getUntrustedBaseUrl() {
        if (untrustedEnvironment == null) {
            throw new IllegalStateException("ClientIpRateLimitExtension has not been initialized");
        }
        return untrustedEnvironment.getBaseUrl();
    }

    public static String getTrustedBaseUrl() {
        if (trustedEnvironment == null) {
            throw new IllegalStateException("ClientIpRateLimitExtension has not been initialized");
        }
        return trustedEnvironment.getBaseUrl();
    }

    public static String getDefaultBaseUrl() {
        if (defaultEnvironment == null) {
            throw new IllegalStateException("ClientIpRateLimitExtension has not been initialized");
        }
        return defaultEnvironment.getBaseUrl();
    }

    private boolean isDockerAvailable() {
        try {
            return DockerClientFactory.instance().isDockerAvailable();
        } catch (Exception e) {
            log.warn("Failed to check Docker availability: {}", e.getMessage());
            return false;
        }
    }
}
