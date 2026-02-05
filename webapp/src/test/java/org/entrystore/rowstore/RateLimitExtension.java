/*
 * Copyright (c) 2011-2024 MetaSolutions AB <info@metasolutions.se>
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

/**
 * JUnit 5 extension that starts a RowStore test environment with rate limiting enabled.
 *
 * Configuration:
 * - Global rate limit: 5 requests per time range
 * - Per-dataset rate limit: 3 requests per time range
 * - Time range: 60 seconds
 *
 * This is intentionally set low to make rate limiting easily testable.
 *
 * Usage:
 * <pre>
 * {@code
 * @ExtendWith(RateLimitExtension.class)
 * @Tag("ratelimit")
 * class MyRateLimitTest {
 *     // tests...
 * }
 * }
 * </pre>
 */
public class RateLimitExtension implements BeforeAllCallback, ExecutionCondition {

    private static final Logger log = LoggerFactory.getLogger(RateLimitExtension.class);

    // Limits set high enough to allow setup but low enough to test
    // Setup needs ~6-10 requests for await polling + initial checks
    // We need these high enough that setup can complete reliably
    public static final int GLOBAL_LIMIT = 100;
    public static final int DATASET_LIMIT = 50;
    public static final int TIME_RANGE_SECONDS = 60;

    private static ConfigurableTestEnvironment environment;
    private static final Object LOCK = new Object();

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        // If external URL is provided, skip rate limit tests (can't control external server config)
        String externalUrl = System.getProperty("rowstore.baseUrl");
        if (externalUrl != null && !externalUrl.isEmpty()) {
            return ConditionEvaluationResult.disabled(
                    "Rate limit tests require managed test environment (cannot use external server)");
        }

        // Check if Docker is available
        if (!isDockerAvailable()) {
            return ConditionEvaluationResult.disabled(
                    "Docker is not available. RateLimitExtension tests require Docker.");
        }

        return ConditionEvaluationResult.enabled("Docker is available");
    }

    @Override
    public void beforeAll(ExtensionContext context) {
        synchronized (LOCK) {
            if (environment == null) {
                environment = ConfigurableTestEnvironment.builder()
                        .rateLimitEnabled(true)
                        .rateLimitTimeRange(TIME_RANGE_SECONDS)
                        .rateLimitRequestsGlobal(GLOBAL_LIMIT)
                        .rateLimitRequestsDataset(DATASET_LIMIT)
                        .buildEnvironment();
                environment.start();

                // Register shutdown hook
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    if (environment != null) {
                        environment.stop();
                    }
                }));
            }
        }
    }

    /**
     * Returns the base URL of the test environment.
     */
    public static String getBaseUrl() {
        return environment != null ? environment.getBaseUrl() : null;
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
