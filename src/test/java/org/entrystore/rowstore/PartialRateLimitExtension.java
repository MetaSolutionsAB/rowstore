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

/**
 * JUnit 5 extension that starts a RowStore test environment with partial rate limit config.
 *
 * Configuration:
 * - Global rate limit: -1 (disabled)
 * - Per-dataset rate limit: {@value #DATASET_LIMIT} requests per time range
 * - Time range: {@value #TIME_RANGE_SECONDS} seconds
 *
 * This tests the scenario where global is disabled but dataset limit is active,
 * which exposed a bug when the constructor guard used {@code != -1} instead of {@code > 0}.
 */
public class PartialRateLimitExtension implements BeforeAllCallback, ExecutionCondition {

    private static final Logger log = LoggerFactory.getLogger(PartialRateLimitExtension.class);

    public static final int GLOBAL_LIMIT = -1;
    public static final int DATASET_LIMIT = 10;
    public static final int TIME_RANGE_SECONDS = 10;

    private static ConfigurableTestEnvironment environment;
    private static final Object LOCK = new Object();

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        String externalUrl = System.getProperty("rowstore.baseUrl");
        if (externalUrl != null && !externalUrl.isEmpty()) {
            return ConditionEvaluationResult.disabled(
                    "Partial rate limit tests require managed test environment");
        }

        if (!isDockerAvailable()) {
            return ConditionEvaluationResult.disabled(
                    "Docker is not available. PartialRateLimitExtension tests require Docker.");
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

                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    if (environment != null) {
                        environment.stop();
                    }
                }));
            }
        }
    }

    public static String getBaseUrl() {
        if (environment == null) {
            throw new IllegalStateException("PartialRateLimitExtension has not been initialized");
        }
        return environment.getBaseUrl();
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
