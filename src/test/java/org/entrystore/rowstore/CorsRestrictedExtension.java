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
 * JUnit Jupiter extension that starts a RowStore test environment whose "cors.allowedorigins" lists only
 * {@link #ALLOWED_ORIGIN}, so that other origins are rejected (SEC-6.02).
 *
 * Usage:
 * <pre>
 * {@code
 * @ExtendWith(CorsRestrictedExtension.class)
 * @Tag("cors-restricted")
 * class MyCorsRestrictedTest {
 *     // tests...
 * }
 * }
 * </pre>
 */
public class CorsRestrictedExtension implements BeforeAllCallback, ExecutionCondition {

    private static final Logger log = LoggerFactory.getLogger(CorsRestrictedExtension.class);

    /**
     * The only origin listed in "cors.allowedorigins".
     */
    public static final String ALLOWED_ORIGIN = "https://app.example.com";

    private static ConfigurableTestEnvironment environment;
    private static final Object LOCK = new Object();

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        // An external server has its own CORS configuration, which these tests cannot assume
        String externalUrl = System.getProperty("rowstore.baseUrl");
        if (externalUrl != null && !externalUrl.isEmpty()) {
            return ConditionEvaluationResult.disabled("CORS restriction tests need their own server configuration");
        }

        if (!isDockerAvailable()) {
            return ConditionEvaluationResult.disabled(
                    "Docker is not available. CorsRestrictedExtension tests require Docker.");
        }

        return ConditionEvaluationResult.enabled("Docker is available");
    }

    @Override
    public void beforeAll(ExtensionContext context) {
        synchronized (LOCK) {
            if (environment == null) {
                environment = ConfigurableTestEnvironment.builder()
                        .allowedOrigins(List.of(ALLOWED_ORIGIN))
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
        if (environment == null) {
            throw new IllegalStateException("CorsRestrictedExtension has not been initialized");
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
