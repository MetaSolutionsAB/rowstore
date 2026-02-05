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
 * JUnit 5 extension that starts a RowStore test environment with simple regex mode.
 *
 * In simple mode:
 * - Query values are treated as exact matches by default
 * - Patterns starting with ^ are interpreted as regex
 * - Patterns prefixed with ~ are interpreted as regex
 *
 * Usage:
 * <pre>
 * {@code
 * @ExtendWith(RegexSimpleExtension.class)
 * @Tag("regex-simple")
 * class MyRegexSimpleTest {
 *     // tests...
 * }
 * }
 * </pre>
 */
public class RegexSimpleExtension implements BeforeAllCallback, ExecutionCondition {

    private static final Logger log = LoggerFactory.getLogger(RegexSimpleExtension.class);

    private static ConfigurableTestEnvironment environment;
    private static final Object LOCK = new Object();

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        // If external URL is provided, always run tests
        String externalUrl = System.getProperty("rowstore.baseUrl");
        if (externalUrl != null && !externalUrl.isEmpty()) {
            return ConditionEvaluationResult.enabled("Using external RowStore at " + externalUrl);
        }

        // Check if Docker is available
        if (!isDockerAvailable()) {
            return ConditionEvaluationResult.disabled(
                    "Docker is not available. RegexSimpleExtension tests require Docker.");
        }

        return ConditionEvaluationResult.enabled("Docker is available");
    }

    @Override
    public void beforeAll(ExtensionContext context) {
        // Only start if not using external server
        String externalUrl = System.getProperty("rowstore.baseUrl");
        if (externalUrl == null || externalUrl.isEmpty()) {
            synchronized (LOCK) {
                if (environment == null) {
                    environment = ConfigurableTestEnvironment.builder()
                            .regexpQueries("simple")
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
    }

    /**
     * Returns the base URL of the test environment.
     */
    public static String getBaseUrl() {
        String externalUrl = System.getProperty("rowstore.baseUrl");
        if (externalUrl != null && !externalUrl.isEmpty()) {
            return externalUrl;
        }
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
