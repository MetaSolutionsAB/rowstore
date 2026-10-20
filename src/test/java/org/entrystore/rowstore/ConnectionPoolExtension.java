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
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.DockerClientFactory;

/**
 * JUnit 5 extension that starts a RowStore test environment with HikariCP connection pooling enabled.
 *
 * Uses connectionPoolMax=3 to verify that the HikariCP code path works correctly, with a separate
 * queryDatabase (pointing to the same container) so that the read-only query pool is exercised too.
 */
public class ConnectionPoolExtension implements BeforeAllCallback, ExecutionCondition {

    private static final Logger log = LoggerFactory.getLogger(ConnectionPoolExtension.class);

    private static ConfigurableTestEnvironment environment;
    private static final Object LOCK = new Object();

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        String externalUrl = System.getProperty("rowstore.baseUrl");
        if (externalUrl != null && !externalUrl.isEmpty()) {
            return ConditionEvaluationResult.enabled("Using external RowStore at " + externalUrl);
        }

        if (!isDockerAvailable()) {
            return ConditionEvaluationResult.disabled(
                    "Docker is not available. ConnectionPoolExtension tests require Docker.");
        }

        return ConditionEvaluationResult.enabled("Docker is available");
    }

    @Override
    public void beforeAll(ExtensionContext context) {
        String externalUrl = System.getProperty("rowstore.baseUrl");
        if (externalUrl == null || externalUrl.isEmpty()) {
            synchronized (LOCK) {
                if (environment == null) {
                    environment = ConfigurableTestEnvironment.builder()
                            .connectionPoolMax(3)
                            .socketTimeout(60)
                            .separateQueryDatabase(true)
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
    }

    public static String getBaseUrl() {
        String externalUrl = System.getProperty("rowstore.baseUrl");
        if (externalUrl != null && !externalUrl.isEmpty()) {
            return externalUrl;
        }
        if (environment == null) {
            throw new IllegalStateException("ConnectionPoolExtension has not been initialized");
        }
        return environment.getBaseUrl();
    }

    /**
     * Returns the Spring context of the in-process RowStore server, or null when testing an external instance.
     */
    public static ConfigurableApplicationContext getApplicationContext() {
        return environment != null ? environment.getApplicationContext() : null;
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
