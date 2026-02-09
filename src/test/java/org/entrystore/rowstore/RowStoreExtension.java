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
 * JUnit 5 extension that starts the RowStore test environment before any tests run.
 *
 * The environment (PostgreSQL container + RowStore server) is started once and
 * shared across all test classes that use this extension.
 *
 * If Docker is not available and no external URL is configured, tests are skipped.
 *
 * Usage:
 * <pre>
 * {@code
 * @ExtendWith(RowStoreExtension.class)
 * class MyIntegrationTest extends BaseIntegrationTest {
 *     // tests...
 * }
 * }
 * </pre>
 */
public class RowStoreExtension implements BeforeAllCallback, ExecutionCondition {

    private static final Logger log = LoggerFactory.getLogger(RowStoreExtension.class);

    private static Boolean dockerAvailable;

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
                    "Docker is not available. To run integration tests, either:\n" +
                    "  1. Start Docker daemon: sudo systemctl start docker\n" +
                    "  2. Or provide external RowStore: mvn verify -Drowstore.baseUrl=http://localhost:8282");
        }

        return ConditionEvaluationResult.enabled("Docker is available");
    }

    @Override
    public void beforeAll(ExtensionContext context) {
        // Only start if not using external server
        String externalUrl = System.getProperty("rowstore.baseUrl");
        if (externalUrl == null || externalUrl.isEmpty()) {
            RowStoreTestEnvironment.getInstance().start();
        }
    }

    private static synchronized boolean isDockerAvailable() {
        if (dockerAvailable == null) {
            try {
                dockerAvailable = DockerClientFactory.instance().isDockerAvailable();
                if (dockerAvailable) {
                    log.info("Docker is available");
                } else {
                    log.warn("Docker is not available");
                }
            } catch (Exception e) {
                log.warn("Failed to check Docker availability: {}", e.getMessage());
                dockerAvailable = false;
            }
        }
        return dockerAvailable;
    }

}
