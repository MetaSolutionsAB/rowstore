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
 * JUnit 5 extension that starts a RowStore test environment with the legacy CSV parser ({@code legacyparser: true}).
 *
 * When testing an external RowStore instance ({@code rowstore.baseUrl}), the tests are skipped, since the parser
 * of the external instance is unknown.
 */
public class LegacyParserExtension implements BeforeAllCallback, ExecutionCondition {

    private static final Logger log = LoggerFactory.getLogger(LegacyParserExtension.class);

    private static ConfigurableTestEnvironment environment;
    private static final Object LOCK = new Object();

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        String externalUrl = System.getProperty("rowstore.baseUrl");
        if (externalUrl != null && !externalUrl.isEmpty()) {
            return ConditionEvaluationResult.disabled("The parser of the external RowStore at " + externalUrl + " is unknown");
        }

        if (!isDockerAvailable()) {
            return ConditionEvaluationResult.disabled(
                    "Docker is not available. LegacyParserExtension tests require Docker.");
        }

        return ConditionEvaluationResult.enabled("Docker is available");
    }

    @Override
    public void beforeAll(ExtensionContext context) {
        synchronized (LOCK) {
            if (environment == null) {
                environment = ConfigurableTestEnvironment.builder()
                        .legacyParser(true)
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

    /**
     * Returns the base URL of the test environment.
     */
    public static String getBaseUrl() {
        if (environment == null) {
            throw new IllegalStateException("LegacyParserExtension has not been initialized");
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
