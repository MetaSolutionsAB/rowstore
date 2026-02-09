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

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.containers.PostgreSQLContainer;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A configurable test environment for RowStore integration tests.
 * Allows creating test environments with different configurations (e.g., regex modes, rate limiting).
 *
 * Unlike {@link RowStoreTestEnvironment} which is a singleton, this class can create
 * multiple independent environments with different configurations.
 *
 * Usage:
 * <pre>
 * ConfigurableTestEnvironment env = ConfigurableTestEnvironment.builder()
 *     .regexpQueries("simple")
 *     .rateLimitEnabled(true)
 *     .build();
 * env.start();
 * // ... run tests ...
 * env.stop();
 * </pre>
 */
public class ConfigurableTestEnvironment {

    private static final Logger log = LoggerFactory.getLogger(ConfigurableTestEnvironment.class);

    // Shared PostgreSQL container across all environments for efficiency
    private static PostgreSQLContainer<?> sharedPostgresContainer;
    private static int postgresUsageCount = 0;
    private static final Object POSTGRES_LOCK = new Object();

    // Cache for environments by configuration key
    private static final ConcurrentHashMap<String, ConfigurableTestEnvironment> environmentCache = new ConcurrentHashMap<>();

    private final Configuration config;
    private ConfigurableApplicationContext applicationContext;
    private int serverPort;
    private Path tempConfigFile;
    private boolean started = false;

    private ConfigurableTestEnvironment(Configuration config) {
        this.config = config;
    }

    /**
     * Gets or creates a cached environment with the specified configuration.
     * Environments are cached by their configuration key for reuse.
     */
    public static ConfigurableTestEnvironment getOrCreate(Configuration config) {
        String key = config.getCacheKey();
        return environmentCache.computeIfAbsent(key, k -> new ConfigurableTestEnvironment(config));
    }

    /**
     * Creates a new builder for configuring the test environment.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Starts the test environment if not already started.
     */
    public synchronized void start() {
        if (started) {
            return;
        }

        log.info("Starting configurable test environment with config: {}", config);

        // Start or reuse shared PostgreSQL container
        startSharedPostgres();

        try {
            // Find an available port
            serverPort = findAvailablePort();

            // Create configuration file
            createConfig();

            // Start RowStore server
            startRowStore();
        } catch (Exception e) {
            // Clean up on failure: delete temp config and release postgres
            if (tempConfigFile != null) {
                try {
                    Files.deleteIfExists(tempConfigFile);
                } catch (IOException cleanupEx) {
                    log.warn("Failed to clean up temp config file on startup failure", cleanupEx);
                }
            }
            releaseSharedPostgres();
            throw e;
        }

        started = true;
        log.info("Configurable test environment started at {}", getBaseUrl());
    }

    /**
     * Stops the test environment.
     */
    public synchronized void stop() {
        if (!started) {
            return;
        }

        log.info("Stopping configurable test environment...");

        // Stop RowStore
        if (applicationContext != null) {
            try {
                applicationContext.close();
                log.info("RowStore server stopped");
            } catch (Exception e) {
                log.error("Error stopping RowStore server", e);
            }
        }

        // Release shared PostgreSQL
        releaseSharedPostgres();

        // Clean up temp config file
        if (tempConfigFile != null) {
            try {
                Files.deleteIfExists(tempConfigFile);
            } catch (IOException e) {
                log.warn("Failed to delete temp config file", e);
            }
        }

        started = false;
    }

    /**
     * Returns the base URL of the running RowStore server.
     */
    public String getBaseUrl() {
        return "http://localhost:" + serverPort;
    }

    /**
     * Returns whether the test environment is started.
     */
    public boolean isStarted() {
        return started;
    }

    /**
     * Returns the configuration used by this environment.
     */
    public Configuration getConfig() {
        return config;
    }

    private static void startSharedPostgres() {
        synchronized (POSTGRES_LOCK) {
            if (sharedPostgresContainer == null || !sharedPostgresContainer.isRunning()) {
                log.info("Starting shared PostgreSQL container...");
                sharedPostgresContainer = new PostgreSQLContainer<>("postgres:16-alpine")
                        .withDatabaseName("rowstoretest")
                        .withUsername("rowstoretest")
                        .withPassword("rowstoretestpw");
                sharedPostgresContainer.start();
                log.info("Shared PostgreSQL container started at {}:{}",
                        sharedPostgresContainer.getHost(),
                        sharedPostgresContainer.getMappedPort(5432));
            }
            postgresUsageCount++;
        }
    }

    private static void releaseSharedPostgres() {
        synchronized (POSTGRES_LOCK) {
            postgresUsageCount--;
            if (postgresUsageCount <= 0 && sharedPostgresContainer != null) {
                log.info("Stopping shared PostgreSQL container (no more users)");
                sharedPostgresContainer.stop();
                sharedPostgresContainer = null;
                postgresUsageCount = 0;
            }
        }
    }

    private void createConfig() {
        try {
            JSONObject jsonConfig = new JSONObject();
            jsonConfig.put("baseurl", "http://localhost:" + serverPort + "/");
            jsonConfig.put("regexpqueries", config.regexpQueries);
            jsonConfig.put("maxetlprocesses", config.maxEtlProcesses);
            jsonConfig.put("querytimeout", config.queryTimeout);
            jsonConfig.put("querymaxlimit", config.queryMaxLimit);
            jsonConfig.put("loglevel", "INFO");

            // Rate limiting configuration
            if (config.rateLimitEnabled) {
                JSONObject rateLimit = new JSONObject();
                rateLimit.put("timerange", config.rateLimitTimeRange);
                rateLimit.put("global", config.rateLimitRequestsGlobal);
                rateLimit.put("dataset", config.rateLimitRequestsDataset);
                rateLimit.put("type", config.rateLimitType);
                if (config.rateLimitRequestsClientIp != -1) {
                    rateLimit.put("clientip", config.rateLimitRequestsClientIp);
                }
                jsonConfig.put("ratelimit", rateLimit);
            }

            // Database configuration
            JSONObject database = new JSONObject();
            database.put("type", "postgresql");
            database.put("host", sharedPostgresContainer.getHost());
            database.put("port", sharedPostgresContainer.getMappedPort(5432));
            database.put("database", sharedPostgresContainer.getDatabaseName());
            database.put("user", sharedPostgresContainer.getUsername());
            database.put("password", sharedPostgresContainer.getPassword());
            database.put("ssl", false);
            jsonConfig.put("database", database);

            tempConfigFile = Files.createTempFile("rowstore-config-test-", ".json");
            Files.writeString(tempConfigFile, jsonConfig.toString(2));

            log.debug("Created temp config file: {}", tempConfigFile);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create test configuration", e);
        }
    }

    private void startRowStore() {
        try {
            applicationContext = SpringApplication.run(RowStoreApplication.class,
                    "--server.port=" + serverPort,
                    "--rowstore.config.uri=" + tempConfigFile.toUri(),
                    "--spring.main.banner-mode=off");

            log.info("RowStore server started on port {} with regexpqueries={}", serverPort, config.regexpQueries);
        } catch (Exception e) {
            throw new RuntimeException("Failed to start RowStore server", e);
        }
    }

    private int findAvailablePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException("Failed to find available port", e);
        }
    }

    /**
     * Configuration for a test environment.
     */
    public static class Configuration {
        private final String regexpQueries;
        private final int maxEtlProcesses;
        private final int queryTimeout;
        private final int queryMaxLimit;
        private final boolean rateLimitEnabled;
        private final int rateLimitTimeRange;
        private final int rateLimitRequestsGlobal;
        private final int rateLimitRequestsDataset;
        private final int rateLimitRequestsClientIp;
        private final String rateLimitType;

        private Configuration(Builder builder) {
            this.regexpQueries = builder.regexpQueries;
            this.maxEtlProcesses = builder.maxEtlProcesses;
            this.queryTimeout = builder.queryTimeout;
            this.queryMaxLimit = builder.queryMaxLimit;
            this.rateLimitEnabled = builder.rateLimitEnabled;
            this.rateLimitTimeRange = builder.rateLimitTimeRange;
            this.rateLimitRequestsGlobal = builder.rateLimitRequestsGlobal;
            this.rateLimitRequestsDataset = builder.rateLimitRequestsDataset;
            this.rateLimitRequestsClientIp = builder.rateLimitRequestsClientIp;
            this.rateLimitType = builder.rateLimitType;
        }

        public String getRegexpQueries() {
            return regexpQueries;
        }

        public boolean isRateLimitEnabled() {
            return rateLimitEnabled;
        }

        /**
         * Returns a unique key for caching environments with this configuration.
         */
        public String getCacheKey() {
            return String.format("regexp=%s,rateLimit=%b,timeRange=%d,global=%d,dataset=%d,clientIp=%d,type=%s",
                    regexpQueries, rateLimitEnabled, rateLimitTimeRange,
                    rateLimitRequestsGlobal, rateLimitRequestsDataset, rateLimitRequestsClientIp, rateLimitType);
        }

        @Override
        public String toString() {
            return getCacheKey();
        }
    }

    /**
     * Builder for creating Configuration objects.
     */
    public static class Builder {
        private String regexpQueries = "full";
        private int maxEtlProcesses = 5;
        private int queryTimeout = 30;
        private int queryMaxLimit = 10000;
        private boolean rateLimitEnabled = false;
        private int rateLimitTimeRange = 60;
        private int rateLimitRequestsGlobal = 100;
        private int rateLimitRequestsDataset = 20;
        private int rateLimitRequestsClientIp = -1;
        private String rateLimitType = "slidingwindow";

        public Builder regexpQueries(String regexpQueries) {
            this.regexpQueries = regexpQueries;
            return this;
        }

        public Builder maxEtlProcesses(int maxEtlProcesses) {
            this.maxEtlProcesses = maxEtlProcesses;
            return this;
        }

        public Builder queryTimeout(int queryTimeout) {
            this.queryTimeout = queryTimeout;
            return this;
        }

        public Builder queryMaxLimit(int queryMaxLimit) {
            this.queryMaxLimit = queryMaxLimit;
            return this;
        }

        public Builder rateLimitEnabled(boolean enabled) {
            this.rateLimitEnabled = enabled;
            return this;
        }

        public Builder rateLimitTimeRange(int timeRange) {
            this.rateLimitTimeRange = timeRange;
            return this;
        }

        public Builder rateLimitRequestsGlobal(int requestsGlobal) {
            this.rateLimitRequestsGlobal = requestsGlobal;
            return this;
        }

        public Builder rateLimitRequestsDataset(int requestsDataset) {
            this.rateLimitRequestsDataset = requestsDataset;
            return this;
        }

        public Builder rateLimitRequestsClientIp(int requestsClientIp) {
            this.rateLimitRequestsClientIp = requestsClientIp;
            return this;
        }

        public Builder rateLimitType(String rateLimitType) {
            this.rateLimitType = rateLimitType;
            return this;
        }

        public Configuration build() {
            return new Configuration(this);
        }

        /**
         * Builds and returns a test environment with this configuration.
         */
        public ConfigurableTestEnvironment buildEnvironment() {
            return ConfigurableTestEnvironment.getOrCreate(build());
        }
    }
}
