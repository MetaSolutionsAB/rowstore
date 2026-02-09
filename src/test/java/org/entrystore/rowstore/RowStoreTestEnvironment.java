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

/**
 * Manages the test environment for RowStore integration tests.
 * Starts a PostgreSQL container via Testcontainers and a RowStore server.
 *
 * This class implements a singleton pattern to ensure only one PostgreSQL
 * container and RowStore server are started per test run.
 */
public class RowStoreTestEnvironment {

    private static final Logger log = LoggerFactory.getLogger(RowStoreTestEnvironment.class);

    private static RowStoreTestEnvironment instance;

    private PostgreSQLContainer<?> postgresContainer;
    private ConfigurableApplicationContext applicationContext;
    private int serverPort;
    private Path tempConfigFile;
    private boolean started = false;

    private RowStoreTestEnvironment() {
        // Private constructor for singleton
    }

    public static synchronized RowStoreTestEnvironment getInstance() {
        if (instance == null) {
            instance = new RowStoreTestEnvironment();
        }
        return instance;
    }

    /**
     * Starts the test environment if not already started.
     * This method is idempotent - calling it multiple times has no effect
     * after the first successful start.
     */
    public synchronized void start() {
        if (started) {
            return;
        }

        log.info("Starting RowStore test environment...");

        // Start PostgreSQL container
        startPostgres();

        // Find an available port for the RowStore server
        serverPort = findAvailablePort();

        // Create configuration file pointing to the container
        createConfig();

        // Start RowStore server
        startRowStore();

        // Register shutdown hook for cleanup
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));

        started = true;
        log.info("RowStore test environment started at {}", getBaseUrl());
    }

    /**
     * Stops the test environment.
     */
    public synchronized void stop() {
        if (!started) {
            return;
        }

        log.info("Stopping RowStore test environment...");

        // Stop RowStore
        if (applicationContext != null) {
            try {
                applicationContext.close();
                log.info("RowStore server stopped");
            } catch (Exception e) {
                log.error("Error stopping RowStore server", e);
            }
        }

        // Stop PostgreSQL container
        if (postgresContainer != null && postgresContainer.isRunning()) {
            postgresContainer.stop();
            log.info("PostgreSQL container stopped");
        }

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

    private void startPostgres() {
        log.info("Starting PostgreSQL container...");

        postgresContainer = new PostgreSQLContainer<>("postgres:16-alpine")
                .withDatabaseName("rowstoretest")
                .withUsername("rowstoretest")
                .withPassword("rowstoretestpw");

        postgresContainer.start();

        log.info("PostgreSQL container started at {}:{}",
                postgresContainer.getHost(),
                postgresContainer.getMappedPort(5432));
    }

    private void createConfig() {
        try {
            JSONObject config = new JSONObject();
            config.put("baseurl", "http://localhost:" + serverPort + "/");
            config.put("regexpqueries", "full");
            config.put("maxetlprocesses", 5);
            config.put("querytimeout", 30);
            config.put("loglevel", "INFO");

            JSONObject database = new JSONObject();
            database.put("type", "postgresql");
            database.put("host", postgresContainer.getHost());
            database.put("port", postgresContainer.getMappedPort(5432));
            database.put("database", postgresContainer.getDatabaseName());
            database.put("user", postgresContainer.getUsername());
            database.put("password", postgresContainer.getPassword());
            database.put("ssl", false);
            config.put("database", database);

            tempConfigFile = Files.createTempFile("rowstore-test-config-", ".json");
            Files.writeString(tempConfigFile, config.toString(2));

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

            log.info("RowStore server started on port {}", serverPort);
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

}
