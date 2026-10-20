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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

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
     * Opens a JDBC connection to the PostgreSQL container, for tests that need to
     * inspect the database schema directly. The caller must close the connection.
     */
    public Connection openDatabaseConnection() throws SQLException {
        if (!started) {
            throw new IllegalStateException("Test environment is not started");
        }
        return DriverManager.getConnection(postgresContainer.getJdbcUrl(),
                postgresContainer.getUsername(), postgresContainer.getPassword());
    }

    /**
     * Opens a JDBC connection to the maintenance database "postgres" of the PostgreSQL container, for tests that
     * administer the test database (some statements cannot be run on the database the session is connected to).
     * The caller must close the connection.
     */
    public Connection openMaintenanceDatabaseConnection() throws SQLException {
        return openDatabaseConnection("postgres");
    }

    /**
     * Opens a JDBC connection to another database of the PostgreSQL container. The caller must close the connection.
     */
    public Connection openDatabaseConnection(String databaseName) throws SQLException {
        if (!started) {
            throw new IllegalStateException("Test environment is not started");
        }
        String url = "jdbc:postgresql://" + postgresContainer.getHost() + ":" +
                postgresContainer.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT) + "/" + databaseName;
        return DriverManager.getConnection(url, postgresContainer.getUsername(), postgresContainer.getPassword());
    }

    /**
     * Starts an additional RowStore server that uses another (existing) database of the PostgreSQL container, for
     * tests that need to (re)start RowStore without affecting the shared server. The caller must close the context.
     *
     * @return Returns the context of the started server, whose base URL is http://localhost:{port}.
     */
    public ConfigurableApplicationContext startAdditionalRowStore(String databaseName, int port) {
        if (!started) {
            throw new IllegalStateException("Test environment is not started");
        }
        Path configFile = writeConfig(port, databaseName);
        try {
            return SpringApplication.run(RowStoreApplication.class,
                    "--server.port=" + port,
                    "--rowstore.config.uri=" + configFile.toUri(),
                    "--spring.main.banner-mode=off");
        } finally {
            try {
                Files.deleteIfExists(configFile);
            } catch (IOException e) {
                log.warn("Failed to delete temp config file", e);
            }
        }
    }

    /**
     * Returns a bean of the running RowStore application, for tests that need to call into it directly.
     */
    public <T> T getBean(Class<T> type) {
        if (!started) {
            throw new IllegalStateException("Test environment is not started");
        }
        return applicationContext.getBean(type);
    }

    /**
     * Returns whether the test environment is started.
     */
    public boolean isStarted() {
        return started;
    }

    /**
     * Opens a direct JDBC connection to the test database, e.g., for failure injection.
     */
    public Connection openDatabaseConnection() throws SQLException {
        if (postgresContainer == null) {
            throw new IllegalStateException("Test environment has not been started");
        }
        return DriverManager.getConnection(postgresContainer.getJdbcUrl(),
                postgresContainer.getUsername(), postgresContainer.getPassword());
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
        tempConfigFile = writeConfig(serverPort, postgresContainer.getDatabaseName());
    }

    private Path writeConfig(int port, String databaseName) {
        try {
            JSONObject config = new JSONObject();
            config.put("baseurl", "http://localhost:" + port + "/");
            config.put("regexpqueries", "full");
            config.put("maxetlprocesses", 5);
            config.put("querytimeout", 30);
            config.put("loglevel", "INFO");

            JSONObject database = new JSONObject();
            database.put("type", "postgresql");
            database.put("host", postgresContainer.getHost());
            database.put("port", postgresContainer.getMappedPort(5432));
            database.put("database", databaseName);
            database.put("user", postgresContainer.getUsername());
            database.put("password", postgresContainer.getPassword());
            database.put("ssl", false);
            config.put("database", database);

            Path configFile = Files.createTempFile("rowstore-test-config-", ".json");
            Files.writeString(configFile, config.toString(2));

            log.debug("Created temp config file: {}", configFile);
            return configFile;
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

    public static int findAvailablePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException("Failed to find available port", e);
        }
    }

}
