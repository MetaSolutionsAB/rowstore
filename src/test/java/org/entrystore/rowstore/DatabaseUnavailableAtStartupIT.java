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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test for the startup of RowStore while its database is not reachable (ARCH-7.08): the startup waits
 * for the database, without listening on its port meanwhile, and completes once the database is reachable.
 * The database is made unreachable by pausing its container, so that connection attempts time out.
 * Specification: 12-connection-limits.md
 */
@DisplayName("Database Unavailable At Startup Tests")
class DatabaseUnavailableAtStartupIT {

    /**
     * Long enough for several failed connection attempts (login timeout 2 s, delays 1, 2, 4 s).
     */
    private static final Duration UNREACHABLE = Duration.ofSeconds(10);

    private static final Duration STARTUP_AFTER_RECOVERY = Duration.ofSeconds(90);

    private PostgreSQLContainer<?> postgres;

    private Path configFile;

    private CompletableFuture<ConfigurableApplicationContext> startup;

    @AfterEach
    void tearDown() throws IOException {
        if (postgres != null && postgres.isRunning() && isPaused()) {
            DockerClientFactory.instance().client().unpauseContainerCmd(postgres.getContainerId()).exec();
        }
        if (startup != null) {
            ConfigurableApplicationContext context = startup.getNow(null);
            if (context != null) {
                context.close();
            } else {
                startup.cancel(true);
            }
        }
        if (postgres != null) {
            postgres.stop();
        }
        if (configFile != null) {
            Files.deleteIfExists(configFile);
        }
    }

    @Test
    @DisplayName("TC-CONN-036: RowStore waits at startup for a database that is not reachable and starts once it is")
    void startupWithUnreachableDatabase_waitsAndStarts() throws Exception {
        String externalUrl = System.getProperty("rowstore.baseUrl");
        Assumptions.assumeTrue(externalUrl == null || externalUrl.isEmpty(), "Needs its own RowStore instance");
        postgres = new PostgreSQLContainer<>("postgres:16-alpine")
                .withDatabaseName("rowstorestartup")
                .withUsername("test")
                .withPassword("test");
        postgres.start();
        int port = findAvailablePort();
        configFile = createConfig(port);

        DockerClientFactory.instance().client().pauseContainerCmd(postgres.getContainerId()).exec();
        startup = CompletableFuture.supplyAsync(() -> SpringApplication.run(RowStoreApplication.class,
                "--server.port=" + port,
                "--rowstore.config.uri=" + configFile.toUri(),
                "--spring.main.banner-mode=off"));

        Thread.sleep(UNREACHABLE.toMillis());
        // still waiting for the database, the startup has neither failed nor opened the port
        assertThat(startup).isNotDone();
        assertThat(isListening(port)).isFalse();

        DockerClientFactory.instance().client().unpauseContainerCmd(postgres.getContainerId()).exec();

        await().atMost(STARTUP_AFTER_RECOVERY).until(startup::isDone);
        assertThat(startup).isCompletedWithValueMatching(ConfigurableApplicationContext::isActive);
        given().get("http://localhost:" + port + "/status").then().statusCode(200);
        // the migration ran exactly once
        try (Connection db = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
             Statement stmt = db.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT count(*) FROM flyway_schema_history WHERE version = '1' AND success")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt(1)).isEqualTo(1);
        }
    }

    private boolean isPaused() {
        return Boolean.TRUE.equals(DockerClientFactory.instance().client()
                .inspectContainerCmd(postgres.getContainerId()).exec().getState().getPaused());
    }

    private Path createConfig(int port) throws IOException {
        JSONObject database = new JSONObject()
                .put("host", postgres.getHost())
                .put("port", postgres.getMappedPort(5432))
                .put("database", postgres.getDatabaseName())
                .put("user", postgres.getUsername())
                .put("password", postgres.getPassword())
                .put("ssl", false)
                .put("connectTimeout", 2)
                .put("loginTimeout", 2);
        JSONObject config = new JSONObject()
                .put("baseurl", "http://localhost:" + port + "/")
                .put("loglevel", "INFO")
                .put("database", database);
        Path file = Files.createTempFile("rowstore-startup-test-", ".json");
        Files.writeString(file, config.toString(2));
        return file;
    }

    private static boolean isListening(int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("localhost", port), 500);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private static int findAvailablePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }

}
