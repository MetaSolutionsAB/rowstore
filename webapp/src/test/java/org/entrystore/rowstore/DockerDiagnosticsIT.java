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

import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Diagnostic test to verify Docker and Testcontainers are working.
 * Run with: mvn verify -Dit.test=DockerDiagnosticsIT
 */
class DockerDiagnosticsIT {

    @Test
    void checkDockerEnvironment() {
        System.out.println("=== Docker Environment Diagnostics ===");

        // Check environment variables
        System.out.println("\n--- Environment Variables ---");
        System.out.println("DOCKER_HOST: " + System.getenv("DOCKER_HOST"));
        System.out.println("DOCKER_API_VERSION: " + System.getenv("DOCKER_API_VERSION"));
        System.out.println("DOCKER_TLS_VERIFY: " + System.getenv("DOCKER_TLS_VERIFY"));
        System.out.println("DOCKER_CERT_PATH: " + System.getenv("DOCKER_CERT_PATH"));
        System.out.println("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE: " + System.getenv("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE"));
        System.out.println("TESTCONTAINERS_RYUK_DISABLED: " + System.getenv("TESTCONTAINERS_RYUK_DISABLED"));
        System.out.println("HOME: " + System.getenv("HOME"));
        System.out.println("USER: " + System.getenv("USER"));

        // Check socket files
        System.out.println("\n--- Docker Socket Files ---");
        String[] socketPaths = {
            "/var/run/docker.sock",
            "/run/docker.sock",
            System.getenv("HOME") + "/.docker/run/docker.sock",
            "/run/user/" + getUid() + "/docker.sock",
            "/run/user/" + getUid() + "/podman/podman.sock"
        };

        for (String path : socketPaths) {
            if (path != null) {
                File socket = new File(path);
                System.out.println(path + ": exists=" + socket.exists() + ", canRead=" + socket.canRead());
            }
        }

        // Check testcontainers properties file
        System.out.println("\n--- Testcontainers Config ---");
        File tcProps = new File(System.getenv("HOME") + "/.testcontainers.properties");
        System.out.println("~/.testcontainers.properties exists: " + tcProps.exists());

        // Try to get Docker client info
        System.out.println("\n--- Testcontainers Docker Client ---");
        try {
            boolean available = DockerClientFactory.instance().isDockerAvailable();
            System.out.println("Docker available via Testcontainers: " + available);

            if (available) {
                String dockerHostIp = DockerClientFactory.instance().dockerHostIpAddress();
                System.out.println("Docker host IP: " + dockerHostIp);
            }
        } catch (Exception e) {
            System.err.println("Error checking Docker via Testcontainers: " + e.getClass().getName());
            System.err.println("Message: " + e.getMessage());
            if (e.getCause() != null) {
                System.err.println("Cause: " + e.getCause().getClass().getName() + ": " + e.getCause().getMessage());
            }
        }

        System.out.println("\n=== End Diagnostics ===");
    }

    @Test
    void checkDockerAvailable() {
        System.out.println("=== Docker Availability Test ===");

        try {
            boolean available = DockerClientFactory.instance().isDockerAvailable();
            System.out.println("Docker available: " + available);
            assertTrue(available, "Docker should be available. Run checkDockerEnvironment test for diagnostics.");

            String dockerHostIp = DockerClientFactory.instance().dockerHostIpAddress();
            System.out.println("Docker host IP: " + dockerHostIp);
            assertNotNull(dockerHostIp);

        } catch (Exception e) {
            System.err.println("Error checking Docker: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }

    @Test
    void checkPostgreSQLContainer() {
        System.out.println("=== PostgreSQL Container Test ===");
        System.out.println("Starting PostgreSQL container...");

        try (PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine")) {
            postgres.start();

            System.out.println("Container started successfully!");
            System.out.println("  Host: " + postgres.getHost());
            System.out.println("  Port: " + postgres.getMappedPort(5432));
            System.out.println("  Database: " + postgres.getDatabaseName());
            System.out.println("  Username: " + postgres.getUsername());
            System.out.println("  JDBC URL: " + postgres.getJdbcUrl());

            assertTrue(postgres.isRunning(), "PostgreSQL container should be running");
        }

        System.out.println("Container stopped and cleaned up.");
    }

    private String getUid() {
        try {
            Process p = Runtime.getRuntime().exec("id -u");
            byte[] output = p.getInputStream().readAllBytes();
            return new String(output).trim();
        } catch (Exception e) {
            return "1000";
        }
    }

}
