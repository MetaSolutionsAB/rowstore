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

import io.restassured.http.ContentType;
import io.restassured.response.Response;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Integration tests for extended /status endpoint functionality.
 * Specification: 01-status.md
 *
 * Tests JVM status information and dataset count tracking.
 */
@DisplayName("Extended Status Endpoint Tests")
class StatusExtendedIT extends BaseIntegrationTest {

    @Test
    @DisplayName("TC-STATUS-002: GET status with JVM parameter returns JVM info")
    void getStatusWithJvm_returnsJvmInfo() {
        given()
                .spec(jsonSpec)
        .when()
                .get("/status?jvm")
        .then()
                .statusCode(200)
                .contentType(ContentType.JSON)
                .body("totalMemory", instanceOf(Number.class))
                .body("freeMemory", instanceOf(Number.class))
                .body("maxMemory", instanceOf(Number.class))
                .body("availableProcessors", instanceOf(Number.class))
                .body("totalCommittedMemory", instanceOf(Number.class))
                .body("committedHeap", instanceOf(Number.class))
                .body("totalUsedMemory", instanceOf(Number.class))
                .body("usedHeap", instanceOf(Number.class));
    }

    @Test
    @DisplayName("TC-STATUS-003: JVM memory values are positive")
    void jvmMemoryValues_arePositive() {
        Response response = given()
                .spec(jsonSpec)
        .when()
                .get("/status?jvm")
        .then()
                .statusCode(200)
                .body("availableProcessors", greaterThan(0))
                .extract()
                .response();

        // Memory values come back as integers/longs, verify they're positive
        long totalMemory = response.jsonPath().getLong("totalMemory");
        long freeMemory = response.jsonPath().getLong("freeMemory");
        long maxMemory = response.jsonPath().getLong("maxMemory");
        long committedHeap = response.jsonPath().getLong("committedHeap");
        long usedHeap = response.jsonPath().getLong("usedHeap");

        assertThat(totalMemory).isGreaterThan(0);
        assertThat(freeMemory).isGreaterThanOrEqualTo(0);
        assertThat(maxMemory).isGreaterThan(0);
        assertThat(committedHeap).isGreaterThan(0);
        assertThat(usedHeap).isGreaterThan(0);
    }

    @Test
    @DisplayName("TC-STATUS-004: Status datasets count increases after creation")
    void statusDatasetsCount_increasesAfterCreation() {
        // Get initial count
        int initialCount = getDatasetCount();

        // Create a dataset
        byte[] csvData = loadTestData("dataset1_utf8.csv");
        DatasetUrls urls = createDatasetAndWait(csvData);

        try {
            // Verify count increased
            int newCount = getDatasetCount();
            assertThat(newCount).isEqualTo(initialCount + 1);
        } finally {
            // Cleanup
            deleteDataset(urls.datasetUrl);
        }

        // Verify count decreased after delete
        int finalCount = getDatasetCount();
        assertThat(finalCount).isEqualTo(initialCount);
    }

    @Test
    @DisplayName("TC-STATUS-005: Status activeEtlProcesses tracking")
    void statusActiveEtlProcesses_tracking() {
        // Note: This test verifies that activeEtlProcesses is a valid non-negative value
        // Testing actual process tracking during ETL would require precise timing

        Response response = given()
                .spec(jsonSpec)
        .when()
                .get("/status")
        .then()
                .statusCode(200)
                .body("activeEtlProcesses", instanceOf(Integer.class))
                .body("activeEtlProcesses", greaterThanOrEqualTo(0))
                .extract()
                .response();

        int activeProcesses = response.jsonPath().getInt("activeEtlProcesses");
        assertThat(activeProcesses).isGreaterThanOrEqualTo(0);
    }
}
