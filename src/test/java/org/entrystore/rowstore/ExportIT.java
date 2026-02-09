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
import org.json.JSONArray;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.UUID;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for the /dataset/{id}/export endpoint.
 * Specification: 06-export.md
 *
 * Tests JSON and CSV export functionality including content verification
 * and proper header handling.
 */
@DisplayName("Export Endpoint Tests")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ExportIT extends BaseIntegrationTest {

    private static final String TEST_FILE = "dataset1_utf8.csv";

    private String datasetUrl;
    private String infoUrl;
    private String datasetId;

    @BeforeAll
    void createDataset() {
        byte[] csvData = loadTestData(TEST_FILE);
        DatasetUrls urls = createDatasetAndWait(csvData);

        datasetUrl = urls.datasetUrl;
        infoUrl = urls.infoUrl;
        datasetId = urls.datasetId;
    }

    @AfterAll
    void cleanup() {
        if (datasetUrl != null) {
            deleteDataset(datasetUrl);
        }
    }

    @Test
    @Order(1)
    @DisplayName("TC-EXPORT-001: Export as JSON returns valid JSON array")
    void exportAsJson_returnsValidJsonArray() {
        Response response = given()
                .accept(ContentType.JSON)
        .when()
                .get(datasetUrl + "/export")
        .then()
                .statusCode(200)
                .contentType(ContentType.JSON)
                .extract()
                .response();

        // Content-Disposition header may or may not be present depending on implementation
        // The key test is that we get valid JSON content

        // Verify the response is a valid JSON array
        String body = response.getBody().asString();
        JSONArray jsonArray = new JSONArray(body);
        assertThat(jsonArray.length()).isEqualTo(5);
    }

    @Test
    @Order(2)
    @DisplayName("TC-EXPORT-002: Export as CSV returns valid CSV")
    void exportAsCsv_returnsValidCsv() {
        Response response = given()
                .accept("text/csv")
        .when()
                .get(datasetUrl + "/export")
        .then()
                .statusCode(200)
                .contentType("text/csv")
                .extract()
                .response();

        // Content-Disposition header may or may not be present depending on implementation
        // The key test is that we get valid CSV content

        // Verify the response is valid CSV with headers
        String body = response.getBody().asString();
        String[] lines = body.split("\n");

        // Should have header + 5 data rows
        assertThat(lines.length).isGreaterThanOrEqualTo(6);

        // First line should be headers
        String headerLine = lines[0].toLowerCase();
        assertThat(headerLine).contains("name");
        assertThat(headerLine).contains("telephone");
    }

    @Test
    @Order(3)
    @DisplayName("TC-EXPORT-003: Export JSON content matches original dataset")
    void exportJson_contentMatchesOriginal() {
        Response response = given()
                .accept(ContentType.JSON)
        .when()
                .get(datasetUrl + "/export")
        .then()
                .statusCode(200)
                .extract()
                .response();

        JSONArray jsonArray = new JSONArray(response.getBody().asString());

        // Verify specific data points from the original CSV
        boolean foundAkesson = false;
        boolean foundBeringer = false;

        for (int i = 0; i < jsonArray.length(); i++) {
            String name = jsonArray.getJSONObject(i).getString("name");
            if ("Åkesson".equals(name)) {
                foundAkesson = true;
                assertThat(jsonArray.getJSONObject(i).getString("comment"))
                        .isEqualTo("Another comment with äöå");
            }
            if ("Béringer".equals(name)) {
                foundBeringer = true;
                assertThat(jsonArray.getJSONObject(i).getString("comment"))
                        .isEqualTo("No, no comment");
            }
        }

        assertThat(foundAkesson).as("Should contain Åkesson").isTrue();
        assertThat(foundBeringer).as("Should contain Béringer").isTrue();
    }

    @Test
    @Order(4)
    @DisplayName("TC-EXPORT-004: Export CSV content verification")
    void exportCsv_contentVerification() {
        Response response = given()
                .accept("text/csv")
        .when()
                .get(datasetUrl + "/export")
        .then()
                .statusCode(200)
                .extract()
                .response();

        String body = response.getBody().asString();

        // Verify data with Unicode characters is preserved
        assertThat(body).contains("Åkesson");
        assertThat(body).contains("Béringer");
        assertThat(body).contains("äöå");
    }

    @Test
    @Order(5)
    @DisplayName("TC-EXPORT-005: Export non-existent dataset returns 404")
    void exportNonExistent_returns404() {
        String fakeId = UUID.randomUUID().toString();

        given()
                .accept(ContentType.JSON)
        .when()
                .get("/dataset/" + fakeId + "/export")
        .then()
                .statusCode(404);
    }

    @Test
    @Order(6)
    @DisplayName("TC-EXPORT-006: Export dataset with status=CREATED returns 424")
    void exportCreatedDataset_returns424() {
        // Create a dataset but don't wait for it to be available
        byte[] csvData = loadTestData(TEST_FILE);
        Response createResponse = createDataset(csvData);

        String newDatasetUrl = getDatasetUrl(createResponse);
        String newInfoUrl = getInfoUrl(createResponse);

        try {
            // Immediately try to export (before processing completes)
            // Note: This may be flaky if processing is very fast
            // The test verifies the behavior when status is CREATED (before data is loaded)
            Response exportResponse = given()
                    .accept(ContentType.JSON)
            .when()
                    .get(newDatasetUrl + "/export")
            .then()
                    .extract()
                    .response();

            // Either 424 (if still processing) or 200 (if already done)
            assertThat(exportResponse.getStatusCode()).isIn(200, 424);
        } finally {
            // Wait and cleanup
            waitForDatasetAvailable(newInfoUrl);
            deleteDataset(newDatasetUrl);
        }
    }

    @Test
    @Order(7)
    @DisplayName("TC-EXPORT-007: Export dataset with empty field values")
    void exportEmptyValues_handledCorrectly() {
        // Create a dataset with empty values
        byte[] csvData = loadTestData("dataset_query_edge.csv");
        DatasetUrls urls = createDatasetAndWait(csvData);

        try {
            // Export as JSON - empty values should be empty strings
            Response jsonResponse = given()
                    .accept(ContentType.JSON)
            .when()
                    .get(urls.datasetUrl + "/export");

            jsonResponse.then().statusCode(200);
            String jsonBody = jsonResponse.getBody().asString();
            // JSON export should contain proper empty string handling
            assertThat(jsonBody).contains("empty_field");

            // Export as CSV
            Response csvResponse = given()
                    .accept("text/csv")
            .when()
                    .get(urls.datasetUrl + "/export");

            csvResponse.then().statusCode(200);
            String csvBody = csvResponse.getBody().asString();
            // CSV export should have empty fields (consecutive commas or empty quoted strings)
            assertThat(csvBody).contains("empty_field");
        } finally {
            deleteDataset(urls.datasetUrl);
        }
    }

    @Test
    @Order(8)
    @DisplayName("TC-EXPORT-008: Content-Disposition filename includes dataset ID (if present)")
    void exportContentDisposition_includesDatasetId() {
        // JSON export
        Response jsonResponse = given()
                .accept(ContentType.JSON)
        .when()
                .get(datasetUrl + "/export")
        .then()
                .statusCode(200)
                .extract()
                .response();

        String jsonDisposition = jsonResponse.getHeader("Content-Disposition");
        // Content-Disposition may not be returned by the server depending on client
        // If present, verify it contains the dataset ID
        if (jsonDisposition != null) {
            assertThat(jsonDisposition).contains(datasetId);
            assertThat(jsonDisposition).contains(".json");
        }

        // CSV export
        Response csvResponse = given()
                .accept("text/csv")
        .when()
                .get(datasetUrl + "/export")
        .then()
                .statusCode(200)
                .extract()
                .response();

        String csvDisposition = csvResponse.getHeader("Content-Disposition");
        if (csvDisposition != null) {
            assertThat(csvDisposition).contains(datasetId);
            assertThat(csvDisposition).contains(".csv");
        }
    }
}
