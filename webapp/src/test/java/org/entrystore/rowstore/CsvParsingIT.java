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

import io.restassured.http.ContentType;
import io.restassured.response.Response;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;

/**
 * Integration tests for CSV parsing edge cases.
 * Specification: 04-csv-formats.md
 *
 * Tests various CSV format edge cases including headers only, duplicate columns,
 * long fields, embedded newlines, whitespace in headers, and quoted delimiters.
 */
@DisplayName("CSV Parsing Edge Cases Tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class CsvParsingIT extends BaseIntegrationTest {

    @Test
    @Order(1)
    @DisplayName("TC-CSV-001: Headers only, no data rows")
    void headersOnly_noDataRows() {
        byte[] csvData = loadTestData("headers_only.csv");
        DatasetUrls urls = createDatasetAndWait(csvData);

        try {
            // Dataset should be available with 0 rows
            given()
                    .spec(jsonSpec)
            .when()
                    .get(urls.infoUrl)
            .then()
                    .statusCode(200)
                    .body("status", equalTo(ETL_STATUS_AVAILABLE))
                    .body("rowcount", equalTo(0));

            // Query should return empty results
            given()
                    .spec(jsonSpec)
            .when()
                    .get(urls.datasetUrl)
            .then()
                    .statusCode(200)
                    .body("resultCount", equalTo(0));
        } finally {
            deleteDataset(urls.datasetUrl);
        }
    }

    @Test
    @Order(2)
    @DisplayName("TC-CSV-002: Duplicate column names behavior")
    void duplicateColumnNames() {
        byte[] csvData = loadTestData("duplicate_cols.csv");
        Response createResponse = createDataset(csvData);
        createResponse.then().statusCode(202);

        String datasetUrl = getDatasetUrl(createResponse);
        String infoUrl = getInfoUrl(createResponse);

        try {
            // Poll for either AVAILABLE or ERROR (both are valid for duplicate columns)
            await().atMost(MAX_WAIT).pollInterval(POLL_INTERVAL)
                    .until(() -> {
                        int s = given().spec(jsonSpec).get(infoUrl).jsonPath().getInt("status");
                        return s == ETL_STATUS_AVAILABLE || s == ETL_STATUS_ERROR;
                    });

            int finalStatus = given().spec(jsonSpec).get(infoUrl).jsonPath().getInt("status");
            if (finalStatus == ETL_STATUS_AVAILABLE) {
                // If it succeeded, check how duplicates were handled
                Response infoResponse = given()
                        .spec(jsonSpec)
                .when()
                        .get(infoUrl);
                assertThat(infoResponse.jsonPath().getInt("rowcount")).isEqualTo(2);
            }
            // Error status is also acceptable for duplicate columns
        } finally {
            deleteDataset(datasetUrl);
        }
    }

    @Test
    @Order(3)
    @DisplayName("TC-CSV-003: Very long field value (>256 chars)")
    void longFieldValue() {
        byte[] csvData = loadTestData("long_fields.csv");
        DatasetUrls urls = createDatasetAndWait(csvData);

        try {
            // Dataset should be available
            given()
                    .spec(jsonSpec)
            .when()
                    .get(urls.infoUrl)
            .then()
                    .statusCode(200)
                    .body("status", equalTo(ETL_STATUS_AVAILABLE))
                    .body("rowcount", equalTo(2));

            // Query should return the long field intact
            Response queryResponse = given()
                    .spec(jsonSpec)
                    .queryParam("name", "Test")
            .when()
                    .get(urls.datasetUrl);

            queryResponse.then().statusCode(200);
            String description = queryResponse.jsonPath().getString("results[0].description");
            assertThat(description.length()).isGreaterThan(256);
            assertThat(description).contains("This is a very long field value");
        } finally {
            deleteDataset(urls.datasetUrl);
        }
    }

    @Test
    @Order(4)
    @DisplayName("TC-CSV-004: Field with embedded newlines (RFC4180)")
    void embeddedNewlines() {
        byte[] csvData = loadTestData("embedded_newlines.csv");
        DatasetUrls urls = createDatasetAndWait(csvData);

        try {
            // Dataset should be available with 2 rows
            given()
                    .spec(jsonSpec)
            .when()
                    .get(urls.infoUrl)
            .then()
                    .statusCode(200)
                    .body("status", equalTo(ETL_STATUS_AVAILABLE))
                    .body("rowcount", equalTo(2));

            // Query should return the multiline address
            Response queryResponse = given()
                    .spec(jsonSpec)
                    .queryParam("name", "John Smith")
            .when()
                    .get(urls.datasetUrl);

            queryResponse.then().statusCode(200).body("resultCount", equalTo(1));
            String address = queryResponse.jsonPath().getString("results[0].address");
            assertThat(address).contains("Apt 4B");
            assertThat(address).contains("New York");
        } finally {
            deleteDataset(urls.datasetUrl);
        }
    }

    @Test
    @Order(5)
    @DisplayName("TC-CSV-005: Trailing whitespace in headers")
    void whitespaceInHeaders() {
        byte[] csvData = loadTestData("whitespace_headers.csv");
        DatasetUrls urls = createDatasetAndWait(csvData);

        try {
            // Dataset should be available
            Response infoResponse = given()
                    .spec(jsonSpec)
            .when()
                    .get(urls.infoUrl);

            infoResponse.then()
                    .statusCode(200)
                    .body("status", equalTo(ETL_STATUS_AVAILABLE))
                    .body("rowcount", equalTo(2));

            // Headers might be trimmed - check if query works
            // Try with trimmed column name
            Response queryResponse = given()
                    .spec(jsonSpec)
                    .queryParam("name", "John")
            .when()
                    .get(urls.datasetUrl);

            // Headers are trimmed, so query with trimmed name works
            assertThat(queryResponse.getStatusCode()).isEqualTo(200);
        } finally {
            deleteDataset(urls.datasetUrl);
        }
    }

    @Test
    @Order(6)
    @DisplayName("TC-CSV-006: Quoted fields with delimiter inside")
    void quotedFieldsWithDelimiter() {
        byte[] csvData = loadTestData("quoted_delim.csv");
        DatasetUrls urls = createDatasetAndWait(csvData);

        try {
            // Dataset should be available
            given()
                    .spec(jsonSpec)
            .when()
                    .get(urls.infoUrl)
            .then()
                    .statusCode(200)
                    .body("status", equalTo(ETL_STATUS_AVAILABLE))
                    .body("rowcount", equalTo(2));

            // Query should return correct data with commas preserved
            Response queryResponse = given()
                    .spec(jsonSpec)
            .when()
                    .get(urls.datasetUrl);

            queryResponse.then().statusCode(200);

            // Find the row with commas in the name
            String firstName = queryResponse.jsonPath().getString("results[0].name");
            String firstDesc = queryResponse.jsonPath().getString("results[0].description");

            // The name should have the comma preserved
            assertThat(firstName).contains(",");
            // The description should have commas preserved
            assertThat(firstDesc).contains(",");
        } finally {
            deleteDataset(urls.datasetUrl);
        }
    }

    @Test
    @Order(7)
    @DisplayName("TC-CSV-007: Dataset query edge cases file")
    void queryEdgeCasesDataset() {
        byte[] csvData = loadTestData("dataset_query_edge.csv");
        DatasetUrls urls = createDatasetAndWait(csvData);

        try {
            // Verify dataset loaded correctly
            given()
                    .spec(jsonSpec)
            .when()
                    .get(urls.infoUrl)
            .then()
                    .statusCode(200)
                    .body("status", equalTo(ETL_STATUS_AVAILABLE))
                    .body("rowcount", equalTo(5));

            // Test query with SQL-like characters (O'Brien)
            Response queryResponse = given()
                    .spec(jsonSpec)
                    .queryParam("name", "O'Brien")
            .when()
                    .get(urls.datasetUrl);

            queryResponse.then()
                    .statusCode(200)
                    .body("resultCount", equalTo(1));
        } finally {
            deleteDataset(urls.datasetUrl);
        }
    }

    @Test
    @Order(8)
    @DisplayName("TC-CSV-008: Empty value handling")
    void emptyValueHandling() {
        byte[] csvData = loadTestData("dataset_query_edge.csv");
        DatasetUrls urls = createDatasetAndWait(csvData);

        try {
            // Query with empty filter - behavior depends on implementation
            Response queryResponse = given()
                    .spec(jsonSpec)
                    .queryParam("empty_field", "")
            .when()
                    .get(urls.datasetUrl);

            // Empty filter value is rejected as invalid
            assertThat(queryResponse.getStatusCode()).isEqualTo(400);
        } finally {
            deleteDataset(urls.datasetUrl);
        }
    }

    @Test
    @Order(9)
    @DisplayName("TC-CSV-009: ISO-8859-1 encoding detection")
    void iso8859EncodingDetection() {
        byte[] csvData = loadTestData("iso8859.csv");
        DatasetUrls urls = createDatasetAndWait(csvData);

        try {
            // Dataset should be available with 3 rows
            given()
                    .spec(jsonSpec)
            .when()
                    .get(urls.infoUrl)
            .then()
                    .statusCode(200)
                    .body("status", equalTo(ETL_STATUS_AVAILABLE))
                    .body("rowcount", equalTo(3));

            // Query should return correctly converted UTF-8 characters
            // Original ISO-8859-1: Müller should be converted to UTF-8 Müller
            Response queryResponse = given()
                    .spec(jsonSpec)
            .when()
                    .get(urls.datasetUrl);

            queryResponse.then().statusCode(200);

            // Check that the encoding was detected and characters converted
            String allResults = queryResponse.getBody().asString();
            // The response should contain properly converted characters
            // (either preserved or converted from ISO-8859-1 to UTF-8)
            assertThat(allResults).containsAnyOf("Müller", "M\\u00fcller", "München", "M\\u00fcnchen");
        } finally {
            deleteDataset(urls.datasetUrl);
        }
    }

    @Test
    @Order(10)
    @DisplayName("TC-CSV-010: Tab-delimited file handling")
    void tabDelimitedFile() {
        byte[] csvData = loadTestData("tab_delimited.csv");
        Response createResponse = createDataset(csvData);
        createResponse.then().statusCode(202);

        String datasetUrl = getDatasetUrl(createResponse);
        String infoUrl = getInfoUrl(createResponse);

        try {
            // Wait for processing and check the final status
            waitForProcessingComplete(infoUrl);

            Response infoResponse = given()
                    .spec(jsonSpec)
            .when()
                    .get(infoUrl);

            int status = infoResponse.jsonPath().getInt("status");

            // Tab delimiter may or may not be supported - both available and error are valid
            assertThat(status).isIn(ETL_STATUS_AVAILABLE, ETL_STATUS_ERROR);

            if (status == ETL_STATUS_AVAILABLE) {
                assertThat(infoResponse.jsonPath().getInt("rowcount")).isEqualTo(3);

                Response queryResponse = given()
                        .spec(jsonSpec)
                        .queryParam("name", "Alice")
                .when()
                        .get(datasetUrl);

                if (queryResponse.getStatusCode() == 200) {
                    assertThat(queryResponse.jsonPath().getInt("resultCount")).isEqualTo(1);
                }
            }
        } finally {
            given().delete(datasetUrl);
        }
    }

    /**
     * Waits for dataset processing to complete (either available or error).
     */
    private void waitForProcessingComplete(String infoUrl) {
        await()
                .atMost(MAX_WAIT)
                .pollInterval(POLL_INTERVAL)
                .until(() -> {
                    int status = given()
                            .spec(jsonSpec)
                            .get(infoUrl)
                            .jsonPath()
                            .getInt("status");
                    return status == ETL_STATUS_AVAILABLE || status == ETL_STATUS_ERROR;
                });
    }
}
