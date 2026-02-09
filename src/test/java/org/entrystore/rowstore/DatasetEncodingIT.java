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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests for Windows-1252 encoding and append/replace operations.
 * Specification: 05-dataset-encoding-append-replace.md
 *
 * Tests:
 * - Windows-1252 encoding detection and conversion to UTF-8
 * - POST to existing dataset appends data
 * - PUT to existing dataset replaces data
 */
@DisplayName("Dataset Encoding and Append/Replace Tests")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DatasetEncodingIT extends BaseIntegrationTest {

    private static final String WINDOWS1252_FILE = "dataset3_windows1252.csv";
    private static final String UTF8_SEMICOLON_FILE = "dataset2_utf8_semicolon.csv";

    private String datasetUrl;
    private String infoUrl;

    @BeforeAll
    void createDataset() {
        byte[] csvData = loadTestData(WINDOWS1252_FILE);
        Response response = createDataset(csvData);

        response.then()
                .statusCode(202)
                .body("url", notNullValue())
                .body("info", notNullValue());

        datasetUrl = getDatasetUrl(response);
        infoUrl = getInfoUrl(response);

        waitForDatasetAvailable(infoUrl);
    }

    @AfterAll
    void cleanup() {
        if (datasetUrl != null) {
            given().delete(datasetUrl)
                    .then()
                    .statusCode(204);
        }
    }

    @Test
    @Order(1)
    @DisplayName("TC-DATASET3-001: Dataset created from Windows-1252 CSV with encoding detection")
    void datasetCreated_withEncodingDetection() {
        // Create a fresh dataset to verify creation response for encoding detection test
        byte[] csvData = loadTestData(WINDOWS1252_FILE);
        Response response = given()
                .spec(csvSpec)
                .body(csvData)
        .when()
                .post("/datasets");

        // Verify 202 response structure per spec
        response.then()
                .statusCode(202)
                .contentType(ContentType.JSON)
                .body("id", notNullValue())
                .body("url", notNullValue())
                .body("url", containsString("/dataset/"))
                .body("info", notNullValue())
                .body("status", instanceOf(Integer.class));

        // Verify Location header
        assertThat(response.getHeader("Location")).isNotNull();

        // Clean up test dataset
        String testUrl = response.jsonPath().getString("url");
        String testInfoUrl = response.jsonPath().getString("info");
        waitForDatasetAvailable(testInfoUrl);
        deleteDataset(testUrl);

        // Also verify main dataset is available (from @BeforeAll)
        given()
                .spec(jsonSpec)
        .when()
                .get(infoUrl)
        .then()
                .statusCode(200)
                .body("status", equalTo(ETL_STATUS_AVAILABLE));
    }

    @Test
    @Order(2)
    @DisplayName("TC-DATASET3-002: GET dataset info shows 5 rows")
    void getDatasetInfo_shows5Rows() {
        given()
                .spec(jsonSpec)
        .when()
                .get(infoUrl)
        .then()
                .statusCode(200)
                .contentType(ContentType.JSON)
                .body("rowcount", equalTo(5))
                .body("status", equalTo(ETL_STATUS_AVAILABLE));
    }

    @Test
    @Order(3)
    @DisplayName("TC-DATASET3-003: Query verifies encoding conversion to UTF-8")
    void query_verifiesEncodingConversion() {
        given()
                .spec(jsonSpec)
                .queryParam("Name", "Åkesson")
        .when()
                .get(datasetUrl)
        .then()
                .statusCode(200)
                .contentType(ContentType.JSON)
                .body("results[0].name", equalTo("Åkesson"))
                .body("results[0].comment", equalTo("Another comment with äöå"));
    }

    @Test
    @Order(4)
    @DisplayName("TC-DATASET3-004: POST appends data to existing dataset")
    void postAppendsData() {
        byte[] appendData = loadTestData(UTF8_SEMICOLON_FILE);

        given()
                .spec(csvSpec)
                .body(appendData)
        .when()
                .post(datasetUrl)
        .then()
                .statusCode(202)
                .contentType(ContentType.JSON);

        // Wait for append to complete
        waitForDatasetAvailable(infoUrl);
    }

    @Test
    @Order(5)
    @DisplayName("TC-DATASET3-005: Verify row count and data integrity after append")
    void verifyRowCountAndDataAfterAppend() {
        // Verify row count
        given()
                .spec(jsonSpec)
        .when()
                .get(infoUrl)
        .then()
                .statusCode(200)
                .contentType(ContentType.JSON)
                .body("rowcount", equalTo(10))
                .body("status", equalTo(ETL_STATUS_AVAILABLE));

        // Verify data integrity - original rows should still be present
        Response response = given()
                .spec(jsonSpec)
                .queryParam("Name", "Åkesson")
        .when()
                .get(datasetUrl);

        // Should find rows from both original and appended data (both CSVs have Åkesson)
        response.then()
                .statusCode(200)
                .body("resultCount", equalTo(2));
    }

    @Test
    @Order(6)
    @DisplayName("TC-DATASET3-006: PUT replaces dataset content")
    void putReplacesData() {
        byte[] replaceData = loadTestData(WINDOWS1252_FILE);

        given()
                .spec(csvSpec)
                .body(replaceData)
        .when()
                .put(datasetUrl)
        .then()
                .statusCode(202)
                .contentType(ContentType.JSON);

        // Wait for replace to complete
        waitForDatasetAvailable(infoUrl);
    }

    @Test
    @Order(7)
    @DisplayName("TC-DATASET3-007: Verify row count after replace is 5")
    void verifyRowCountAfterReplace() {
        given()
                .spec(jsonSpec)
        .when()
                .get(infoUrl)
        .then()
                .statusCode(200)
                .contentType(ContentType.JSON)
                .body("rowcount", equalTo(5))
                .body("status", equalTo(ETL_STATUS_AVAILABLE));
    }

}
