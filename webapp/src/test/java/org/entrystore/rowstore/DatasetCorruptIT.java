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
 * Integration tests for corrupt CSV error handling.
 * Specification: 06-dataset-corrupt.md
 *
 * Tests that RowStore properly detects and reports errors when processing
 * malformed CSV files (e.g., column count mismatch).
 */
@DisplayName("Dataset Corrupt CSV Tests")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DatasetCorruptIT extends BaseIntegrationTest {

    private static final String TEST_FILE = "dataset4_corrupt.csv";

    private String datasetUrl;
    private String infoUrl;

    @BeforeAll
    void submitCorruptCsv() {
        byte[] csvData = loadTestData(TEST_FILE);
        Response response = createDataset(csvData);

        response.then()
                .statusCode(202)
                .body("url", notNullValue())
                .body("info", notNullValue());

        datasetUrl = getDatasetUrl(response);
        infoUrl = getInfoUrl(response);

        // Wait for error status before running tests
        waitForDatasetError(infoUrl);
    }

    @AfterAll
    void cleanup() {
        if (datasetUrl != null) {
            // Delete dataset in error state - should still return 204
            given().delete(datasetUrl)
                    .then()
                    .statusCode(204);
        }
    }

    @Test
    @Order(1)
    @DisplayName("TC-DATASET4-001: Corrupt CSV is accepted for processing (async error model)")
    void corruptCsvAccepted_asyncErrorModel() {
        // Test that corrupt CSV is initially accepted with 202
        // This demonstrates the async error model: files are accepted for processing,
        // errors are detected during ETL and reflected in status

        byte[] csvData = loadTestData(TEST_FILE);
        Response response = given()
                .spec(csvSpec)
                .body(csvData)
        .when()
                .post("/datasets");

        // Verify 202 Accepted - corrupt file is accepted for processing
        response.then()
                .statusCode(202)
                .contentType(ContentType.JSON)
                .body("id", notNullValue())
                .body("url", notNullValue())
                .body("url", containsString("/dataset/"))
                .body("info", notNullValue())
                .body("status", instanceOf(Integer.class));

        // Verify Location header is present
        assertThat(response.getHeader("Location")).isNotNull();

        // Wait for error and cleanup
        String testUrl = response.jsonPath().getString("url");
        String testInfoUrl = response.jsonPath().getString("info");
        waitForDatasetError(testInfoUrl);
        given().delete(testUrl).then().statusCode(204);
    }

    @Test
    @Order(2)
    @DisplayName("TC-DATASET4-002: Dataset status shows ERROR after processing")
    void datasetStatus_showsError() {
        given()
                .spec(jsonSpec)
        .when()
                .get(infoUrl)
        .then()
                .statusCode(200)
                .body("status", equalTo(ETL_STATUS_ERROR));
    }

    @Test
    @Order(3)
    @DisplayName("TC-DATASET4-003: Query dataset in ERROR state (behavior check)")
    void queryErrorDataset_behaviorCheck() {
        // Note: The application intentionally allows queries on ERROR status datasets
        // because an error might occur during an update while the dataset still has
        // valid data from before the failed operation. This is documented behavior.
        // We verify the response is either 200 (data available) or 424 (no data)
        int statusCode = given()
                .spec(jsonSpec)
        .when()
                .get(datasetUrl)
        .then()
                .extract()
                .statusCode();

        // Either the dataset has no data (returns 200 with empty results)
        // or it may return data from a previous successful load
        assertThat(statusCode).isIn(200, 424);
    }

    @Test
    @Order(4)
    @DisplayName("TC-DATASET4-004: Delete dataset in ERROR state succeeds")
    void deleteErrorDataset_succeeds() {
        // Create another corrupt dataset specifically for delete test
        byte[] csvData = loadTestData(TEST_FILE);
        Response response = createDataset(csvData);

        String deleteUrl = response.jsonPath().getString("url");
        String deleteInfoUrl = response.jsonPath().getString("info");

        // Wait for error status
        waitForDatasetError(deleteInfoUrl);

        // Delete should succeed with 204
        given()
                .delete(deleteUrl)
        .then()
                .statusCode(204);

        // Verify dataset no longer exists
        given()
                .spec(jsonSpec)
                .get(deleteInfoUrl)
        .then()
                .statusCode(404);
    }

}
