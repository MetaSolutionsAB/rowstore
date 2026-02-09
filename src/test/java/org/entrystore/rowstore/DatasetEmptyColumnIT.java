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
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests for CSV with empty column labels.
 * Specification: 07-dataset-empty-column.md
 *
 * Tests that RowStore handles columns without names gracefully,
 * excluding empty labels from the column names list.
 */
@DisplayName("Dataset Empty Column Label Tests")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DatasetEmptyColumnIT extends BaseIntegrationTest {

    private static final String TEST_FILE = "dataset5_utf8_emptycolumn.csv";

    private String datasetUrl;
    private String infoUrl;

    @BeforeAll
    void createDataset() {
        byte[] csvData = loadTestData(TEST_FILE);
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
    @DisplayName("TC-DATASET5-001: Dataset created from CSV with empty column labels (edge case handling)")
    void datasetCreated_edgeCaseHandling() {
        // Test creation assertions for edge case handling
        byte[] csvData = loadTestData(TEST_FILE);
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

        // Also verify main dataset is available
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
    @DisplayName("TC-DATASET5-002: Column count excludes empty labels")
    void columnCount_excludesEmptyLabels() {
        // The CSV has 4 named columns plus 2 empty column labels at the end
        // Only the 4 named columns should be in columnnames
        given()
                .spec(jsonSpec)
        .when()
                .get(infoUrl)
        .then()
                .statusCode(200)
                .contentType(ContentType.JSON)
                .body("columnnames", hasSize(4));
    }

    @Test
    @Order(3)
    @DisplayName("TC-DATASET5-003: Dataset with empty column labels is queryable")
    void datasetWithEmptyColumns_isQueryable() {
        // Verify the dataset is queryable despite having empty column labels
        Response response = given()
                .spec(jsonSpec)
        .when()
                .get(datasetUrl);

        response.then()
                .statusCode(200)
                .contentType(ContentType.JSON)
                .body("results", notNullValue())
                .body("resultCount", equalTo(5));
    }

}
