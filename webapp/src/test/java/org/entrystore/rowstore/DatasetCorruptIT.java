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
import static org.hamcrest.Matchers.equalTo;
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
    }

    @AfterAll
    void cleanup() {
        if (datasetUrl != null) {
            // Attempt cleanup - may fail if dataset is in error state
            given().delete(datasetUrl);
        }
    }

    @Test
    @Order(1)
    @DisplayName("TC-DATASET4-001: Corrupt CSV is accepted for processing")
    void corruptCsvAccepted() {
        // Verified in @BeforeAll - creation returned 202
        // The file is accepted initially; error occurs during async ETL processing
    }

    @Test
    @Order(2)
    @DisplayName("TC-DATASET4-002: Dataset status shows ERROR after processing")
    void datasetStatus_showsError() {
        waitForDatasetError(infoUrl);

        given()
                .spec(jsonSpec)
        .when()
                .get(infoUrl)
        .then()
                .statusCode(200)
                .body("status", equalTo(ETL_STATUS_ERROR));
    }

}
