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

import java.util.List;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests for semicolon-separated CSV handling.
 * Specification: 04-dataset-semicolon.md
 *
 * Tests RowStore's ability to detect and parse semicolon-delimited CSV files.
 */
@DisplayName("Dataset Semicolon CSV Tests")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DatasetSemicolonIT extends BaseIntegrationTest {

    private static final String TEST_FILE = "dataset2_utf8_semicolon.csv";

    private String datasetUrl;
    private String infoUrl;

    @BeforeAll
    void createDataset() {
        byte[] csvData = loadTestData(TEST_FILE);
        Response response = createDataset(csvData);

        response.then()
                .statusCode(202)
                .contentType(ContentType.JSON)
                .body("id", notNullValue())
                .body("url", notNullValue())
                .body("info", notNullValue())
                .body("status", instanceOf(Integer.class));

        datasetUrl = getDatasetUrl(response);
        infoUrl = getInfoUrl(response);

        waitForDatasetAvailable(infoUrl);
    }

    @AfterAll
    void cleanup() {
        if (datasetUrl != null) {
            given().delete(datasetUrl);
        }
    }

    @Test
    @Order(1)
    @DisplayName("TC-DATASET2-001: Dataset created from semicolon CSV")
    void datasetCreated() {
        // Verified in @BeforeAll - creation returned 202
        // This test confirms the dataset is available
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
    @DisplayName("TC-DATASET2-002: GET dataset info shows correct row count")
    void getDatasetInfo_correctRowCount() {
        given()
                .spec(jsonSpec)
        .when()
                .get(infoUrl)
        .then()
                .statusCode(200)
                .contentType(ContentType.JSON)
                .body("rowcount", equalTo(5))
                .body("status", equalTo(ETL_STATUS_AVAILABLE))
                .body("columnnames", instanceOf(List.class));
    }

    @Test
    @Order(3)
    @DisplayName("TC-DATASET2-003: Query with exact match verifies correct parsing")
    void query_verifiesCorrectParsing() {
        given()
                .spec(jsonSpec)
                .queryParam("Name", "Béringer")
        .when()
                .get(datasetUrl)
        .then()
                .statusCode(200)
                .contentType(ContentType.JSON)
                .body("results[0].name", equalTo("Béringer"))
                .body("results[0].comment", equalTo("No, no comment"));
    }

}
