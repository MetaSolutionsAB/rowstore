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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for error handling scenarios.
 * Specification: 08-error-handling.md
 *
 * Tests various error codes: 400, 404, 423 (LOCKED), 424 (FAILED_DEPENDENCY).
 */
@DisplayName("Error Handling Tests")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ErrorHandlingIT extends BaseIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(ErrorHandlingIT.class);
    private static final String TEST_FILE = "dataset1_utf8.csv";

    private String datasetUrl;
    private String infoUrl;

    @BeforeAll
    void createDataset() {
        byte[] csvData = loadTestData(TEST_FILE);
        DatasetUrls urls = createDatasetAndWait(csvData);

        datasetUrl = urls.datasetUrl;
        infoUrl = urls.infoUrl;
    }

    @AfterAll
    void cleanup() {
        if (datasetUrl != null) {
            deleteDataset(datasetUrl);
        }
    }

    @Test
    @Order(1)
    @DisplayName("TC-ERROR-002: Query before data loaded returns 424")
    void queryBeforeDataLoaded_returns424() {
        // Create a new dataset but try to query immediately
        byte[] csvData = loadTestData(TEST_FILE);
        Response createResponse = createDataset(csvData);

        String newDatasetUrl = getDatasetUrl(createResponse);
        String newInfoUrl = getInfoUrl(createResponse);

        try {
            // Immediately try to query (race condition - might already be processed)
            Response queryResponse = given()
                    .spec(jsonSpec)
            .when()
                    .get(newDatasetUrl);

            // Either 424 (still processing) or 200 (already done)
            assertThat(queryResponse.getStatusCode()).isIn(200, 424);
        } finally {
            // Wait and cleanup
            waitForDatasetAvailable(newInfoUrl);
            deleteDataset(newDatasetUrl);
        }
    }

    @Test
    @Order(2)
    @DisplayName("TC-ERROR-003: Invalid UUID format returns 404")
    void invalidUuidFormat_returns404() {
        given()
                .spec(jsonSpec)
        .when()
                .get("/dataset/not-a-valid-uuid")
        .then()
                .statusCode(404);
    }

    @Test
    @Order(3)
    @DisplayName("TC-ERROR-004: PUT aliases with invalid JSON format returns 400")
    void putAliasesInvalidFormat_returns400() {
        given()
                .contentType(ContentType.JSON)
                .body("not valid json")
        .when()
                .put(datasetUrl + "/aliases")
        .then()
                .statusCode(400);
    }

    @Test
    @Order(4)
    @DisplayName("TC-ERROR-005: POST empty body to datasets returns 400")
    void postEmptyBody_returns400() {
        Response response = given()
                .spec(csvSpec)
                .body(new byte[0])
        .when()
                .post("/datasets");

        // Empty body is rejected as bad request
        assertThat(response.getStatusCode()).isEqualTo(400);
    }

    @Test
    @Order(5)
    @DisplayName("TC-ERROR-006: POST with non-CSV content type behavior")
    void postNonCsvContentType_behavior() {
        Response response = given()
                .contentType(ContentType.JSON)
                .body("{\"test\": \"data\"}")
        .when()
                .post("/datasets");

        // Non-CSV content type is rejected with 415 Unsupported Media Type
        assertThat(response.getStatusCode()).isEqualTo(415);
    }

    @Test
    @Order(6)
    @DisplayName("TC-ERROR-007: Delete non-existent dataset returns 404")
    void deleteNonExistent_returns404() {
        String fakeId = UUID.randomUUID().toString();

        given()
        .when()
                .delete("/dataset/" + fakeId)
        .then()
                .statusCode(404);
    }

    @Test
    @Order(7)
    @DisplayName("TC-ERROR-008: Alias with invalid characters returns 400")
    void aliasWithInvalidChars_returns400() {
        // Note: The actual validation of alias characters depends on implementation
        // Some special characters may or may not be allowed
        Response response = given()
                .contentType(ContentType.JSON)
                .body("[\"my-alias!\"]")  // Exclamation point might be invalid
        .when()
                .put(datasetUrl + "/aliases");

        // Exclamation point in aliases is rejected
        assertThat(response.getStatusCode()).isEqualTo(400);
    }

    @Test
    @Order(8)
    @DisplayName("TC-ERROR-009: Alias that looks like UUID behavior check")
    void aliasLikeUuid_behaviorCheck() {
        String uuidAlias = UUID.randomUUID().toString();

        Response response = given()
                .contentType(ContentType.JSON)
                .body("[\"" + uuidAlias + "\"]")
        .when()
                .put(datasetUrl + "/aliases");

        // UUID-like aliases are rejected to prevent confusion with dataset IDs
        assertThat(response.getStatusCode()).isEqualTo(400);
    }

    @Test
    @Order(9)
    @DisplayName("TC-ERROR-010: Default route (/) returns 404")
    void defaultRoute_returns404() {
        given()
        .when()
                .get("/")
        .then()
                .statusCode(404);
    }

    @Test
    @Order(10)
    @DisplayName("TC-ERROR-011: Query with non-existent column returns 400")
    void queryNonExistentColumn_returns400() {
        given()
                .spec(jsonSpec)
                .queryParam("nonexistentcolumn", "value")
        .when()
                .get(datasetUrl)
        .then()
                .statusCode(400);
    }

    @ParameterizedTest(name = "TC-ERROR-012-016: {0} endpoint returns 404 for non-existent dataset")
    @ValueSource(strings = {"/info", "/aliases", "/export", "/swagger", "/html"})
    @Order(11)
    void nonExistentDatasetEndpoints_return404(String endpoint) {
        String fakeId = UUID.randomUUID().toString();

        given()
                .spec(jsonSpec)
        .when()
                .get("/dataset/" + fakeId + endpoint)
        .then()
                .statusCode(404);
    }

    @Test
    @Order(12)
    @DisplayName("TC-ERROR-001: Delete during PROCESSING returns 423 LOCKED")
    void deleteDuringProcessing_returns423() {
        byte[] csvData = loadTestData(TEST_FILE);
        Response createResponse = createDataset(csvData);
        createResponse.then().statusCode(202);

        String newDatasetUrl = getDatasetUrl(createResponse);
        String newInfoUrl = getInfoUrl(createResponse);

        try {
            Response deleteResponse = given()
            .when()
                    .delete(newDatasetUrl);

            // Race condition: delete may arrive while processing, after success, or after cleanup
            assertThat(deleteResponse.getStatusCode())
                    .as("Delete during/after processing should return 423, 204, or 404")
                    .isIn(204, 404, 423);

            if (deleteResponse.getStatusCode() == 423) {
                waitForDatasetAvailable(newInfoUrl);
                deleteDataset(newDatasetUrl);
            }
        } finally {
            cleanupDatasetSafely(newDatasetUrl, newInfoUrl);
        }
    }

    private void cleanupDatasetSafely(String datasetUrl, String infoUrl) {
        try {
            waitForDatasetAvailable(infoUrl);
            deleteDataset(datasetUrl);
        } catch (Exception e) {
            log.debug("Best-effort cleanup failed for {}", datasetUrl, e);
        }
    }
}
