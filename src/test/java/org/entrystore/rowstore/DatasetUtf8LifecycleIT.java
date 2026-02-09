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
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.List;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests for dataset lifecycle with UTF-8 CSV data.
 * Specification: 03-dataset-utf8-lifecycle.md
 *
 * Tests dataset creation, info retrieval, querying, pagination, and alias management.
 */
@DisplayName("Dataset UTF-8 Lifecycle Tests")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
class DatasetUtf8LifecycleIT extends BaseIntegrationTest {

    private static final String TEST_FILE = "dataset1_utf8.csv";

    private String datasetUrl;
    private String infoUrl;
    private String datasetId;

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
        datasetId = response.jsonPath().getString("id");

        waitForDatasetAvailable(infoUrl);
    }

    @AfterAll
    void cleanup() {
        if (datasetUrl != null) {
            // Remove aliases first to ensure clean delete
            given().delete(datasetUrl + "/aliases")
                    .then()
                    .statusCode(204);
            // Delete the dataset and verify 204 status
            given().delete(datasetUrl)
                    .then()
                    .statusCode(204);
        }
    }

    @Nested
    @Order(1)
    @DisplayName("Dataset Info Tests")
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class InfoTests {

        @Test
        @Order(1)
        @DisplayName("TC-DATASET1-001: Dataset creation returns 202 with Location header")
        void createDataset_returns202WithLocationHeader() {
            // Create another dataset to test creation assertions (main dataset created in @BeforeAll)
            byte[] csvData = loadTestData(TEST_FILE);
            Response response = given()
                    .spec(csvSpec)
                    .body(csvData)
            .when()
                    .post("/datasets");

            // Verify 202 Accepted response
            response.then()
                    .statusCode(202)
                    .contentType(ContentType.JSON)
                    .body("id", notNullValue())
                    .body("url", notNullValue())
                    .body("url", containsString("/dataset/"))
                    .body("info", notNullValue())
                    .body("info", containsString("/info"))
                    .body("status", instanceOf(Integer.class));

            // Verify Location header is present
            String locationHeader = response.getHeader("Location");
            assertThat(locationHeader).isNotNull();
            assertThat(locationHeader).contains("/dataset/");

            // Clean up the test dataset
            String testDatasetUrl = response.jsonPath().getString("url");
            String testInfoUrl = response.jsonPath().getString("info");
            waitForDatasetAvailable(testInfoUrl);
            deleteDataset(testDatasetUrl);
        }

        @Test
        @Order(2)
        @DisplayName("TC-DATASET1-002: GET dataset info returns valid structure")
        void getDatasetInfo_returnsValidStructure() {
            given()
                    .spec(jsonSpec)
            .when()
                    .get(infoUrl)
            .then()
                    .statusCode(200)
                    .contentType(ContentType.JSON)
                    .body("rowcount", equalTo(5))
                    .body("status", equalTo(ETL_STATUS_AVAILABLE))
                    .body("columnnames", instanceOf(List.class))
                    .body("created", notNullValue())
                    .body("aliases", instanceOf(List.class))
                    .body("identifier", notNullValue())
                    .body("@id", notNullValue())
                    .body("@context", notNullValue());
        }
    }

    @Nested
    @Order(2)
    @DisplayName("Query Tests")
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class QueryTests {

        @Test
        @Order(1)
        @DisplayName("TC-DATASET1-003: Query with exact match (Unicode characters)")
        void query_exactMatchUnicode() {
            given()
                    .spec(jsonSpec)
                    .queryParam("Name", "Åkesson")
            .when()
                    .get(datasetUrl)
            .then()
                    .statusCode(200)
                    .contentType(ContentType.JSON)
                    .body("queryTime", notNullValue())
                    .body("queryTime", greaterThanOrEqualTo(0))
                    .body("results", hasSize(1))
                    .body("results[0].name", equalTo("Åkesson"))
                    .body("results[0].comment", equalTo("Another comment with äöå"));
        }

        @Test
        @Order(2)
        @DisplayName("TC-DATASET1-004: Query keys are case-insensitive")
        void query_caseInsensitiveKey() {
            // Using "Name" (capitalized) should work the same as "name"
            Response response = given()
                    .spec(jsonSpec)
                    .queryParam("Name", "Åkesson")
            .when()
                    .get(datasetUrl);

            response.then().statusCode(200);
            assertThat(response.jsonPath().getList("results")).hasSize(1);
        }

        @Test
        @Order(3)
        @DisplayName("TC-DATASET1-005: Query with exact match (column with spaces)")
        void query_columnWithSpaces() {
            given()
                    .spec(jsonSpec)
                    .queryParam("Some other column", "x")
            .when()
                    .get(datasetUrl)
            .then()
                    .statusCode(200)
                    .contentType(ContentType.JSON)
                    .body("queryTime", notNullValue())
                    .body("queryTime", greaterThanOrEqualTo(0))
                    .body("results[0].name", equalTo("McLoud"))
                    .body("results[0].telephone", equalTo("0987654321"))
                    .body("results[0].'some other column'", equalTo("x"))
                    .body("results[0].comment", equalTo("A comment with five words, and a comma"));
        }

        @Test
        @Order(4)
        @DisplayName("TC-DATASET1-006: Query with regular expression")
        void query_withRegex() {
            // Matches names containing Å or é
            given()
                    .spec(jsonSpec)
                    .queryParam("Name", "(Å|é)")
            .when()
                    .get(datasetUrl)
            .then()
                    .statusCode(200)
                    .contentType(ContentType.JSON)
                    .body("results", hasSize(2));
        }

        @Test
        @Order(5)
        @DisplayName("TC-DATASET1-022: Query with tilde regex prefix")
        void query_tildeRegexPrefix() {
            // In full regex mode, ~ prefix forces regex interpretation
            given()
                    .spec(jsonSpec)
                    .queryParam("Name", "~Åke.*")
            .when()
                    .get(datasetUrl)
            .then()
                    .statusCode(200)
                    .contentType(ContentType.JSON)
                    .body("results", hasSize(1))
                    .body("results[0].name", equalTo("Åkesson"));
        }

        @Test
        @Order(6)
        @DisplayName("TC-DATASET1-007: Query with non-existing column returns 400")
        void query_nonExistingColumn_returns400() {
            given()
                    .spec(jsonSpec)
                    .queryParam("nonexistingkey", "test")
            .when()
                    .get(datasetUrl)
            .then()
                    .statusCode(400);
        }
    }

    @Nested
    @Order(3)
    @DisplayName("Pagination Tests")
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class PaginationTests {

        @Test
        @Order(1)
        @DisplayName("TC-DATASET1-008: Pagination - first page")
        void pagination_firstPage() {
            given()
                    .spec(jsonSpec)
                    .queryParam("Name", "(Å|é)")
                    .queryParam("_limit", 1)
            .when()
                    .get(datasetUrl)
            .then()
                    .statusCode(200)
                    .contentType(ContentType.JSON)
                    .body("results", hasSize(1))
                    .body("offset", equalTo(0))
                    .body("limit", equalTo(1))
                    .body("resultCount", equalTo(2))
                    .body("results[0].name", equalTo("Béringer"));
        }

        @Test
        @Order(2)
        @DisplayName("TC-DATASET1-009: Pagination - second page")
        void pagination_secondPage() {
            given()
                    .spec(jsonSpec)
                    .queryParam("Name", "(Å|é)")
                    .queryParam("_limit", 1)
                    .queryParam("_offset", 1)
            .when()
                    .get(datasetUrl)
            .then()
                    .statusCode(200)
                    .contentType(ContentType.JSON)
                    .body("results", hasSize(1))
                    .body("offset", equalTo(1))
                    .body("limit", equalTo(1))
                    .body("resultCount", equalTo(2))
                    .body("results[0].name", equalTo("Åkesson"));
        }
    }

    @Nested
    @Order(4)
    @DisplayName("Alias Management Tests")
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class AliasTests {

        @Test
        @Order(1)
        @DisplayName("TC-DATASET1-010: GET aliases initially empty")
        void getAliases_initiallyEmpty() {
            given()
                    .spec(jsonSpec)
            .when()
                    .get(datasetUrl + "/aliases")
            .then()
                    .statusCode(200)
                    .contentType(ContentType.JSON)
                    .body("", hasSize(0));
        }

        @Test
        @Order(2)
        @DisplayName("TC-DATASET1-011: PUT aliases")
        void putAliases() {
            given()
                    .contentType(ContentType.JSON)
                    .body("[\"dataset1\"]")
            .when()
                    .put(datasetUrl + "/aliases")
            .then()
                    .statusCode(204);
        }

        @Test
        @Order(3)
        @DisplayName("TC-DATASET1-012: Verify alias was set")
        void verifyAliasSet() {
            Response response = given()
                    .spec(jsonSpec)
            .when()
                    .get(datasetUrl + "/aliases")
            .then()
                    .statusCode(200)
                    .contentType(ContentType.JSON)
                    .body("", hasSize(1))
                    .extract()
                    .response();

            // Verify the actual alias content
            List<String> aliases = response.jsonPath().getList("");
            assertThat(aliases).containsExactly("dataset1");
        }

        @Test
        @Order(4)
        @DisplayName("TC-DATASET1-013: Access dataset info via alias")
        void accessViaAlias() {
            given()
                    .spec(jsonSpec)
            .when()
                    .get("/dataset/dataset1/info")
            .then()
                    .statusCode(200)
                    .contentType(ContentType.JSON)
                    .body("rowcount", equalTo(5))
                    .body("status", equalTo(ETL_STATUS_AVAILABLE));
        }

        @Test
        @Order(5)
        @DisplayName("TC-DATASET1-014: POST adds alias (duplicate handling)")
        void postAliases_duplicateHandling() {
            // Intentionally providing the same alias twice
            given()
                    .contentType(ContentType.JSON)
                    .body("[\"dataset1b\", \"dataset1b\"]")
            .when()
                    .post(datasetUrl + "/aliases")
            .then()
                    .statusCode(204);
        }

        @Test
        @Order(6)
        @DisplayName("TC-DATASET1-015: Verify aliases after POST")
        void verifyAliasesAfterPost() {
            given()
                    .spec(jsonSpec)
            .when()
                    .get(datasetUrl + "/aliases")
            .then()
                    .statusCode(200)
                    .body("", hasSize(2));
        }

        @Test
        @Order(7)
        @DisplayName("TC-DATASET1-016: DELETE all aliases")
        void deleteAllAliases() {
            given()
            .when()
                    .delete(datasetUrl + "/aliases")
            .then()
                    .statusCode(204);
        }

        @Test
        @Order(8)
        @DisplayName("TC-DATASET1-017: Verify aliases after DELETE")
        void verifyAliasesAfterDelete() {
            given()
                    .spec(jsonSpec)
            .when()
                    .get(datasetUrl + "/aliases")
            .then()
                    .statusCode(200)
                    .body("", hasSize(0));
        }
    }

    @Nested
    @Order(5)
    @DisplayName("Alias Uniqueness Tests")
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class AliasUniquenessTests {

        private String dataset1bUrl;

        @Test
        @Order(1)
        @DisplayName("TC-DATASET1-018: Setup - set alias 'theone' on first dataset")
        void setupAlias() {
            given()
                    .contentType(ContentType.JSON)
                    .body("[\"theone\"]")
            .when()
                    .put(datasetUrl + "/aliases")
            .then()
                    .statusCode(204);
        }

        @Test
        @Order(2)
        @DisplayName("TC-DATASET1-019: Create second dataset for uniqueness test")
        void createSecondDataset() {
            byte[] csvData = loadTestData(TEST_FILE);
            Response response = createDataset(csvData);

            response.then().statusCode(202);
            dataset1bUrl = getDatasetUrl(response);

            waitForDatasetAvailable(response.jsonPath().getString("info"));
        }

        @Test
        @Order(3)
        @DisplayName("TC-DATASET1-020: Attempt duplicate alias returns 400")
        void duplicateAlias_returns400() {
            given()
                    .contentType(ContentType.JSON)
                    .body("[\"theone\"]")
            .when()
                    .put(dataset1bUrl + "/aliases")
            .then()
                    .statusCode(400);
        }

        @Test
        @Order(4)
        @DisplayName("TC-DATASET1-021: Cleanup - delete second dataset")
        void cleanupSecondDataset() {
            if (dataset1bUrl != null) {
                given()
                .when()
                        .delete(dataset1bUrl)
                .then()
                        .statusCode(204);
            }
        }
    }

}
