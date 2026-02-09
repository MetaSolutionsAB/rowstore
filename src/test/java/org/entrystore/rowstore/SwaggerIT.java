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

import java.util.UUID;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests for the /dataset/{id}/swagger endpoint.
 * Specification: 07-metadata-endpoints.md
 *
 * Tests Swagger/OpenAPI specification generation for datasets.
 */
@DisplayName("Swagger Endpoint Tests")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SwaggerIT extends BaseIntegrationTest {

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
    @DisplayName("TC-SWAGGER-001: GET Swagger spec returns valid JSON")
    void getSwagger_returnsValidJson() {
        given()
                .spec(jsonSpec)
        .when()
                .get(datasetUrl + "/swagger")
        .then()
                .statusCode(200)
                .contentType(ContentType.JSON)
                .body("swagger", notNullValue())
                .body("info", notNullValue())
                .body("paths", notNullValue());
    }

    @Test
    @Order(2)
    @DisplayName("TC-SWAGGER-002: Swagger contains dataset columns as parameters")
    void swagger_containsDatasetColumns() {
        Response response = given()
                .spec(jsonSpec)
        .when()
                .get(datasetUrl + "/swagger")
        .then()
                .statusCode(200)
                .extract()
                .response();

        String body = response.getBody().asString();

        // The Swagger spec should contain the column names from our dataset
        assertThat(body).contains("\"name\"");
        assertThat(body).contains("\"telephone\"");
        assertThat(body).contains("\"comment\"");
    }

    @Test
    @Order(3)
    @DisplayName("TC-SWAGGER-003: Swagger for non-existent dataset returns 404")
    void swaggerNonExistent_returns404() {
        String fakeId = UUID.randomUUID().toString();

        given()
                .spec(jsonSpec)
        .when()
                .get("/dataset/" + fakeId + "/swagger")
        .then()
                .statusCode(404);
    }

    @Test
    @Order(4)
    @DisplayName("TC-SWAGGER-004: Swagger contains standard parameters")
    void swagger_containsStandardParams() {
        Response response = given()
                .spec(jsonSpec)
        .when()
                .get(datasetUrl + "/swagger")
        .then()
                .statusCode(200)
                .extract()
                .response();

        String body = response.getBody().asString();

        // Standard parameters should be documented
        assertThat(body).contains("_limit");
        assertThat(body).contains("_offset");
    }

    @Test
    @Order(5)
    @DisplayName("TC-SWAGGER-005: Swagger contains dataset ID")
    void swagger_containsDatasetId() {
        Response response = given()
                .spec(jsonSpec)
        .when()
                .get(datasetUrl + "/swagger")
        .then()
                .statusCode(200)
                .body(containsString(datasetId))
                .extract()
                .response();
    }

    @Test
    @Order(6)
    @DisplayName("TC-SWAGGER-006: Swagger info section contains version")
    void swagger_infoContainsVersion() {
        given()
                .spec(jsonSpec)
        .when()
                .get(datasetUrl + "/swagger")
        .then()
                .statusCode(200)
                .body("info.version", notNullValue());
    }
}
