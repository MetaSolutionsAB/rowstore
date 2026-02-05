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
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for JSONP callback functionality.
 * Specification: 05-querying.md
 *
 * Tests the _callback parameter for JSONP responses.
 */
@DisplayName("JSONP Callback Tests")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class JsonpIT extends BaseIntegrationTest {

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
    @DisplayName("TC-JSONP-001: Query with _callback wraps response")
    void queryWithCallback_wrapsResponse() {
        Response response = given()
                .spec(jsonSpec)
                .queryParam("_callback", "myCallback")
        .when()
                .get(datasetUrl)
        .then()
                .statusCode(200)
                .extract()
                .response();

        String body = response.getBody().asString();

        // Response should be wrapped in the callback function
        assertThat(body).startsWith("myCallback(");
        assertThat(body).endsWith(")");

        // Inner content should be valid JSON
        String jsonContent = body.substring("myCallback(".length(), body.length() - 1);
        assertThat(jsonContent).contains("results");
        assertThat(jsonContent).contains("resultCount");
    }

    @Test
    @Order(2)
    @DisplayName("TC-JSONP-002: Callback with valid JS function name")
    void callbackWithValidFunctionName() {
        // Test standard valid JS function name patterns (excluding special chars that may be filtered)
        String[] validNames = {"myFunc", "my_func", "myFunc123"};

        for (String callbackName : validNames) {
            Response response = given()
                    .spec(jsonSpec)
                    .queryParam("_callback", callbackName)
            .when()
                    .get(datasetUrl)
            .then()
                    .statusCode(200)
                    .extract()
                    .response();

            String body = response.getBody().asString();
            assertThat(body)
                    .as("Response should start with callback: " + callbackName)
                    .startsWith(callbackName + "(");
        }
    }

    @Test
    @Order(3)
    @DisplayName("TC-JSONP-003: Empty _callback behavior")
    void emptyCallback_behavior() {
        Response response = given()
                .spec(jsonSpec)
                .queryParam("_callback", "")
        .when()
                .get(datasetUrl);

        // Empty callback may be:
        // - Treated as invalid (400)
        // - Use default "callback" (200 with wrapper)
        // - Return plain JSON (200 without wrapper)
        assertThat(response.getStatusCode()).isIn(200, 400);

        if (response.getStatusCode() == 200) {
            String body = response.getBody().asString();

            // Check if wrapped or plain JSON
            boolean isWrapped = body.startsWith("callback(") || !body.startsWith("{");

            if (isWrapped) {
                assertThat(body).startsWith("callback(");
            } else {
                // Plain JSON response
                assertThat(body).contains("results");
            }
        }
    }

    @Test
    @Order(4)
    @DisplayName("TC-JSONP-004: Callback on info endpoint")
    void callbackOnInfoEndpoint() {
        Response response = given()
                .spec(jsonSpec)
                .queryParam("_callback", "infoCallback")
        .when()
                .get(infoUrl);

        // Info endpoint may or may not support JSONP
        assertThat(response.getStatusCode()).isIn(200, 400);

        if (response.getStatusCode() == 200) {
            String body = response.getBody().asString();

            // Check if wrapped (JSONP supported) or plain JSON
            if (body.startsWith("infoCallback(")) {
                assertThat(body).endsWith(")");
                String jsonContent = body.substring("infoCallback(".length(), body.length() - 1);
                assertThat(jsonContent).contains("rowcount");
            } else {
                // Plain JSON response (JSONP not supported on this endpoint)
                assertThat(body).contains("rowcount");
            }
        }
    }

    @Test
    @Order(5)
    @DisplayName("TC-JSONP-005: Callback combined with query filters")
    void callbackCombinedWithFilters() {
        Response response = given()
                .spec(jsonSpec)
                .queryParam("_callback", "filterCallback")
                .queryParam("Name", "Åkesson")
        .when()
                .get(datasetUrl)
        .then()
                .statusCode(200)
                .extract()
                .response();

        String body = response.getBody().asString();

        // Should be wrapped
        assertThat(body).startsWith("filterCallback(");
        assertThat(body).endsWith(")");

        // Inner JSON should have filtered results
        String jsonContent = body.substring("filterCallback(".length(), body.length() - 1);
        assertThat(jsonContent).contains("Åkesson");
    }
}
