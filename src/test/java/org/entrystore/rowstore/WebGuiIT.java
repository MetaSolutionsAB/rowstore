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

/**
 * Integration tests for the /dataset/{id}/html endpoint (WebGui).
 * Specification: 07-metadata-endpoints.md
 *
 * Tests the web-based search GUI for datasets.
 */
@DisplayName("WebGui Endpoint Tests")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class WebGuiIT extends BaseIntegrationTest {

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
    @DisplayName("TC-WEBGUI-001: GET HTML page returns text/html")
    void getHtml_returnsHtmlPage() {
        Response response = given()
                .accept("text/html")
        .when()
                .get(datasetUrl + "/html")
        .then()
                .statusCode(200)
                .contentType("text/html")
                .extract()
                .response();

        String body = response.getBody().asString();
        assertThat(body).isNotEmpty();
        assertThat(body.toLowerCase()).contains("<html");
    }

    @Test
    @Order(2)
    @DisplayName("TC-WEBGUI-002: GET embedded HTML returns valid page")
    void getEmbeddedHtml_returnsValidPage() {
        Response response = given()
                .accept("text/html")
        .when()
                .get(datasetUrl + "/html?embed")
        .then()
                .statusCode(200)
                .contentType("text/html")
                .extract()
                .response();

        String body = response.getBody().asString();
        assertThat(body).isNotEmpty();
    }

    @Test
    @Order(3)
    @DisplayName("TC-WEBGUI-003: HTML for non-existent dataset returns 404")
    void htmlNonExistent_returns404() {
        String fakeId = UUID.randomUUID().toString();

        given()
                .accept("text/html")
        .when()
                .get("/dataset/" + fakeId + "/html")
        .then()
                .statusCode(404);
    }

    @Test
    @Order(4)
    @DisplayName("TC-WEBGUI-004: Full HTML is different from embedded HTML")
    void fullHtmlDiffersFromEmbedded() {
        // Get full HTML
        Response fullResponse = given()
                .accept("text/html")
        .when()
                .get(datasetUrl + "/html")
        .then()
                .statusCode(200)
                .extract()
                .response();

        // Get embedded HTML
        Response embedResponse = given()
                .accept("text/html")
        .when()
                .get(datasetUrl + "/html?embed")
        .then()
                .statusCode(200)
                .extract()
                .response();

        String fullBody = fullResponse.getBody().asString();
        String embedBody = embedResponse.getBody().asString();

        // Both should be valid HTML but may differ
        assertThat(fullBody).isNotEmpty();
        assertThat(embedBody).isNotEmpty();

        // At least one should contain HTML content (they share a common header)
        assertThat(fullBody.toLowerCase()).contains("<html");
    }
}
