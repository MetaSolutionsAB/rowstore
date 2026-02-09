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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Integration tests for regex queries disabled mode.
 * Specification: 05-querying.md
 *
 * Uses a separate test environment with regexpqueries=disabled.
 *
 * Note: These tests require a separate server instance with different configuration.
 * They are tagged so they can be run separately when a matching server is available.
 */
@DisplayName("Regex Disabled Mode Tests")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@ExtendWith(RegexDisabledExtension.class)
@Tag("regex-disabled")
class RegexDisabledIT extends ConfigurableTestBase {

    private static final String TEST_FILE = "dataset1_utf8.csv";

    private String datasetUrl;

    @Override
    protected String getExtensionBaseUrl() {
        return RegexDisabledExtension.getBaseUrl();
    }

    @BeforeAll
    void createDataset() {
        byte[] csvData = loadTestData(TEST_FILE);
        DatasetUrls urls = createDatasetAndWait(csvData);
        datasetUrl = urls.datasetUrl;
    }

    @AfterAll
    void cleanup() {
        if (datasetUrl != null) {
            given().delete(datasetUrl);
        }
    }

    @Test
    @Order(1)
    @DisplayName("TC-REGEX-DIS-001: Exact match works in disabled mode")
    void exactMatch_works() {
        given()
                .spec(jsonSpec)
                .queryParam("Name", "Åkesson")
        .when()
                .get(datasetUrl)
        .then()
                .statusCode(200)
                .body("results", hasSize(1))
                .body("results[0].name", equalTo("Åkesson"));
    }

    @Test
    @Order(2)
    @DisplayName("TC-REGEX-DIS-002: Regex pattern treated as literal")
    void regexPattern_treatedAsLiteral() {
        // In disabled mode, ".*" should be treated as literal characters
        given()
                .spec(jsonSpec)
                .queryParam("Name", ".*")  // This should match literal ".*", not as regex
        .when()
                .get(datasetUrl)
        .then()
                .statusCode(200)
                .body("results", hasSize(0));  // No row has literal ".*" as name
    }

    @Test
    @Order(3)
    @DisplayName("TC-REGEX-DIS-003: Tilde prefix treated literally")
    void tildePrefix_treatedLiterally() {
        // In disabled mode, "~pattern" should be treated as literal "~pattern"
        given()
                .spec(jsonSpec)
                .queryParam("Name", "~Åkesson")  // Should match literal "~Åkesson"
        .when()
                .get(datasetUrl)
        .then()
                .statusCode(200)
                .body("results", hasSize(0));  // No row has literal "~Åkesson"
    }

    @Test
    @Order(4)
    @DisplayName("TC-REGEX-DIS-004: Swagger shows regex disabled mode")
    void swagger_showsRegexDisabled() {
        // In disabled mode, Swagger should indicate "Exact matching" instead of "Regular expressions"
        Response response = given()
                .spec(jsonSpec)
        .when()
                .get(datasetUrl + "/swagger");

        response.then().statusCode(200);

        String body = response.getBody().asString();
        // Should contain "Exact matching" and NOT "Regular expressions"
        assertThat(body).contains("Exact matching");
        assertThat(body).doesNotContain("Regular expressions may be used");
    }

    @Test
    @Order(5)
    @DisplayName("TC-REGEX-DIS-005: Partial match does not work")
    void partialMatch_doesNotWork() {
        // In disabled mode, partial match patterns should not work
        given()
                .spec(jsonSpec)
                .queryParam("Name", "Åk")  // Should NOT match "Åkesson" (no regex)
        .when()
                .get(datasetUrl)
        .then()
                .statusCode(200)
                .body("results", hasSize(0));  // Exact match only
    }
}
