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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Integration tests for simple regex mode.
 * Specification: 05-querying.md
 *
 * In simple mode:
 * - Query values are treated as exact matches by default
 * - Patterns starting with ^ are interpreted as regex
 *
 * Note: The tilde (~) prefix is ONLY supported in FULL mode, not in SIMPLE mode.
 * This is by design - simple mode uses caret (^) as the regex trigger.
 *
 * Note: These tests require a separate server instance with regexpqueries=simple.
 */
@DisplayName("Regex Simple Mode Tests")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@ExtendWith(RegexSimpleExtension.class)
@Tag("regex-simple")
class RegexSimpleIT extends ConfigurableTestBase {

    private static final String TEST_FILE = "dataset1_utf8.csv";

    private String datasetUrl;

    @Override
    protected String getExtensionBaseUrl() {
        return RegexSimpleExtension.getBaseUrl();
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
    @DisplayName("TC-REGEX-SIM-001: Pattern without ^ treated as exact match")
    void patternWithoutCaret_exactMatch() {
        // Without ^, pattern should be treated as exact match
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
    @DisplayName("TC-REGEX-SIM-002: Pattern with ^ triggers regex")
    void patternWithCaret_triggersRegex() {
        // With ^, pattern should be interpreted as regex
        given()
                .spec(jsonSpec)
                .queryParam("Name", "^Åke.*")  // Should match "Åkesson" as regex
        .when()
                .get(datasetUrl)
        .then()
                .statusCode(200)
                .body("results", hasSize(1))
                .body("results[0].name", equalTo("Åkesson"));
    }

    @Test
    @Order(3)
    @DisplayName("TC-REGEX-SIM-003: Tilde prefix is NOT supported in simple mode")
    void tildePrefix_notSupportedInSimpleMode() {
        // In simple mode, tilde (~) is NOT a regex trigger - only caret (^) is
        // This test verifies that tilde is treated as a literal character
        Response response = given()
                .spec(jsonSpec)
                .queryParam("Name", "~Åke")  // Should match literal "~Åke" not regex
        .when()
                .get(datasetUrl);

        response.then().statusCode(200);

        // Since no row has "~Åke" as an exact value, we expect 0 results
        int resultSize = response.jsonPath().getList("results").size();
        assertThat(resultSize)
                .as("Tilde should NOT trigger regex in simple mode")
                .isEqualTo(0);
    }

    @Test
    @Order(5)
    @DisplayName("TC-REGEX-SIM-005: Alternation pattern with caret works")
    void caretWithAlternation_works() {
        // Caret with alternation should work in simple mode
        given()
                .spec(jsonSpec)
                .queryParam("Name", "^(Åkesson|Béringer)")
        .when()
                .get(datasetUrl)
        .then()
                .statusCode(200)
                .body("results.size()", greaterThanOrEqualTo(1));
    }

    @Test
    @Order(4)
    @DisplayName("TC-REGEX-SIM-004: Plain pattern does not match partial")
    void plainPattern_noPartialMatch() {
        // Without regex prefix, partial matches should not work
        given()
                .spec(jsonSpec)
                .queryParam("Name", "Åke")  // Should NOT match "Åkesson"
        .when()
                .get(datasetUrl)
        .then()
                .statusCode(200)
                .body("results", hasSize(0));  // Exact match only
    }

    @Test
    @Order(6)
    @DisplayName("TC-REGEX-SIM-006: Swagger shows regex enabled in simple mode")
    void swagger_showsRegexEnabled() {
        // In simple mode, Swagger should indicate regex support is available
        Response response = given()
                .spec(jsonSpec)
        .when()
                .get(datasetUrl + "/swagger");

        response.then().statusCode(200);

        String body = response.getBody().asString();
        // Should contain "Regular expressions may be used" since regex is enabled (in simple mode)
        assertThat(body).contains("Regular expressions may be used");
    }
}
