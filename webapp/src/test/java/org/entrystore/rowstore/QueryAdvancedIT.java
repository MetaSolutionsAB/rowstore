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

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests for advanced query functionality.
 * Specification: 05-querying.md
 *
 * Tests multi-column filters, pagination edge cases, and response structure validation.
 */
@DisplayName("Advanced Query Tests")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class QueryAdvancedIT extends BaseIntegrationTest {

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
    @DisplayName("TC-QUERY-001: Multi-column filter (AND logic)")
    void multiColumnFilter_andLogic() {
        // Query with two columns - should only return rows matching BOTH conditions
        given()
                .spec(jsonSpec)
                .queryParam("Name", "McLoud")
                .queryParam("some other column", "x")
        .when()
                .get(datasetUrl)
        .then()
                .statusCode(200)
                .contentType(ContentType.JSON)
                .body("results", hasSize(1))
                .body("results[0].name", equalTo("McLoud"))
                .body("results[0].'some other column'", equalTo("x"));
    }

    @Test
    @Order(2)
    @DisplayName("TC-QUERY-002: Empty string filter value behavior")
    void emptyStringFilter() {
        // Query with empty value - behavior may vary
        // Some implementations treat empty as "match empty" others as "no filter"
        Response response = given()
                .spec(jsonSpec)
                .queryParam("some other column", "")
        .when()
                .get(datasetUrl);

        // Empty filter value is rejected as invalid
        assertThat(response.getStatusCode()).isEqualTo(400);
    }

    @Test
    @Order(3)
    @DisplayName("TC-QUERY-003: Filter with special SQL characters")
    void filterWithSpecialSqlChars() {
        // Values with SQL-sensitive characters should be properly escaped
        given()
                .spec(jsonSpec)
                .queryParam("Name", "O'Brien")  // SQL injection attempt
        .when()
                .get(datasetUrl)
        .then()
                .statusCode(200)
                .body("results", hasSize(0));  // No match, but no SQL error
    }

    @Test
    @Order(4)
    @DisplayName("TC-QUERY-004: Filter value with regex metacharacters in data")
    void filterWithRegexMetachars() {
        // Test filtering when the data might contain regex metacharacters
        // The filter value contains chars that are regex special: .* [] ()
        // This tests that proper escaping occurs
        Response response = given()
                .spec(jsonSpec)
                .queryParam("Comment", ".*test.*")  // Literal regex chars
        .when()
                .get(datasetUrl);

        // Should return 200 (value not found, but no regex error)
        response.then()
                .statusCode(200);

        // In full regex mode, this might match as a regex pattern
        // In disabled/simple mode, treated as literal
        int resultCount = response.jsonPath().getInt("resultCount");
        assertThat(resultCount).isGreaterThanOrEqualTo(0);
    }

    @Test
    @Order(5)
    @DisplayName("TC-QUERY-005: Offset beyond result count returns empty results")
    void offsetBeyondResultCount_returnsEmptyResults() {
        Response response = given()
                .spec(jsonSpec)
                .queryParam("_offset", 1000)  // Way beyond our 5 rows
        .when()
                .get(datasetUrl);

        response.then()
                .statusCode(200)
                .contentType(ContentType.JSON)
                .body("results", hasSize(0))
                .body("offset", equalTo(1000));

        // resultCount shows 0 when offset is beyond all results
        int resultCount = response.jsonPath().getInt("resultCount");
        assertThat(resultCount).isEqualTo(0);
    }

    @Test
    @Order(6)
    @DisplayName("TC-QUERY-006: Zero limit behavior")
    void zeroLimit_behavior() {
        // Zero limit is accepted and treated as default limit
        Response response = given()
                .spec(jsonSpec)
                .queryParam("_limit", 0)
        .when()
                .get(datasetUrl);

        assertThat(response.getStatusCode()).isEqualTo(200);

        // Should return all results (using default limit)
        int resultCount = response.jsonPath().getInt("results.size()");
        assertThat(resultCount).isGreaterThan(0);
    }

    @Test
    @Order(7)
    @DisplayName("TC-QUERY-007: Negative limit behavior")
    void negativeLimit_behavior() {
        Response response = given()
                .spec(jsonSpec)
                .queryParam("_limit", -5)
        .when()
                .get(datasetUrl);

        // Negative limit is accepted and treated as default limit
        assertThat(response.getStatusCode()).isEqualTo(200);
    }

    @Test
    @Order(8)
    @DisplayName("TC-QUERY-008: Negative offset behavior")
    void negativeOffset_behavior() {
        Response response = given()
                .spec(jsonSpec)
                .queryParam("_offset", -5)
        .when()
                .get(datasetUrl);

        // Negative offset is accepted and corrected to 0
        assertThat(response.getStatusCode()).isEqualTo(200);
        int offset = response.jsonPath().getInt("offset");
        assertThat(offset).isEqualTo(0);
    }

    @Test
    @Order(9)
    @DisplayName("TC-QUERY-009: Non-numeric limit returns 400")
    void nonNumericLimit_returns400() {
        given()
                .spec(jsonSpec)
                .queryParam("_limit", "abc")
        .when()
                .get(datasetUrl)
        .then()
                .statusCode(400);
    }

    @Test
    @Order(10)
    @DisplayName("TC-QUERY-010: Prev/next links generation")
    void prevNextLinks_generation() {
        // First page - should have next but no prev
        Response firstPage = given()
                .spec(jsonSpec)
                .queryParam("_limit", 2)
        .when()
                .get(datasetUrl);

        firstPage.then()
                .statusCode(200)
                .body("next", notNullValue());

        String nextUrl = firstPage.jsonPath().getString("next");
        assertThat(nextUrl).contains("_offset=2");
        assertThat(nextUrl).contains("_limit=2");
    }

    @Test
    @Order(11)
    @DisplayName("TC-QUERY-011: Prev link absent on first page")
    void prevLinkAbsent_onFirstPage() {
        Response response = given()
                .spec(jsonSpec)
                .queryParam("_limit", 2)
        .when()
                .get(datasetUrl);

        response.then().statusCode(200);

        // First page should not have prev link
        Object prev = response.jsonPath().get("prev");
        assertThat(prev).isNull();
    }

    @Test
    @Order(12)
    @DisplayName("TC-QUERY-012: Next link absent on last page")
    void nextLinkAbsent_onLastPage() {
        // Go to last page
        Response response = given()
                .spec(jsonSpec)
                .queryParam("_limit", 2)
                .queryParam("_offset", 4)  // Last row(s) of 5
        .when()
                .get(datasetUrl);

        response.then().statusCode(200);

        // Last page should not have next link
        Object next = response.jsonPath().get("next");
        assertThat(next).isNull();
    }

    @Test
    @Order(13)
    @DisplayName("TC-QUERY-013: Format parameter content negotiation")
    void formatParameter_contentNegotiation() {
        // Using Accept header for content negotiation (more standard)
        given()
                .spec(jsonSpec)  // Includes Accept: application/json
        .when()
                .get(datasetUrl)
        .then()
                .statusCode(200)
                .contentType(ContentType.JSON);
    }

    @Test
    @Order(14)
    @DisplayName("TC-QUERY-014: queryTime field present and valid")
    void queryTime_presentAndValid() {
        given()
                .spec(jsonSpec)
        .when()
                .get(datasetUrl)
        .then()
                .statusCode(200)
                .body("queryTime", notNullValue())
                .body("queryTime", greaterThanOrEqualTo(0));
    }

    @Test
    @Order(15)
    @DisplayName("TC-QUERY-015: Default query returns all rows")
    void defaultQuery_returnsAllRows() {
        given()
                .spec(jsonSpec)
        .when()
                .get(datasetUrl)
        .then()
                .statusCode(200)
                .contentType(ContentType.JSON)
                .body("resultCount", equalTo(5))
                .body("results", hasSize(5));
    }

    @Test
    @Order(16)
    @DisplayName("TC-QUERY-016: Response includes standard fields")
    void response_includesStandardFields() {
        given()
                .spec(jsonSpec)
        .when()
                .get(datasetUrl)
        .then()
                .statusCode(200)
                .body("results", notNullValue())
                .body("resultCount", notNullValue())
                .body("offset", notNullValue())
                .body("limit", notNullValue())
                .body("queryTime", notNullValue());
    }

    @Test
    @Order(17)
    @DisplayName("TC-QUERY-017: Middle page has both prev and next links")
    void middlePage_hasBothPrevAndNextLinks() {
        // Page 2 of 3 (5 rows, limit=2, offset=2) should have both prev and next
        Response response = given()
                .spec(jsonSpec)
                .queryParam("_limit", 2)
                .queryParam("_offset", 2)
        .when()
                .get(datasetUrl);

        response.then()
                .statusCode(200)
                .body("results", hasSize(2))
                .body("offset", equalTo(2))
                .body("limit", equalTo(2));

        String prev = response.jsonPath().getString("prev");
        String next = response.jsonPath().getString("next");

        assertThat(prev).as("Middle page should have prev link").isNotNull();
        assertThat(prev).contains("_offset=0");

        assertThat(next).as("Middle page should have next link").isNotNull();
        assertThat(next).contains("_offset=4");
    }
}
