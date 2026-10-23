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

import org.entrystore.rowstore.TestUtils.RawResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Integration tests for the parsing of query strings as in 1.x: characters sent unencoded, malformed percent-encodings,
 * trimming and regular expressions that 2.0 rejected before.
 * Specification: 05-querying.md (QUERY-2.03, QUERY-4.05, QUERY-9.03, QUERY-9.04)
 *
 * The requests are sent with {@link TestUtils#rawRequest}, since HTTP clients encode or reject these query strings.
 */
@DisplayName("Query String Parsing Tests")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class QueryParsingIT extends BaseIntegrationTest {

    private DatasetUrls dataset;

    private String path;

    @BeforeAll
    void createDataset() {
        dataset = createDatasetAndWait(loadTestData("dataset1_utf8.csv"));
        path = "/dataset/" + dataset.datasetId;
    }

    @AfterAll
    void cleanup() {
        if (dataset != null) {
            deleteDataset(dataset.datasetUrl);
        }
    }

    private RawResponse get(String query) {
        return TestUtils.rawGet(getBaseUrl(), path + "?" + query, "Accept: application/json");
    }

    private static List<String> names(RawResponse response) {
        assertThat(response.status()).as(response.body()).isEqualTo(200);
        return response.json().getList("results.name");
    }

    /**
     * Full regex mode is configured for the shared test server.
     */
    @ParameterizedTest(name = "[{index}] ?{0}")
    @CsvSource(delimiter = '#', value = {
            "Name=^M                          # Name=%5EM                                      # McLoud,Martinsson",
            "Name=^(McLoud|Martinsson)$       # Name=%5E%28McLoud%7CMartinsson%29%24           # McLoud,Martinsson",
            "Name=^[MB]                       # Name=%5E%5BMB%5D                               # Béringer,McLoud,Martinsson",
            "Name=^M.{5}$                     # Name=%5EM.%7B5%7D%24                           # McLoud",
            "Name=^Mc\\w+                     # Name=%5EMc%5Cw%2B                              # McLoud",
            "Name=~^(M|%C3%9C)                # Name=%7E%5E%28M%7C%C3%9C%29                    # McLoud,Martinsson,Überhuber"
    })
    @DisplayName("TC-QUERY-021: Unencoded regex characters in the query string are accepted")
    void unencodedRegexCharacters_areAccepted(String raw, String encoded, String expected) {
        // non-ASCII characters are always percent-encoded: Tomcat rejects them unencoded, and browsers encode them
        List<String> expectedNames = List.of(expected.split(","));
        assertThat(names(get(raw))).containsExactlyInAnyOrderElementsOf(expectedNames);
        assertThat(names(get(encoded))).containsExactlyInAnyOrderElementsOf(expectedNames);
    }

    @ParameterizedTest(name = "[{index}] ?{0}")
    @ValueSource(strings = {"Name=\"x\"", "Name=<x>", "Name=`x`"})
    @DisplayName("TC-QUERY-021: Other characters that clients send unencoded are accepted")
    void otherUnencodedCharacters_areAccepted(String query) {
        assertThat(names(get(query))).isEmpty();
    }

    @ParameterizedTest(name = "[{index}] ?{0}")
    @ValueSource(strings = {
            "Name=100%",
            "Name=%zz",
            "Name=%C",
            "Name=McLoud&Telephone=%zz",
            "Telephone=%zz&Name=McLoud",
            "Na%zzme=McLoud"
    })
    @DisplayName("TC-QUERY-022: A malformed percent-encoding is rejected with 400 instead of dropping the filter")
    void malformedPercentEncoding_isRejected(String query) {
        assertThat(get(query).status()).isEqualTo(400);
        assertThat(TestUtils.rawGet(getBaseUrl(), path + "/json?" + query).status()).isEqualTo(400);
    }

    @ParameterizedTest(name = "[{index}] ?{0}")
    @ValueSource(strings = {
            "Name=McLoud%20",
            "Name=+McLoud",
            "Name=%20McLoud%20",
            "%20Name=McLoud",
            "Name+=McLoud",
            "Name=McLoud&_limit=%201",
            "Name=McLoud&%20=x"
    })
    @DisplayName("TC-QUERY-023: Parameter names and values are trimmed as in 1.x")
    void namesAndValues_areTrimmed(String query) {
        assertThat(names(get(query))).containsExactly("McLoud");
    }

    @Test
    @DisplayName("TC-QUERY-023: Of a repeated parameter, the last value is used as in 1.x")
    void repeatedParameter_lastValueIsUsed() {
        assertThat(names(get("Name=Martinsson&Name=McLoud"))).containsExactly("McLoud");
    }

    @ParameterizedTest(name = "[{index}] ?{0}")
    @CsvSource(delimiter = '#', value = {
            "Name=^(Mc|Ma)(L|r)+              # McLoud,Martinsson",
            "Name=^M[a-z]+(,[0-9]+)*$         # Martinsson",
            "Name=^(M|B)(c|é)*                # Béringer,McLoud,Martinsson",
            "Name=~(a|e)+                     # Béringer,Åkesson,Martinsson,Überhuber"
    })
    @DisplayName("TC-REGEX-FULL-004: Regular expressions with quantified groups are accepted")
    void quantifiedGroups_areAccepted(String query, String expected) {
        given()
                .spec(jsonSpec)
                .queryParam(query.substring(0, query.indexOf('=')), query.substring(query.indexOf('=') + 1))
        .when()
                .get(dataset.datasetUrl)
        .then()
                .statusCode(200)
                .body("results.name", containsInAnyOrder(expected.split(",")));
    }

    @Test
    @DisplayName("TC-REGEX-FULL-005: A value longer than 200 characters that is detected as a regex is accepted")
    void longValueDetectedAsRegex_isAccepted() {
        // detected as a regex because of "/", and as a regex it matches the value itself
        String url = "https://example.org/" + "a".repeat(250) + "/data.csv";
        String csv = "url,n\n" + url + ",1\nhttps://example.org/other,2\n";
        DatasetUrls urls = createDatasetAndWait(csv.getBytes(StandardCharsets.UTF_8));
        try {
            given()
                    .spec(jsonSpec)
                    .queryParam("url", url)
            .when()
                    .get(urls.datasetUrl)
            .then()
                    .statusCode(200)
                    .body("results", hasSize(1))
                    .body("results[0].n", equalTo("1"));
        } finally {
            deleteDataset(urls.datasetUrl);
        }
    }

    @Test
    @DisplayName("TC-REGEX-FULL-006: An invalid regular expression is rejected with 400")
    void invalidRegex_isRejected() {
        given()
                .spec(jsonSpec)
                .queryParam("Name", "^(M")
        .when()
                .get(dataset.datasetUrl)
        .then()
                .statusCode(400);
    }

}
