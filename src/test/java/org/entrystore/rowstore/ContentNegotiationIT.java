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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for content negotiation as in 1.x: requests are never answered with 406.
 * Specification: 03-api.md (API-4.03, API-4.04, API-4.05)
 */
@DisplayName("Content Negotiation Tests")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ContentNegotiationIT extends BaseIntegrationTest {

    private DatasetUrls dataset;

    @BeforeAll
    void createDataset() {
        dataset = createDatasetAndWait(loadTestData("dataset1_utf8.csv"));
    }

    @AfterAll
    void cleanup() {
        if (dataset != null) {
            deleteDataset(dataset.datasetUrl);
        }
    }

    private RawResponse get(String subPath, String accept) {
        String fullPath = subPath.replace("{ds}", dataset.datasetId);
        return accept == null ? TestUtils.rawGet(getBaseUrl(), fullPath) : TestUtils.rawGet(getBaseUrl(), fullPath, "Accept: " + accept);
    }

    @ParameterizedTest(name = "[{index}] {0} with Accept: {1}")
    @CsvSource(delimiter = '|', value = {
            "/dataset/{ds}/info      | application/ld+json",
            "/dataset/{ds}/info      | text/html",
            "/dataset/{ds}/info      | text/plain",
            "/dataset/{ds}/info      | application/xml",
            "/datasets               | text/plain",
            "/datasets               | text/html",
            "/status                 | text/html",
            "/status?jvm             | text/json",
            "/dataset/{ds}/aliases   | application/xml",
            "/dataset/{ds}/swagger   | text/html",
            "/dataset/{ds}/json      | text/html",
            "/dataset/{ds}/json      | text/csv",
            "/dataset/{ds}           | application/ld+json",
            "/dataset/{ds}           | application/xml"
    })
    @DisplayName("TC-CONNEG-001: JSON resources respond with JSON to any Accept header")
    void jsonResources_respondWithJson(String subPath, String accept) {
        RawResponse response = get(subPath, accept);
        assertThat(response.status()).isEqualTo(200);
        assertThat(response.header("Content-Type")).startsWith("application/json");
    }

    @ParameterizedTest(name = "[{index}] Accept: {0}")
    @ValueSource(strings = {"text/plain", "text/csv", "text/json", "text/javascript", "*/*",
            "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"})
    @DisplayName("TC-CONNEG-002: The dataset resource redirects to the HTML view for text types, as in 1.x")
    void datasetResource_textTypes_redirectToHtml(String accept) {
        RawResponse response = get("/dataset/{ds}", accept);
        assertThat(response.status()).isEqualTo(303);
        assertThat(response.header("Location")).isEqualTo(dataset.datasetUrl + "/html");
    }

    @Test
    @DisplayName("TC-CONNEG-002: The dataset resource redirects to the HTML view without Accept header")
    void datasetResource_noAccept_redirectsToHtml() {
        assertThat(get("/dataset/{ds}", null).status()).isEqualTo(303);
    }

    @ParameterizedTest(name = "[{index}] Accept: {0} -> {1}")
    @CsvSource(delimiter = '|', nullValues = "null", value = {
            "null                               | application/json",
            "*/*                                | application/json",
            "text/csv                           | text/csv",
            "text/html                          | text/csv",
            "text/plain                         | text/csv",
            "application/xml                    | application/json",
            "text/csv;q=0.5, application/json   | application/json"
    })
    @DisplayName("TC-CONNEG-003: The export selects JSON or CSV as in 1.x")
    void export_selectsRepresentation(String accept, String expected) {
        RawResponse response = get("/dataset/{ds}/export", accept);
        assertThat(response.status()).isEqualTo(200);
        assertThat(response.header("Content-Type")).startsWith(expected);
        if (expected.equals("text/csv")) {
            assertThat(response.body()).doesNotStartWith("[").contains("McLoud");
        } else {
            assertThat(response.body()).startsWith("[");
        }
    }

    @ParameterizedTest(name = "[{index}] Accept: {0}")
    @CsvSource(delimiter = '|', nullValues = "null", value = {"null", "*/*", "application/json", "text/plain", "text/html"})
    @DisplayName("TC-CONNEG-004: The HTML view is always served as text/html")
    void htmlView_isAlwaysHtml(String accept) {
        RawResponse response = get("/dataset/{ds}/html", accept);
        assertThat(response.status()).isEqualTo(200);
        assertThat(response.header("Content-Type")).isEqualToIgnoringCase("text/html;charset=UTF-8");
        assertThat(response.body()).contains("<html");
    }

    @ParameterizedTest(name = "[{index}] ?{0} with Accept: {1} -> {2}")
    @CsvSource(delimiter = '|', nullValues = "null", value = {
            "media=csv           | null               | text/csv",
            "media=text/csv      | application/json   | text/csv",
            "media=text%2Fcsv    | application/json   | text/csv",
            "media=json          | text/csv           | application/json",
            "media=foo           | text/csv           | text/csv"
    })
    @DisplayName("TC-EXPORT-017: The media parameter selects the export format, as in 1.x")
    void export_mediaParameter(String query, String accept, String expected) {
        RawResponse response = get("/dataset/{ds}/export?" + query, accept);
        assertThat(response.status()).isEqualTo(200);
        assertThat(response.header("Content-Type")).startsWith(expected);
    }

    @Test
    @DisplayName("TC-CONNEG-005: The media parameter selects the representation of the dataset resource")
    void datasetResource_mediaParameter() {
        assertThat(get("/dataset/{ds}?media=html", "application/json").status()).isEqualTo(303);
        RawResponse json = get("/dataset/{ds}?media=json&Name=McLoud", "text/html");
        assertThat(json.status()).isEqualTo(200);
        assertThat(json.json().getList("results.name")).containsExactly("McLoud");
    }

}
