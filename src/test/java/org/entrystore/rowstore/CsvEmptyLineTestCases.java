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
import io.restassured.specification.RequestSpecification;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test cases for empty lines in CSV files, shared by the tests of the default and the legacy parser, which must
 * behave the same. Specification: 04-csv-formats.md
 */
final class CsvEmptyLineTestCases {

    private final RequestSpecification csvSpec;

    private final RequestSpecification jsonSpec;

    CsvEmptyLineTestCases(RequestSpecification csvSpec, RequestSpecification jsonSpec) {
        this.csvSpec = csvSpec;
        this.jsonSpec = jsonSpec;
    }

    /**
     * An empty line between data rows fails the load; a failed replace keeps the previous data.
     */
    void innerEmptyLine_failsAndKeepsPreviousData() {
        TestUtils.DatasetUrls urls = TestUtils.createDatasetAndWait(bytes("name,value\nA1,1\nA2,2\n"), csvSpec, jsonSpec);
        try {
            given().spec(csvSpec).body(bytes("name,value\nB1,1\nB2,2\n\nB3,3\nB4,4\n"))
                    .put(urls.datasetUrl).then().statusCode(202);

            TestUtils.waitForDatasetError(urls.infoUrl, jsonSpec);
            assertThat(rows(urls.datasetUrl)).containsExactly(
                    Map.of("name", "A1", "value", "1"), Map.of("name", "A2", "value", "2"));
        } finally {
            TestUtils.deleteDataset(urls.datasetUrl);
        }
    }

    void emptyLineAfterHeader_fails() {
        Response response = TestUtils.createDataset(bytes("name,value\n\nA1,1\nA2,2\n"), csvSpec);
        response.then().statusCode(202);
        String datasetUrl = TestUtils.getDatasetUrl(response);
        try {
            TestUtils.waitForDatasetError(TestUtils.getInfoUrl(response), jsonSpec);
        } finally {
            TestUtils.deleteDataset(datasetUrl);
        }
    }

    void trailingEmptyLines_areIgnored() {
        TestUtils.DatasetUrls urls = TestUtils.createDatasetAndWait(bytes("name,value\nA1,1\nA2,2\n\n\n"), csvSpec, jsonSpec);
        try {
            assertThat(rows(urls.datasetUrl)).containsExactly(
                    Map.of("name", "A1", "value", "1"), Map.of("name", "A2", "value", "2"));
        } finally {
            TestUtils.deleteDataset(urls.datasetUrl);
        }
    }

    void emptyLineInQuotedValue_isPartOfValue() {
        TestUtils.DatasetUrls urls = TestUtils.createDatasetAndWait(
                bytes("name,value\n\"first\n\nthird\",1\nA2,2\n"), csvSpec, jsonSpec);
        try {
            assertThat(rows(urls.datasetUrl)).containsExactly(
                    Map.of("name", "first\n\nthird", "value", "1"), Map.of("name", "A2", "value", "2"));
        } finally {
            TestUtils.deleteDataset(urls.datasetUrl);
        }
    }

    void whitespaceOnlyLine_isDataRow() {
        TestUtils.DatasetUrls urls = TestUtils.createDatasetAndWait(bytes("name,value\nA1,1\n \nA2,2\n"), csvSpec, jsonSpec);
        try {
            assertThat(rows(urls.datasetUrl)).containsExactly(
                    Map.of("name", "A1", "value", "1"), Map.of("name", " "), Map.of("name", "A2", "value", "2"));
        } finally {
            TestUtils.deleteDataset(urls.datasetUrl);
        }
    }

    /**
     * Unlike an empty line, a quoted empty value of a single-column CSV is a data row, although the legacy parser
     * returns the same record for both.
     */
    void quotedEmptyValueInSingleColumn_isDataRow() {
        TestUtils.DatasetUrls urls = TestUtils.createDatasetAndWait(bytes("name\nA1\n\"\"\nA2\n"), csvSpec, jsonSpec);
        try {
            assertThat(rows(urls.datasetUrl)).containsExactly(
                    Map.of("name", "A1"), Map.of("name", ""), Map.of("name", "A2"));
        } finally {
            TestUtils.deleteDataset(urls.datasetUrl);
        }
    }

    private List<Map<String, Object>> rows(String datasetUrl) {
        return given().spec(jsonSpec)
                .queryParam("_limit", 100)
                .get(datasetUrl)
                .then().statusCode(200)
                .extract().jsonPath().getList("results");
    }

    private static byte[] bytes(String csv) {
        return csv.getBytes(StandardCharsets.UTF_8);
    }

}
