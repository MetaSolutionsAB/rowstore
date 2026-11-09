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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

/**
 * Test cases for loads whose CSV yields no columns (ETL-4.10), shared by the tests of the default and the legacy
 * parser, which must behave the same. Specification: 04-csv-formats.md
 */
final class CsvNoColumnsTestCases {

    private static final byte[] CSV = bytes("name,value\nA1,1\nA2,2\n");

    private final RequestSpecification csvSpec;

    private final RequestSpecification jsonSpec;

    CsvNoColumnsTestCases(RequestSpecification csvSpec, RequestSpecification jsonSpec) {
        this.csvSpec = csvSpec;
        this.jsonSpec = jsonSpec;
    }

    /**
     * A body without a header line (an empty line only) fails the replace, which keeps the previous data.
     */
    void replaceWithoutHeaderLine_failsAndKeepsPreviousData() {
        replace_failsAndKeepsPreviousData(bytes("\n"));
    }

    /**
     * A compressed body sent without Content-Encoding is not decoded and yields no columns, which fails the replace;
     * before ROWS-180, the gzip-compressed dataset1_utf8.csv emptied the dataset (AVAILABLE with 0 rows, 0 columns).
     */
    void replaceWithUndecodedGzip_failsAndKeepsPreviousData() {
        replace_failsAndKeepsPreviousData(gzippedDataset1());
    }

    void appendWithoutColumns_failsAndAddsNothing() {
        TestUtils.DatasetUrls urls = TestUtils.createDatasetAndWait(CSV, csvSpec, jsonSpec);
        try {
            for (byte[] body : List.of(bytes("\n"), gzippedDataset1())) {
                given().spec(csvSpec).body(body).post(urls.datasetUrl).then().statusCode(202);
                TestUtils.waitForDatasetError(urls.infoUrl, jsonSpec);
                assertPreviousData(urls);
            }
        } finally {
            TestUtils.deleteDataset(urls.datasetUrl);
        }
    }

    /**
     * A new dataset whose header has no label (empty labels only, whitespace only, or empty lines only) fails.
     */
    void headerWithoutLabel_fails() {
        for (String csv : List.of(",,,\n", ",,\n1,2,3\n", " \n", "\n\n")) {
            Response response = TestUtils.createDataset(bytes(csv), csvSpec);
            response.then().statusCode(202);
            try {
                TestUtils.waitForDatasetError(TestUtils.getInfoUrl(response), jsonSpec);
                given().spec(jsonSpec).get(TestUtils.getInfoUrl(response)).then()
                        .body("rowcount", equalTo(0))
                        .body("columnnames.size()", equalTo(0));
            } finally {
                TestUtils.deleteDataset(TestUtils.getDatasetUrl(response));
            }
        }
    }

    /**
     * A CSV with a header of column names and no rows is a valid, empty dataset.
     */
    void headerOnly_isAvailableWithColumns() {
        TestUtils.DatasetUrls urls = TestUtils.createDatasetAndWait(CSV, csvSpec, jsonSpec);
        try {
            given().spec(csvSpec).body(bytes("name,value\n")).put(urls.datasetUrl).then().statusCode(202);
            TestUtils.waitForDatasetAvailable(urls.infoUrl, jsonSpec);
            given().spec(jsonSpec).get(urls.infoUrl).then()
                    .body("rowcount", equalTo(0));
            assertThat(rows(urls.datasetUrl)).isEmpty();

            given().spec(csvSpec).body(bytes("name,value\nB1,1\n")).post(urls.datasetUrl).then().statusCode(202);
            TestUtils.waitForDatasetAvailable(urls.infoUrl, jsonSpec);
            given().spec(jsonSpec).get(urls.infoUrl).then()
                    .body("rowcount", equalTo(1))
                    .body("columnnames", containsInAnyOrder("name", "value"));
        } finally {
            TestUtils.deleteDataset(urls.datasetUrl);
        }
    }

    private void replace_failsAndKeepsPreviousData(byte[] body) {
        TestUtils.DatasetUrls urls = TestUtils.createDatasetAndWait(CSV, csvSpec, jsonSpec);
        try {
            given().spec(csvSpec).body(body).put(urls.datasetUrl).then().statusCode(202);
            TestUtils.waitForDatasetError(urls.infoUrl, jsonSpec);
            assertPreviousData(urls);
        } finally {
            TestUtils.deleteDataset(urls.datasetUrl);
        }
    }

    private void assertPreviousData(TestUtils.DatasetUrls urls) {
        given().spec(jsonSpec).get(urls.infoUrl).then()
                .body("rowcount", equalTo(2))
                .body("columnnames", containsInAnyOrder("name", "value"));
        assertThat(rows(urls.datasetUrl)).containsExactly(
                Map.of("name", "A1", "value", "1"), Map.of("name", "A2", "value", "2"));
    }

    private List<Map<String, Object>> rows(String datasetUrl) {
        return given().spec(jsonSpec)
                .queryParam("_limit", 100)
                .get(datasetUrl)
                .then().statusCode(200)
                .extract().jsonPath().getList("results");
    }

    private static byte[] gzippedDataset1() {
        try {
            return ContentCodings.encode(TestUtils.loadTestData("dataset1_utf8.csv", CsvNoColumnsTestCases.class), "gzip");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static byte[] bytes(String csv) {
        return csv.getBytes(StandardCharsets.UTF_8);
    }

}
