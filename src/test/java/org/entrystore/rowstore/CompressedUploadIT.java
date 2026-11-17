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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests for request bodies with a Content-Encoding, which are decoded as by 1.x (ROWS-173).
 * Specification: API-3.17, test specification specs/tests/14-http-compatibility.md
 */
@DisplayName("Compressed Upload Tests")
class CompressedUploadIT extends BaseIntegrationTest {

    private static final byte[] CSV = "name,value\nA1,1\nA2,2\n".getBytes(StandardCharsets.UTF_8);

    private static final byte[] OTHER_CSV = "name,value\nB1,3\nB2,4\nB3,5\n".getBytes(StandardCharsets.UTF_8);

    @Test
    @DisplayName("TC-ENCODING-001: A gzip-compressed upload creates the dataset from the decoded CSV")
    void gzipUpload_createsDataset() throws IOException {
        byte[] csvData = loadTestData("dataset1_utf8.csv");
        Response response = upload("POST", "/datasets", "gzip", ContentCodings.encode(csvData, "gzip"));
        response.then().statusCode(202);
        String datasetUrl = getDatasetUrl(response);
        try {
            waitForDatasetAvailable(getInfoUrl(response));
            given().spec(jsonSpec).get(getInfoUrl(response)).then()
                    .body("rowcount", equalTo(5))
                    .body("columnnames.size()", equalTo(4));
            given().spec(jsonSpec).queryParam("name", "McLoud").get(datasetUrl).then()
                    .body("resultCount", equalTo(1))
                    .body("results[0].telephone", equalTo("0987654321"));
        } finally {
            deleteDataset(datasetUrl);
        }
    }

    @ParameterizedTest(name = "[{index}] Content-Encoding: {0}")
    @ValueSource(strings = {"x-gzip", "deflate", "deflate-no-wrap", "zip", "identity", "gzip, identity", "GZIP"})
    @DisplayName("TC-ENCODING-002: Uploads with the other supported codings are decoded")
    void supportedCodings_areDecoded(String contentEncoding) throws IOException {
        Response response = upload("POST", "/datasets", contentEncoding, ContentCodings.encode(CSV, contentEncoding));
        response.then().statusCode(202);
        String datasetUrl = getDatasetUrl(response);
        try {
            waitForDatasetAvailable(getInfoUrl(response));
            assertThat(rows(datasetUrl)).containsExactly(
                    Map.of("name", "A1", "value", "1"), Map.of("name", "A2", "value", "2"));
        } finally {
            deleteDataset(datasetUrl);
        }
    }

    @Test
    @DisplayName("TC-ENCODING-003: A gzip-compressed replace (PUT) and append (POST) load the decoded CSV")
    void gzipReplaceAndAppend_loadDecodedCsv() throws IOException {
        DatasetUrls urls = createDatasetAndWait(CSV);
        try {
            upload("PUT", urls.datasetUrl, "gzip", ContentCodings.encode(OTHER_CSV, "gzip")).then().statusCode(202);
            waitForDatasetAvailable(urls.infoUrl);
            assertThat(rows(urls.datasetUrl)).extracting(row -> row.get("name")).containsExactly("B1", "B2", "B3");

            upload("POST", urls.datasetUrl, "gzip", ContentCodings.encode(CSV, "gzip")).then().statusCode(202);
            waitForDatasetAvailable(urls.infoUrl);
            assertThat(rows(urls.datasetUrl)).extracting(row -> row.get("name"))
                    .containsExactly("B1", "B2", "B3", "A1", "A2");
        } finally {
            deleteDataset(urls.datasetUrl);
        }
    }

    @Test
    @DisplayName("TC-ENCODING-004: An unsupported coding is rejected with 415 and nothing is stored")
    void unsupportedCoding_isRejectedWith415() throws IOException {
        DatasetUrls urls = createDatasetAndWait(CSV);
        try {
            List<String> before = TestUtils.getDatasetsList(jsonSpec);

            // the body is plain CSV: 1.x ignored the header and loaded it
            given().spec(csvSpec).header("Content-Encoding", "br").header("Origin", "https://app.example.com")
                    .body(OTHER_CSV).post("/datasets")
                    .then().statusCode(415)
                    .header("Access-Control-Allow-Origin", notNullValue())
                    .header("Accept-Encoding", "gzip, x-gzip, deflate, deflate-no-wrap, zip, identity");
            upload("PUT", urls.datasetUrl, "zstd", OTHER_CSV).then().statusCode(415);
            upload("POST", urls.datasetUrl, "gzip, br", OTHER_CSV).then().statusCode(415);
            // more than two compressions
            upload("PUT", urls.datasetUrl, "gzip, gzip, gzip", ContentCodings.encode(OTHER_CSV, "gzip, gzip, gzip"))
                    .then().statusCode(415);

            assertThat(TestUtils.getDatasetsList(jsonSpec)).containsExactlyInAnyOrderElementsOf(before);
            given().spec(jsonSpec).get(urls.infoUrl).then().body("status", equalTo(ETL_STATUS_AVAILABLE));
            assertThat(rows(urls.datasetUrl)).extracting(row -> row.get("name")).containsExactly("A1", "A2");
        } finally {
            deleteDataset(urls.datasetUrl);
        }
    }

    @Test
    @DisplayName("TC-ENCODING-005: Corrupt compressed data is rejected with 400 and nothing is stored")
    void corruptData_isRejectedWith400() throws IOException {
        DatasetUrls urls = createDatasetAndWait(CSV);
        try {
            List<String> before = TestUtils.getDatasetsList(jsonSpec);
            byte[] gzip = ContentCodings.encode(OTHER_CSV, "gzip");
            byte[] truncated = Arrays.copyOf(gzip, gzip.length / 2);

            upload("POST", "/datasets", "gzip", truncated).then().statusCode(400);
            // not gzip at all
            upload("POST", "/datasets", "gzip", OTHER_CSV).then().statusCode(400);
            upload("PUT", urls.datasetUrl, "gzip", truncated).then().statusCode(400);
            upload("POST", urls.datasetUrl, "deflate", truncated).then().statusCode(400);

            assertThat(TestUtils.getDatasetsList(jsonSpec)).containsExactlyInAnyOrderElementsOf(before);
            given().spec(jsonSpec).get(urls.infoUrl).then().body("status", equalTo(ETL_STATUS_AVAILABLE));
            assertThat(rows(urls.datasetUrl)).extracting(row -> row.get("name")).containsExactly("A1", "A2");
        } finally {
            deleteDataset(urls.datasetUrl);
        }
    }

    @Test
    @DisplayName("TC-ENCODING-006: A gzip-compressed alias update is decoded")
    void gzipAliasUpdate_isDecoded() throws IOException {
        DatasetUrls urls = createDatasetAndWait(CSV);
        try {
            byte[] aliases = "[\"compressedalias\"]".getBytes(StandardCharsets.UTF_8);
            given().contentType("application/json").header("Content-Encoding", "gzip")
                    .body(ContentCodings.encode(aliases, "gzip"))
                    .put(urls.datasetUrl + "/aliases")
                    .then().statusCode(204);

            assertThat(given().spec(jsonSpec).get(urls.datasetUrl + "/aliases").then().statusCode(200)
                    .extract().jsonPath().getList("", String.class)).containsExactly("compressedalias");
        } finally {
            deleteDataset(urls.datasetUrl);
        }
    }

    @Test
    @DisplayName("TC-ENCODING-010: An alias body larger than 1 MiB is rejected with 413, also when compressed")
    void largeAliasBody_isRejectedWith413() throws IOException {
        DatasetUrls urls = createDatasetAndWait(CSV);
        try {
            String aliasesUrl = urls.datasetUrl + "/aliases";
            // a JSON array of exactly 1 MiB
            byte[] maxBody = Arrays.copyOf("[\"bigalias\"".getBytes(StandardCharsets.UTF_8), 1024 * 1024);
            Arrays.fill(maxBody, 11, maxBody.length - 1, (byte) ' ');
            maxBody[maxBody.length - 1] = ']';
            given().contentType("application/json").body(maxBody).put(aliasesUrl).then().statusCode(204);

            // 64 MiB after decoding, a few hundred bytes as sent
            byte[] bomb = ContentCodings.encode(new byte[64 * 1024 * 1024], "gzip, gzip");
            given().contentType("application/json").header("Content-Encoding", "gzip, gzip").body(bomb)
                    .put(aliasesUrl).then().statusCode(413);
            byte[] tooLarge = Arrays.copyOf(maxBody, maxBody.length + 1);
            tooLarge[tooLarge.length - 1] = ' ';
            given().contentType("application/json").body(tooLarge).post(aliasesUrl).then().statusCode(413);

            assertThat(given().spec(jsonSpec).get(aliasesUrl).then().statusCode(200)
                    .extract().jsonPath().getList("", String.class)).containsExactly("bigalias");
        } finally {
            deleteDataset(urls.datasetUrl);
        }
    }

    @Test
    @DisplayName("TC-ENCODING-008: An upload that is empty after decoding, or without body and Content-Length, is rejected with 400")
    void emptyBody_isRejectedWith400() throws IOException {
        DatasetUrls urls = createDatasetAndWait(CSV);
        try {
            List<String> before = TestUtils.getDatasetsList(jsonSpec);
            byte[] emptyGzip = ContentCodings.encode(new byte[0], "gzip");
            String path = URI.create(urls.datasetUrl).getPath();

            upload("PUT", urls.datasetUrl, "gzip", emptyGzip).then().statusCode(400);
            upload("POST", urls.datasetUrl, "gzip", emptyGzip).then().statusCode(400);
            upload("POST", "/datasets", "gzip", emptyGzip).then().statusCode(400);
            // HTTP/1.0 without Content-Length: the body is empty, its length unknown to the controller
            for (String method : List.of("PUT", "POST")) {
                assertThat(TestUtils.rawRequest(getBaseUrl(), method, path, List.of("Content-Type: text/csv"), null)
                        .status()).as(method).isEqualTo(400);
                assertThat(TestUtils.rawRequest(getBaseUrl(), method, path,
                        List.of("Content-Type: text/csv", "Content-Encoding: gzip"), null).status()).as(method).isEqualTo(400);
            }

            assertThat(TestUtils.getDatasetsList(jsonSpec)).containsExactlyInAnyOrderElementsOf(before);
            given().spec(jsonSpec).get(urls.infoUrl).then().body("status", equalTo(ETL_STATUS_AVAILABLE));
            assertThat(rows(urls.datasetUrl)).extracting(row -> row.get("name")).containsExactly("A1", "A2");
        } finally {
            deleteDataset(urls.datasetUrl);
        }
    }

    @Test
    @DisplayName("TC-ENCODING-009: The body of a request other than POST and PUT is not decoded or rejected")
    void otherMethods_areNotDecoded() {
        TestUtils.RawResponse response = TestUtils.rawRequest(getBaseUrl(), "GET", "/datasets",
                List.of("Content-Encoding: br"), OTHER_CSV);

        assertThat(response.status()).isEqualTo(200);
    }

    private Response upload(String method, String path, String contentEncoding, byte[] body) {
        return given().spec(csvSpec).header("Content-Encoding", contentEncoding).body(body).request(method, path);
    }

    private List<Map<String, Object>> rows(String datasetUrl) {
        return given().spec(jsonSpec)
                .queryParam("_limit", 100)
                .get(datasetUrl)
                .then().statusCode(200)
                .extract().jsonPath().getList("results");
    }

}
