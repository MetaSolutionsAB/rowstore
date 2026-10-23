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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for the content types accepted by uploads and alias updates, as in 1.x.
 * Specification: 03-api.md (API-3.03, API-3.05, API-3.06, API-3.09), 07-security.md (SEC-4.04)
 */
@DisplayName("Request Content-Type Tests")
class ContentTypeIT extends BaseIntegrationTest {

    private static final byte[] CSV = "name,n\na,1\nb,2\n".getBytes(StandardCharsets.UTF_8);

    private RawResponse send(String method, String path, String contentType, byte[] body) {
        List<String> headers = contentType == null ? List.of() : List.of("Content-Type: " + contentType);
        return TestUtils.rawRequest(getBaseUrl(), method, path, headers, body);
    }

    private int rowCount(DatasetUrls urls) {
        return given().spec(jsonSpec).get(urls.infoUrl).jsonPath().getInt("rowcount");
    }

    @Test
    @DisplayName("TC-ERROR-019: Uploads without Content-Type are read as CSV")
    void uploadsWithoutContentType_areAccepted() {
        RawResponse created = send("POST", "/datasets", null, CSV);
        assertThat(created.status()).as(created.body()).isEqualTo(202);
        assertThat(created.header("Location")).isNotNull();
        String id = created.json().getString("id");
        DatasetUrls urls = new DatasetUrls(created.json().getString("url"), created.json().getString("info"), id);
        try {
            waitForDatasetAvailable(urls.infoUrl);
            assertThat(rowCount(urls)).isEqualTo(2);

            assertThat(send("POST", "/dataset/" + id, null, CSV).status()).isEqualTo(202);
            waitForDatasetAvailable(urls.infoUrl);
            assertThat(rowCount(urls)).isEqualTo(4);

            assertThat(send("PUT", "/dataset/" + id, null, CSV).status()).isEqualTo(202);
            waitForDatasetAvailable(urls.infoUrl);
            assertThat(rowCount(urls)).isEqualTo(2);
        } finally {
            deleteDataset(urls.datasetUrl);
        }
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @CsvSource(delimiter = '|', value = {"text/plain", "application/octet-stream", "application/json"})
    @DisplayName("TC-ERROR-020: Uploads with another Content-Type than text/csv are rejected with 415")
    void uploadsWithOtherContentType_areRejected(String contentType) {
        assertThat(send("POST", "/datasets", contentType, CSV).status()).isEqualTo(415);
    }

    @Test
    @DisplayName("TC-ERROR-019: An upload without Content-Type and without body is rejected with 400")
    void uploadWithoutContentTypeAndBody_isRejected() {
        assertThat(send("POST", "/datasets", null, new byte[0]).status()).isEqualTo(400);
    }

    @ParameterizedTest(name = "[{index}] {0} with {1}")
    @CsvSource(delimiter = '|', nullValues = "null", value = {
            "PUT  | text/plain;charset=UTF-8",
            "PUT  | application/x-www-form-urlencoded; charset=UTF-8",
            "PUT  | null",
            "POST | text/plain;charset=UTF-8",
            "POST | application/x-www-form-urlencoded; charset=UTF-8",
            "POST | null",
            "PUT  | application/json"
    })
    @DisplayName("TC-DATASET1-024: Aliases are read from a JSON array regardless of the Content-Type")
    void aliases_anyContentType(String method, String contentType) {
        DatasetUrls urls = createDatasetAndWait(CSV);
        String alias = "alias" + UUID.randomUUID().toString().replace("-", "");
        try {
            String path = "/dataset/" + urls.datasetId + "/aliases";
            RawResponse response = send(method, path, contentType, ("[\"" + alias + "\"]").getBytes(StandardCharsets.UTF_8));
            assertThat(response.status()).as(response.body()).isEqualTo(204);
            assertThat(given().spec(jsonSpec).get(urls.datasetUrl + "/aliases").jsonPath().getList("", String.class))
                    .containsExactly(alias);
        } finally {
            given().delete(urls.datasetUrl + "/aliases");
            deleteDataset(urls.datasetUrl);
        }
    }

    @Test
    @DisplayName("TC-DATASET1-025: Alias updates whose body is not a JSON array are rejected with 400")
    void aliases_invalidBody_isRejected() {
        DatasetUrls urls = createDatasetAndWait(CSV);
        try {
            String path = "/dataset/" + urls.datasetId + "/aliases";
            assertThat(send("PUT", path, "text/plain", "alias".getBytes(StandardCharsets.UTF_8)).status()).isEqualTo(400);
            assertThat(send("PUT", path, "application/json", "{}".getBytes(StandardCharsets.UTF_8)).status()).isEqualTo(400);
            assertThat(send("POST", path, null, new byte[0]).status()).isEqualTo(400);
        } finally {
            deleteDataset(urls.datasetUrl);
        }
    }

}
