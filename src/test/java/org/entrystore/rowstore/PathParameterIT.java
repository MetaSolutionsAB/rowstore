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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for the rejection of path parameters (";") and for encoded slashes in dataset IDs.
 * Specification: 08-error-handling.md, SEC-4.11, QUERY-9.03
 */
@DisplayName("Path Parameter Tests")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PathParameterIT extends BaseIntegrationTest {

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

    private static int status(String pathAndQuery) {
        return given()
                .urlEncodingEnabled(false)
        .when()
                .get(pathAndQuery)
                .getStatusCode();
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {
            "/v3;x/api-docs",
            "/status;x?jvm",
            "/status;x",
            "/swagger-ui;x/index.html",
            "/actuator;x/health",
            "/actuator/health;x",
            "/datasets;a",
            "/;x"
    })
    @DisplayName("TC-ERROR-017: Request paths with path parameters (;) are rejected with 400")
    void pathParameters_areRejected(String path) {
        given()
                .urlEncodingEnabled(false)
        .when()
                .get(path)
        .then()
                .statusCode(400);
    }

    @Test
    @DisplayName("TC-ERROR-018: Dataset paths with path parameters are rejected, other requests are unaffected")
    void datasetPathParameters_areRejected() {
        String path = "/dataset/" + dataset.datasetId;

        // Each variant would otherwise be served and get its own per-dataset rate limit bucket
        assertThat(status(path + ";x")).isEqualTo(400);
        assertThat(status(path + ";1/info")).isEqualTo(400);
        assertThat(given().urlEncodingEnabled(false).delete(path + ";x").getStatusCode()).isEqualTo(400);

        // Unaffected: plain paths, ";" in the query string, and encoded %3B (not a path parameter delimiter)
        assertThat(status(path)).isEqualTo(200);
        assertThat(status(path + "/info")).isEqualTo(200);
        assertThat(status(path + "?Name=a;b")).isEqualTo(200);
        assertThat(status(path + "%3Bx")).isEqualTo(404);
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {
            "/dataset/a%2Fb",
            "/dataset/a%2fb",
            "/dataset/a%5Cb",
            "/dataset/a%5cb",
            "/dataset/a%2Fb/info",
            "/dataset/a%5Cb/info",
            "/dataset/a%2Fb/export",
            "/dataset/a%5Cb/export",
            "/dataset/a%2Fb/aliases",
            "/dataset/a%5Cb/aliases",
            "/dataset/a%2Fb/json",
            "/dataset/a%2Fb/html",
            // no traversal: the encoded slashes stay part of the ID
            "/dataset/x%2F..%2Fstatus",
            "/dataset/x%5C..%5Cstatus",
            "/dataset/%2E%2E%2Fstatus",
            "/dataset/{ds}%2Finfo",
            "/dataset/{ds}%5Cinfo",
            "/dataset%2F{ds}",
            "/actuator%2Fhealth"
    })
    @DisplayName("TC-ERROR-NEW1: Encoded slashes and backslashes in dataset paths return 404, as in 1.x")
    void encodedSlashes_return404(String path) {
        String fullPath = path.replace("{ds}", dataset.datasetId);
        // a raw request, since REST Assured and java.net.URI would re-encode or normalize the path
        TestUtils.RawResponse response = TestUtils.rawGet(getBaseUrl(), fullPath, "Accept: application/json");
        assertThat(response.status()).as(fullPath).isEqualTo(404);
    }

    @Test
    @DisplayName("TC-ERROR-NEW1: A DELETE with encoded slashes does not reach another dataset")
    void encodedSlashes_deleteDoesNotTraverse() {
        for (String path : new String[]{"/dataset/x%2F..%2F{ds}", "/dataset/x%5C..%5C{ds}", "/dataset/{ds}%2F",
                "/dataset/x%2F..%2F{ds}/aliases"}) {
            String fullPath = path.replace("{ds}", dataset.datasetId);
            assertThat(TestUtils.rawRequest(getBaseUrl(), "DELETE", fullPath, List.of(), null).status())
                    .as("DELETE " + fullPath).isEqualTo(404);
        }
        assertThat(status("/dataset/" + dataset.datasetId)).isEqualTo(200);
    }

}
