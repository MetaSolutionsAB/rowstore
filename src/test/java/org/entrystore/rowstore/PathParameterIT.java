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

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for the rejection of path parameters (";").
 * Specification: 08-error-handling.md, SEC-4.11
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

}
