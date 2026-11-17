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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for writes to /dataset/{id}/json, which is the same resource as /dataset/{id}, as in 1.x.
 * Specification: 03-api.md (API-3.16)
 */
@DisplayName("Dataset JSON Path Tests")
class DatasetJsonPathIT extends BaseIntegrationTest {

    private static final byte[] CSV = "name,n\na,1\nb,2\n".getBytes(StandardCharsets.UTF_8);

    private int rowCount(DatasetUrls urls) {
        return given().spec(jsonSpec).get(urls.infoUrl).jsonPath().getInt("rowcount");
    }

    private void assertAccepted(Response response, DatasetUrls urls) {
        assertThat(response.statusCode()).isEqualTo(202);
        assertThat(response.header("Location")).isEqualTo(urls.datasetUrl);
        assertThat(response.jsonPath().getString("url")).isEqualTo(urls.datasetUrl);
        assertThat(response.jsonPath().getString("info")).isEqualTo(urls.infoUrl);
        waitForDatasetAvailable(urls.infoUrl);
    }

    private static Set<String> allowedMethods(String url) {
        Response response = given().options(url);
        assertThat(response.statusCode()).isEqualTo(200);
        return Arrays.stream(response.header("Allow").split(",")).map(String::trim).collect(Collectors.toSet());
    }

    @Test
    @DisplayName("TC-JSONPATH-001: POST appends and PUT replaces, also tunnelled, with the Location of the dataset")
    void postAndPut_appendAndReplace() {
        DatasetUrls urls = createDatasetAndWait(CSV);
        String jsonUrl = urls.datasetUrl + "/json";
        try {
            assertAccepted(given().spec(csvSpec).body(CSV).post(jsonUrl), urls);
            assertThat(rowCount(urls)).isEqualTo(4);

            assertAccepted(given().spec(csvSpec).body(CSV).put(jsonUrl), urls);
            assertThat(rowCount(urls)).isEqualTo(2);

            assertAccepted(given().spec(csvSpec).body(CSV).post(jsonUrl), urls);
            assertAccepted(given().spec(csvSpec).body(CSV).post(jsonUrl + "?method=PUT"), urls);
            assertThat(rowCount(urls)).isEqualTo(2);
        } finally {
            deleteDataset(urls.datasetUrl);
        }
    }

    @Test
    @DisplayName("TC-JSONPATH-002: DELETE deletes the dataset, also tunnelled")
    void delete_deletesDataset() {
        DatasetUrls deleted = createDatasetAndWait(CSV);
        DatasetUrls tunnelled = createDatasetAndWait(CSV);
        try {
            given().delete(deleted.datasetUrl + "/json").then().statusCode(204);
            given().spec(jsonSpec).get(deleted.infoUrl).then().statusCode(404);

            given().post(tunnelled.datasetUrl + "/json?method=DELETE").then().statusCode(204);
            given().spec(jsonSpec).get(tunnelled.infoUrl).then().statusCode(404);

            given().delete(deleted.datasetUrl + "/json").then().statusCode(404);
        } finally {
            // without status assertion: deleted by the test unless it failed
            given().delete(deleted.datasetUrl);
            given().delete(tunnelled.datasetUrl);
        }
    }

    @Test
    @DisplayName("TC-JSONPATH-003: OPTIONS lists the same methods as for the dataset resource")
    void options_sameMethodsAsDatasetResource() {
        DatasetUrls urls = createDatasetAndWait(CSV);
        try {
            assertThat(allowedMethods(urls.datasetUrl + "/json"))
                    .contains("GET", "PUT", "POST", "DELETE")
                    .isEqualTo(allowedMethods(urls.datasetUrl));
        } finally {
            deleteDataset(urls.datasetUrl);
        }
    }

}
