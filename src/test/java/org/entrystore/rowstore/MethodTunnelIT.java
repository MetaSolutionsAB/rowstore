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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for tunnelling the method of a POST request, as by the TunnelService of Restlet in 1.x.
 * Specification: 03-api.md (API-4.06), 07-security.md (SEC-9.05)
 */
@DisplayName("Method Tunnel Tests")
class MethodTunnelIT extends BaseIntegrationTest {

    private static final byte[] CSV = "name,n\na,1\nb,2\n".getBytes(StandardCharsets.UTF_8);

    private int rowCount(DatasetUrls urls) {
        return given().spec(jsonSpec).get(urls.infoUrl).jsonPath().getInt("rowcount");
    }

    @Test
    @DisplayName("TC-TUNNEL-001: POST with method=PUT replaces the data")
    void post_methodPut_replaces() {
        DatasetUrls urls = createDatasetAndWait(CSV);
        try {
            given().spec(csvSpec).body(CSV).post(urls.datasetUrl + "?method=PUT").then().statusCode(202);
            waitForDatasetAvailable(urls.infoUrl);
            assertThat(rowCount(urls)).isEqualTo(2);

            // without the parameter, POST appends
            given().spec(csvSpec).body(CSV).post(urls.datasetUrl).then().statusCode(202);
            waitForDatasetAvailable(urls.infoUrl);
            assertThat(rowCount(urls)).isEqualTo(4);
        } finally {
            deleteDataset(urls.datasetUrl);
        }
    }

    @Test
    @DisplayName("TC-TUNNEL-002: POST with X-HTTP-Method-Override: DELETE deletes the aliases")
    void post_methodOverrideHeader_deletesAliases() {
        DatasetUrls urls = createDatasetAndWait(CSV);
        String alias = "tunnel" + UUID.randomUUID().toString().replace("-", "");
        try {
            given().contentType("application/json").body("[\"" + alias + "\"]")
                    .put(urls.datasetUrl + "/aliases").then().statusCode(204);

            given().contentType("application/json").header("X-HTTP-Method-Override", "DELETE").body("[\"other\"]")
                    .post(urls.datasetUrl + "/aliases").then().statusCode(204);

            assertThat(given().spec(jsonSpec).get(urls.datasetUrl + "/aliases").jsonPath().getList("")).isEmpty();
        } finally {
            deleteDataset(urls.datasetUrl);
        }
    }

    @Test
    @DisplayName("TC-TUNNEL-003: The method of a GET request is never tunnelled")
    void get_methodOverride_isIgnored() {
        DatasetUrls urls = createDatasetAndWait(CSV);
        try {
            given().spec(jsonSpec).header("X-HTTP-Method-Override", "DELETE")
                    .get(urls.datasetUrl + "?method=DELETE&name=a").then().statusCode(400);
            given().spec(jsonSpec).header("X-HTTP-Method-Override", "DELETE")
                    .get(urls.datasetUrl).then().statusCode(200);
            given().spec(jsonSpec).get(urls.infoUrl).then().statusCode(200);
        } finally {
            deleteDataset(urls.datasetUrl);
        }
    }

    @Test
    @DisplayName("TC-TUNNEL-004: POST with an unsupported tunnelled method is rejected with 400 and does not append")
    void post_unsupportedMethod_isRejected() {
        DatasetUrls urls = createDatasetAndWait(CSV);
        try {
            given().spec(csvSpec).body(CSV).post(urls.datasetUrl + "?method=PATCH").then().statusCode(400);
            given().spec(csvSpec).header("X-HTTP-Method-Override", "TRACE").body(CSV).post(urls.datasetUrl)
                    .then().statusCode(400);
            assertThat(rowCount(urls)).isEqualTo(2);
        } finally {
            deleteDataset(urls.datasetUrl);
        }
    }

}
