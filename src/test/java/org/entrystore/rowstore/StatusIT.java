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

import io.restassured.http.ContentType;
import io.restassured.response.Response;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Integration tests for the /status endpoint.
 * Specification: 01-status.md
 */
@DisplayName("Status Endpoint Tests")
class StatusIT extends BaseIntegrationTest {

    @Test
    @DisplayName("TC-STATUS-001: GET status returns valid structure with correct values")
    void getStatus_returnsValidStructure() {
        Response response = given()
                .spec(jsonSpec)
        .when()
                .get("/status")
        .then()
                .statusCode(200)
                .contentType(ContentType.JSON)
                .body("service", equalTo("RowStore"))
                .body("datasets", instanceOf(Integer.class))
                .body("datasets", greaterThanOrEqualTo(0))
                .body("activeEtlProcesses", instanceOf(Integer.class))
                .body("activeEtlProcesses", greaterThanOrEqualTo(0))
                .body("version", instanceOf(String.class))
                .extract()
                .response();

        // Additional AssertJ validations for complex assertions
        String version = response.jsonPath().getString("version");
        assertThat(version).isNotEmpty();
    }

}
