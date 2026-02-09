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
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.UUID;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests for CORS support.
 * Verifies that the CORS filter is properly configured for cross-origin requests.
 */
@DisplayName("CORS Tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class CorsIT extends BaseIntegrationTest {

    private static final String TEST_ORIGIN = "http://example.com";

    @Test
    @Order(1)
    @DisplayName("TC-CORS-001: Preflight OPTIONS returns CORS headers")
    void preflight_returnsCorsHeaders() {
        given()
                .header("Origin", TEST_ORIGIN)
                .header("Access-Control-Request-Method", "GET")
        .when()
                .options("/status")
        .then()
                .statusCode(200)
                .header("Access-Control-Allow-Origin", notNullValue());
    }

    @Test
    @Order(2)
    @DisplayName("TC-CORS-002: GET with Origin header includes CORS response headers")
    void getWithOrigin_includesCorsHeaders() {
        given()
                .header("Origin", TEST_ORIGIN)
                .accept("application/json")
        .when()
                .get("/status")
        .then()
                .statusCode(200)
                .header("Access-Control-Allow-Origin", notNullValue());
    }

    @Test
    @Order(3)
    @DisplayName("TC-CORS-003: CORS headers on 404 error responses")
    void corsOnError_includesHeaders() {
        String fakeId = UUID.randomUUID().toString();

        given()
                .header("Origin", TEST_ORIGIN)
                .accept("application/json")
        .when()
                .get("/dataset/" + fakeId)
        .then()
                .statusCode(404)
                .header("Access-Control-Allow-Origin", notNullValue());
    }

    @Test
    @Order(4)
    @DisplayName("TC-CORS-004: Preflight allows POST method")
    void preflight_allowsPost() {
        given()
                .header("Origin", TEST_ORIGIN)
                .header("Access-Control-Request-Method", "POST")
        .when()
                .options("/datasets")
        .then()
                .statusCode(200)
                .header("Access-Control-Allow-Methods", containsString("POST"));
    }

    @Test
    @Order(5)
    @DisplayName("TC-CORS-005: Preflight allows DELETE method")
    void preflight_allowsDelete() {
        given()
                .header("Origin", TEST_ORIGIN)
                .header("Access-Control-Request-Method", "DELETE")
        .when()
                .options("/datasets")
        .then()
                .statusCode(200)
                .header("Access-Control-Allow-Methods", containsString("DELETE"));
    }

    @Test
    @Order(6)
    @DisplayName("TC-CORS-006: CORS exposes Location header")
    void cors_exposesLocationHeader() {
        given()
                .header("Origin", TEST_ORIGIN)
                .header("Access-Control-Request-Method", "GET")
        .when()
                .options("/status")
        .then()
                .statusCode(200)
                .header("Access-Control-Expose-Headers", containsString("Location"));
    }
}
