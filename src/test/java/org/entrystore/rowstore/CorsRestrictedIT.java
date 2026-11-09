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
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.entrystore.rowstore.CorsRestrictedExtension.ALLOWED_ORIGIN;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Integration tests for a restricted "cors.allowedorigins" (SEC-6.02): origins that are not listed are rejected with
 * 403, requests without Origin and same-origin requests are not checked.
 * Specification: specs/tests/15-cors.md
 */
@DisplayName("CORS Restricted Origins Tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@ExtendWith(CorsRestrictedExtension.class)
@Tag("cors-restricted")
class CorsRestrictedIT extends ConfigurableTestBase {

    private static final String UNLISTED_ORIGIN = "https://evil.example.com";

    @Override
    protected String getExtensionBaseUrl() {
        return CorsRestrictedExtension.getBaseUrl();
    }

    @Test
    @Order(1)
    @DisplayName("TC-CORS-008: A listed origin gets the CORS headers")
    void listedOrigin_getsCorsHeaders() {
        given()
                .spec(jsonSpec)
                .header("Origin", ALLOWED_ORIGIN)
        .when()
                .get("/status")
        .then()
                .statusCode(200)
                .header("Access-Control-Allow-Origin", equalTo(ALLOWED_ORIGIN));

        given()
                .header("Origin", ALLOWED_ORIGIN)
                .header("Access-Control-Request-Method", "POST")
        .when()
                .options("/datasets")
        .then()
                .statusCode(200)
                .header("Access-Control-Allow-Origin", equalTo(ALLOWED_ORIGIN));
    }

    @Test
    @Order(2)
    @DisplayName("TC-CORS-009: An origin that is not listed is rejected with 403")
    void unlistedOrigin_isRejected() {
        Response get = given()
                .spec(jsonSpec)
                .header("Origin", UNLISTED_ORIGIN)
        .when()
                .get("/status");
        assertThat(get.statusCode()).isEqualTo(403);
        assertThat(get.body().asString()).isEqualTo("Invalid CORS request");
        assertThat(get.header("Access-Control-Allow-Origin")).isNull();

        // also a write, which must not reach the controller
        given()
                .header("Origin", UNLISTED_ORIGIN)
                .contentType("text/csv")
                .body("a,b\n1,2\n")
        .when()
                .post("/datasets")
        .then()
                .statusCode(403)
                .header("Location", nullValue());

        given()
                .header("Origin", UNLISTED_ORIGIN)
                .header("Access-Control-Request-Method", "DELETE")
        .when()
                .options("/datasets")
        .then()
                .statusCode(403)
                .header("Access-Control-Allow-Origin", nullValue());
    }

    @Test
    @Order(3)
    @DisplayName("TC-CORS-010: Origin null is rejected with 403")
    void nullOrigin_isRejected() {
        given()
                .spec(jsonSpec)
                .header("Origin", "null")
        .when()
                .get("/status")
        .then()
                .statusCode(403);
    }

    @Test
    @Order(4)
    @DisplayName("TC-CORS-011: Requests without Origin are not checked")
    void noOrigin_isNotChecked() {
        given()
                .spec(jsonSpec)
        .when()
                .get("/status")
        .then()
                .statusCode(200)
                .header("Access-Control-Allow-Origin", nullValue());
    }

    @Test
    @Order(5)
    @DisplayName("TC-CORS-012: Same-origin requests are not checked, another scheme of the same host is rejected")
    void sameOrigin_isNotChecked() {
        String ownOrigin = getExtensionBaseUrl();
        given()
                .spec(jsonSpec)
                .header("Origin", ownOrigin)
        .when()
                .get("/status")
        .then()
                .statusCode(200);

        // like a page served over https by a TLS-terminating proxy in front of RowStore (SEC-6.02)
        given()
                .spec(jsonSpec)
                .header("Origin", ownOrigin.replaceFirst("^http:", "https:"))
        .when()
                .get("/status")
        .then()
                .statusCode(403);
    }

}
