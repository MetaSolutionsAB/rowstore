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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.ThreadLocalRandom;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for the per-client-IP rate limit behind (trusted and untrusted) reverse proxies.
 * Specification: 09-rate-limiting.md, SEC-5.05, SEC-5.06
 */
@DisplayName("Per-Client-IP Rate Limiting Tests")
@ExtendWith(ClientIpRateLimitExtension.class)
@Tag("ratelimit")
class RateLimitClientIpIT {

    private static final int LIMIT = ClientIpRateLimitExtension.CLIENT_IP_LIMIT;

    private static int get(String baseUrl, String xForwardedFor) {
        return given()
                .baseUri(baseUrl)
                .header("X-Forwarded-For", xForwardedFor)
        .when()
                .get("/datasets")
                .getStatusCode();
    }

    private static String randomIp() {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        return r.nextInt(1, 224) + "." + r.nextInt(256) + "." + r.nextInt(256) + "." + r.nextInt(1, 255);
    }

    @Test
    @DisplayName("TC-RATE-009: Spoofed X-Forwarded-For from an untrusted peer does not bypass the client IP limit")
    void spoofedForwardedFor_isIgnored() {
        String baseUrl = ClientIpRateLimitExtension.getUntrustedBaseUrl();
        int successCount = 0;
        int rateLimitedCount = 0;
        for (int i = 0; i < LIMIT + 5; i++) {
            int status = get(baseUrl, randomIp());
            if (status == 200) {
                successCount++;
            } else if (status == 429) {
                rateLimitedCount++;
            }
        }

        assertThat(successCount).as("At most the client IP limit may be admitted").isLessThanOrEqualTo(LIMIT);
        assertThat(rateLimitedCount).as("Random X-Forwarded-For values must not reset the client IP limit")
                .isGreaterThanOrEqualTo(1);
    }

    @Test
    @DisplayName("TC-CORS-007: Rate-limited responses (429) carry CORS headers and expose Retry-After")
    void rateLimitedResponse_hasCorsHeaders() {
        String baseUrl = ClientIpRateLimitExtension.getUntrustedBaseUrl();
        Response rateLimited = null;
        for (int i = 0; i < LIMIT + 1 && rateLimited == null; i++) {
            Response response = given()
                    .baseUri(baseUrl)
                    .header("Origin", "http://example.com")
            .when()
                    .get("/datasets");
            if (response.getStatusCode() == 429) {
                rateLimited = response;
            }
        }

        assertThat(rateLimited).as("Client IP limit should be exceeded").isNotNull();
        assertThat(rateLimited.getHeader("Access-Control-Allow-Origin")).isEqualTo("*");
        assertThat(rateLimited.getHeader("Access-Control-Expose-Headers")).contains("Retry-After");
        assertThat(rateLimited.getHeader("Retry-After")).isNotEmpty();
    }

    @Test
    @DisplayName("TC-RATE-010: Clients behind a trusted proxy are limited independently by X-Forwarded-For")
    void trustedProxy_limitsClientsIndependently() {
        String baseUrl = ClientIpRateLimitExtension.getTrustedBaseUrl();

        // Exhaust the limit of 10.0.0.1
        for (int i = 0; i < LIMIT; i++) {
            assertThat(get(baseUrl, "10.0.0.1")).as("Request %d of 10.0.0.1", i + 1).isEqualTo(200);
        }
        assertThat(get(baseUrl, "10.0.0.1")).as("10.0.0.1 over its limit").isEqualTo(429);

        // Another client behind the same proxy has its own limit
        assertThat(get(baseUrl, "10.0.0.2")).as("10.0.0.2 is limited independently").isEqualTo(200);

        // A client-supplied left part is ignored: the entry appended by the trusted proxy is the key
        assertThat(get(baseUrl, randomIp() + ", 10.0.0.1")).as("Spoofed prefix, keyed on 10.0.0.1").isEqualTo(429);
        assertThat(get(baseUrl, randomIp() + ", 10.0.0.2")).as("Spoofed prefix, keyed on 10.0.0.2").isEqualTo(200);

        // Trusted hops are skipped from the right until the first untrusted address
        assertThat(get(baseUrl, randomIp() + ", 10.0.0.1, 127.0.0.1")).as("Trusted hop skipped, keyed on 10.0.0.1")
                .isEqualTo(429);
    }

    @Test
    @DisplayName("TC-RATE-013: Without trustedproxies, the last X-Forwarded-For entry of the connecting peer is the client")
    void defaultTrust_lastEntryIsClient() {
        String baseUrl = ClientIpRateLimitExtension.getDefaultBaseUrl();

        // The test client acts as the reverse proxy that appends the client address; the left part is client-supplied
        for (int i = 0; i < LIMIT; i++) {
            assertThat(get(baseUrl, randomIp() + ", 10.0.0.1")).as("Request %d of 10.0.0.1", i + 1).isEqualTo(200);
        }
        assertThat(get(baseUrl, randomIp() + ", 10.0.0.1")).as("10.0.0.1 over its limit").isEqualTo(429);

        // Another client behind the proxy has its own limit
        assertThat(get(baseUrl, "10.0.0.2")).as("10.0.0.2 is limited independently").isEqualTo(200);

        // Private addresses are not skipped: a client from a private network cannot select another key
        assertThat(get(baseUrl, "10.0.0.3, 10.0.0.1")).as("Keyed on the last entry 10.0.0.1").isEqualTo(429);
    }

}
