/*
 * Copyright (c) 2011-2024 MetaSolutions AB <info@metasolutions.se>
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;

/**
 * Integration tests for rate limiting functionality.
 * Specification: 09-rate-limiting.md
 *
 * Uses a separate test environment with rate limiting enabled:
 * - Global limit: {@value RateLimitExtension#GLOBAL_LIMIT} requests per time range
 * - Per-dataset limit: {@value RateLimitExtension#DATASET_LIMIT} requests per time range
 * - Time range: {@value RateLimitExtension#TIME_RANGE_SECONDS} seconds
 *
 * Note: These tests require a separate server instance with rate limiting enabled.
 */
@DisplayName("Rate Limiting Tests")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@ExtendWith(RateLimitExtension.class)
@Tag("ratelimit")
class RateLimitIT extends ConfigurableTestBase {

    private static final Logger log = LoggerFactory.getLogger(RateLimitIT.class);
    private static final String TEST_FILE = "dataset1_utf8.csv";

    private String datasetUrl;
    private String infoUrl;

    @Override
    protected String getExtensionBaseUrl() {
        return RateLimitExtension.getBaseUrl();
    }

    @BeforeAll
    void createDataset() {
        byte[] csvData = loadTestData(TEST_FILE);
        Response response = createDataset(csvData);

        response.then().statusCode(202);
        datasetUrl = getDatasetUrl(response);
        infoUrl = getInfoUrl(response);

        // Wait for dataset with longer intervals to avoid hitting rate limit during setup
        await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                        Response infoResponse = given()
                                .spec(jsonSpec)
                                .get(infoUrl);
                        if (infoResponse.getStatusCode() == 429) {
                            log.warn("Rate limited during setup, will retry...");
                            throw new AssertionError("Rate limited, retry");
                        }
                        infoResponse.then().body("status", equalTo(ETL_STATUS_AVAILABLE));
                });
    }

    @AfterAll
    void cleanup() {
        if (datasetUrl != null) {
            int status = given().delete(datasetUrl).getStatusCode();
            if (status != 204) {
                log.warn("Cleanup delete returned {} for {}", status, datasetUrl);
            }
        }
    }

    @Test
    @Order(1)
    @DisplayName("TC-RATE-001: Per-dataset rate limit exceeded returns 429")
    void perDatasetRateLimit_returns429() {
        // Make requests up to and beyond the limit
        // Note: Setup may have used some of the quota already
        int successCount = 0;
        int rateLimitedCount = 0;

        // Make significantly more requests than the limit to ensure we hit it
        // Even if setup used some quota, we should still trigger the limit
        int requestCount = RateLimitExtension.DATASET_LIMIT + 15;
        for (int i = 0; i < requestCount; i++) {
            Response response = given()
                    .spec(jsonSpec)
            .when()
                    .get(datasetUrl);

            if (response.getStatusCode() == 200) {
                successCount++;
            } else if (response.getStatusCode() == 429) {
                rateLimitedCount++;
            }
        }

        log.info("Rate limit test results: {} successful, {} rate-limited out of {} requests",
                successCount, rateLimitedCount, requestCount);

        // Should have some successful requests (at least up to the limit)
        assertThat(successCount).as("Should have some successful requests").isGreaterThan(0);

        // After exceeding limit, should get 429
        assertThat(rateLimitedCount).as("Should hit rate limit after exceeding dataset limit").isGreaterThanOrEqualTo(1);
    }

    @Test
    @Order(2)
    @DisplayName("TC-RATE-002: Rate limit includes Retry-After header")
    void rateLimitResponse_hasRetryAfterHeader() {
        // Make requests until we hit the rate limit
        Response rateLimitedResponse = null;

        // Continue making requests until we hit a 429
        for (int i = 0; i < RateLimitExtension.DATASET_LIMIT + 30; i++) {
            Response response = given()
                    .spec(jsonSpec)
            .when()
                    .get(datasetUrl);

            if (response.getStatusCode() == 429) {
                rateLimitedResponse = response;
                break;
            }
        }

        // We should have hit the rate limit by now
        assertThat(rateLimitedResponse).as("Should have received a 429 response").isNotNull();

        // Sliding window rate limiter provides Retry-After header
        String retryAfter = rateLimitedResponse.getHeader("Retry-After");
        log.info("Rate limit response Retry-After header: {}", retryAfter);
        assertThat(retryAfter).as("Sliding window rate limiter should return Retry-After header")
                .isNotNull().isNotEmpty();
    }

    @Test
    @Order(3)
    @DisplayName("TC-RATE-004: Status endpoint is exempt from rate limiting")
    void statusEndpoint_exemptFromRateLimit() {
        // Status endpoint should always succeed regardless of rate limit
        for (int i = 0; i < 10; i++) {
            given()
                    .spec(jsonSpec)
            .when()
                    .get("/status")
            .then()
                    .statusCode(200);
        }
    }

    @Test
    @Order(4)
    @DisplayName("TC-RATE-005: POST/PUT bypass rate limiting")
    void mutationsNotRateLimited() {
        // POST/PUT should not be rate limited (only GET/HEAD are)
        byte[] csvData = loadTestData(TEST_FILE);

        for (int i = 0; i < 5; i++) {
            Response response = given()
                    .spec(csvSpec)
                    .body(csvData)
            .when()
                    .post("/datasets");

            // Should always get 202, not 429
            assertThat(response.getStatusCode()).isEqualTo(202);

            // Clean up
            String newUrl = response.jsonPath().getString("url");
            String newInfoUrl = response.jsonPath().getString("info");
            await()
                    .atMost(Duration.ofSeconds(15))
                    .pollInterval(Duration.ofMillis(1000))
                    .untilAsserted(() -> {
                        Response infoResponse = given()
                                .spec(jsonSpec)
                                .get(newInfoUrl);
                        if (infoResponse.getStatusCode() == 429) {
                            throw new AssertionError("Rate limited, retry");
                        }
                        infoResponse.then().body("status", equalTo(ETL_STATUS_AVAILABLE));
                    });
            int deleteStatus = given().delete(newUrl).getStatusCode();
            if (deleteStatus != 204) {
                log.warn("Cleanup delete returned {} for {}", deleteStatus, newUrl);
            }
        }
    }

    @Test
    @Order(5)
    @DisplayName("TC-RATE-006: Rate limit resets after time window")
    void rateLimitResetsAfterTimeWindow() throws InterruptedException {
        // First, exhaust the rate limit
        int rateLimitedCount = 0;
        for (int i = 0; i < RateLimitExtension.DATASET_LIMIT + 10; i++) {
            Response response = given()
                    .spec(jsonSpec)
            .when()
                    .get(datasetUrl);
            if (response.getStatusCode() == 429) {
                rateLimitedCount++;
            }
        }

        // Verify we hit the limit
        assertThat(rateLimitedCount).as("Should have hit rate limit").isGreaterThanOrEqualTo(1);

        // Wait for the time window to expire (just beyond the configured time range)
        log.info("Waiting for rate limit window to expire ({} seconds)...", RateLimitExtension.TIME_RANGE_SECONDS);
        Thread.sleep((RateLimitExtension.TIME_RANGE_SECONDS + 1) * 1000L);

        // After waiting, we should be able to make at least one successful request
        // as the entries should have expired from the sliding window
        boolean foundSuccess = false;
        for (int i = 0; i < 3; i++) {
            Response response = given()
                    .spec(jsonSpec)
            .when()
                    .get(datasetUrl);
            if (response.getStatusCode() == 200) {
                foundSuccess = true;
                log.info("Rate limit window reset verified - request succeeded");
                break;
            }
            // Small delay between retries
            Thread.sleep(2000);
        }

        assertThat(foundSuccess)
                .as("Should be able to make requests after rate limit window expires")
                .isTrue();
    }

    @Test
    @Order(6)
    @DisplayName("TC-RATE-007: Global rate limit enforced across datasets")
    void globalRateLimit_enforcedAcrossDatasets() throws InterruptedException {
        // Wait for rate limit window to reset from previous tests
        Thread.sleep((RateLimitExtension.TIME_RANGE_SECONDS + 1) * 1000L);

        // Create a second dataset to alternate requests between
        byte[] csvData = loadTestData(TEST_FILE);
        Response response = createDataset(csvData);
        response.then().statusCode(202);

        String dataset2Url = getDatasetUrl(response);
        String dataset2InfoUrl = getInfoUrl(response);

        await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    Response infoResponse = given().spec(jsonSpec).get(dataset2InfoUrl);
                    if (infoResponse.getStatusCode() == 429) {
                        throw new AssertionError("Rate limited, retry");
                    }
                    infoResponse.then().body("status", equalTo(ETL_STATUS_AVAILABLE));
                });

        try {
            // Alternate requests across two datasets to exceed global limit
            int successCount = 0;
            int rateLimitedCount = 0;
            int requestCount = RateLimitExtension.GLOBAL_LIMIT + 10;

            for (int i = 0; i < requestCount; i++) {
                String url = (i % 2 == 0) ? datasetUrl : dataset2Url;
                Response resp = given().spec(jsonSpec).when().get(url);
                if (resp.getStatusCode() == 200) {
                    successCount++;
                } else if (resp.getStatusCode() == 429) {
                    rateLimitedCount++;
                }
            }

            log.info("Global rate limit test: {} successful, {} rate-limited out of {} requests",
                    successCount, rateLimitedCount, requestCount);

            assertThat(successCount).as("Should have some successful requests").isGreaterThan(0);
            assertThat(rateLimitedCount).as("Should hit global rate limit").isGreaterThanOrEqualTo(1);
        } finally {
            int deleteStatus = given().delete(dataset2Url).getStatusCode();
            if (deleteStatus != 204) {
                log.warn("Cleanup delete returned {} for {}", deleteStatus, dataset2Url);
            }
        }
    }

    @Test
    @Order(7)
    @DisplayName("TC-RATE-008: HEAD requests are rate-limited")
    void headRequestsAreRateLimited() throws InterruptedException {
        // Wait for rate limit window to reset from previous tests
        Thread.sleep((RateLimitExtension.TIME_RANGE_SECONDS + 1) * 1000L);

        int successCount = 0;
        int rateLimitedCount = 0;

        int requestCount = RateLimitExtension.DATASET_LIMIT + 15;
        for (int i = 0; i < requestCount; i++) {
            Response response = given()
                    .spec(jsonSpec)
            .when()
                    .head(datasetUrl);

            if (response.getStatusCode() == 200) {
                successCount++;
            } else if (response.getStatusCode() == 429) {
                rateLimitedCount++;
            }
        }

        log.info("HEAD rate limit test: {} successful, {} rate-limited out of {} requests",
                successCount, rateLimitedCount, requestCount);

        assertThat(successCount).as("Should have some successful HEAD requests").isGreaterThan(0);
        assertThat(rateLimitedCount).as("HEAD requests should be rate-limited").isGreaterThanOrEqualTo(1);
    }
}
