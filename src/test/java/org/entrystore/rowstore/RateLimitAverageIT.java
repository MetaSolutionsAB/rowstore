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
 * Integration tests for average-type rate limiting.
 *
 * The average type uses Guava's {@code RateLimiter} instead of a sliding window.
 * Key difference: average type returns -1 from the rate check (unknown retry time),
 * so no Retry-After header is set.
 */
@DisplayName("Rate Limiting - Average Type Tests")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@ExtendWith(RateLimitAverageExtension.class)
@Tag("ratelimit")
class RateLimitAverageIT extends ConfigurableTestBase {

    private static final Logger log = LoggerFactory.getLogger(RateLimitAverageIT.class);
    private static final String TEST_FILE = "dataset1_utf8.csv";

    private String datasetUrl;
    private String infoUrl;

    @Override
    protected String getExtensionBaseUrl() {
        return RateLimitAverageExtension.getBaseUrl();
    }

    @BeforeAll
    void createDataset() {
        byte[] csvData = loadTestData(TEST_FILE);
        Response response = createDataset(csvData);

        response.then().statusCode(202);
        datasetUrl = getDatasetUrl(response);
        infoUrl = getInfoUrl(response);

        await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    Response infoResponse = given()
                            .spec(jsonSpec)
                            .get(infoUrl);
                    if (infoResponse.getStatusCode() == 429) {
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
    @DisplayName("TC-RATE-AVG-001: Per-dataset rate limit works with average type")
    void perDatasetLimit_worksWithAverageType() throws InterruptedException {
        // Wait a moment for any previous rate limiting to expire
        Thread.sleep(2000);

        int successCount = 0;
        int rateLimitedCount = 0;

        // Average rate limiter (Guava RateLimiter) uses permits/second.
        // With dataset=10 and timeRange=10, that's 1 permit/sec.
        // tryAcquire() returns immediately, so rapid requests will be rejected.
        // The first request may succeed due to stored permits (burst).
        int requestCount = 20;
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

        log.info("Average rate limit test: {} successful, {} rate-limited out of {} requests",
                successCount, rateLimitedCount, requestCount);

        // With averaging, rapid bursts should trigger rate limiting
        assertThat(rateLimitedCount).as("Should hit rate limit with average type").isGreaterThanOrEqualTo(1);
    }

    @Test
    @Order(2)
    @DisplayName("TC-RATE-AVG-002: Average type 429 response does NOT have Retry-After header")
    void averageType_noRetryAfterHeader() {
        // Make requests until we hit the rate limit
        Response rateLimitedResponse = null;

        for (int i = 0; i < RateLimitAverageExtension.DATASET_LIMIT + 30; i++) {
            Response response = given()
                    .spec(jsonSpec)
            .when()
                    .get(datasetUrl);

            if (response.getStatusCode() == 429) {
                rateLimitedResponse = response;
                break;
            }
        }

        assertThat(rateLimitedResponse).as("Should have received a 429 response").isNotNull();

        // Average type returns -1 (unknown retry time), so no Retry-After header
        String retryAfter = rateLimitedResponse.getHeader("Retry-After");
        log.info("Average type rate limit Retry-After header: {}", retryAfter);
        assertThat(retryAfter).as("Average type should NOT have Retry-After header").isNull();
    }
}
