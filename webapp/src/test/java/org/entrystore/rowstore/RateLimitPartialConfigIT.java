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
 * Integration tests for partial rate limit configuration.
 *
 * Tests the scenario where global rate limit is disabled (-1) but per-dataset
 * rate limit is active. This covers the bug fixed by changing the constructor
 * guard from {@code != -1} to {@code > 0}.
 */
@DisplayName("Rate Limiting - Partial Config Tests")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@ExtendWith(PartialRateLimitExtension.class)
@Tag("ratelimit")
class RateLimitPartialConfigIT extends ConfigurableTestBase {

    private static final Logger log = LoggerFactory.getLogger(RateLimitPartialConfigIT.class);
    private static final String TEST_FILE = "dataset1_utf8.csv";

    private String datasetUrl;
    private String infoUrl;

    @Override
    protected String getExtensionBaseUrl() {
        return PartialRateLimitExtension.getBaseUrl();
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
            given().delete(datasetUrl);
        }
    }

    @Test
    @Order(1)
    @DisplayName("TC-RATE-PARTIAL-001: Server starts with global=-1 (disabled) and dataset limit active")
    void serverStartsWithPartialConfig() {
        // If we get here, the server started without error, which means the
        // constructor guard (> 0) correctly handles global=-1
        given()
                .spec(jsonSpec)
        .when()
                .get("/status")
        .then()
                .statusCode(200);
    }

    @Test
    @Order(2)
    @DisplayName("TC-RATE-PARTIAL-002: Per-dataset rate limit works with global disabled")
    void perDatasetLimit_worksWithGlobalDisabled() {
        int successCount = 0;
        int rateLimitedCount = 0;

        int requestCount = PartialRateLimitExtension.DATASET_LIMIT + 10;
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

        log.info("Partial config rate limit test: {} successful, {} rate-limited out of {} requests",
                successCount, rateLimitedCount, requestCount);

        assertThat(successCount).as("Should have some successful requests").isGreaterThan(0);
        assertThat(rateLimitedCount).as("Should hit dataset rate limit").isGreaterThanOrEqualTo(1);
    }

    @Test
    @Order(3)
    @DisplayName("TC-RATE-PARTIAL-003: Status endpoint remains exempt")
    void statusEndpoint_exemptFromRateLimit() {
        for (int i = 0; i < 15; i++) {
            given()
                    .spec(jsonSpec)
            .when()
                    .get("/status")
            .then()
                    .statusCode(200);
        }
    }
}
