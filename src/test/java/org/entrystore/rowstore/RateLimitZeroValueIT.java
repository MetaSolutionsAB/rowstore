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

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for zero-value rate limit configuration.
 *
 * Tests the boundary case where global=0 and dataset=0 with the {@code > 0} guard.
 * Zero values should mean rate limiting is disabled (not "block all requests").
 */
@DisplayName("Rate Limiting - Zero Value Config Tests")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@ExtendWith(ZeroValueRateLimitExtension.class)
@Tag("ratelimit")
class RateLimitZeroValueIT extends ConfigurableTestBase {

    private static final Logger log = LoggerFactory.getLogger(RateLimitZeroValueIT.class);
    private static final String TEST_FILE = "dataset1_utf8.csv";

    private String datasetUrl;

    @Override
    protected String getExtensionBaseUrl() {
        return ZeroValueRateLimitExtension.getBaseUrl();
    }

    @BeforeAll
    void createDataset() {
        byte[] csvData = loadTestData(TEST_FILE);
        Response response = createDataset(csvData);

        response.then().statusCode(202);
        datasetUrl = getDatasetUrl(response);
        String infoUrl = getInfoUrl(response);

        waitForDatasetAvailable(infoUrl);
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
    @DisplayName("TC-RATE-ZERO-001: Server starts normally with zero-value config")
    void serverStartsWithZeroValues() {
        given()
                .spec(jsonSpec)
        .when()
                .get("/status")
        .then()
                .statusCode(200);
    }

    @Test
    @Order(2)
    @DisplayName("TC-RATE-ZERO-002: No requests are rate-limited with zero config")
    void noRequestsRateLimited() {
        int successCount = 0;
        int rateLimitedCount = 0;

        // Make many rapid requests - none should be rate-limited
        int requestCount = 30;
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

        log.info("Zero-value rate limit test: {} successful, {} rate-limited out of {} requests",
                successCount, rateLimitedCount, requestCount);

        assertThat(rateLimitedCount)
                .as("No requests should be rate-limited with zero config values")
                .isEqualTo(0);
        assertThat(successCount)
                .as("All requests should succeed")
                .isEqualTo(requestCount);
    }
}
