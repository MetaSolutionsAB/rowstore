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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Integration tests verifying that HikariCP connection pooling works correctly
 * when explicitly enabled via connectionPoolMax in the database configuration.
 *
 * The default (no pool) path is covered by all other integration tests since
 * RowStoreTestEnvironment does not set connectionPoolMax.
 */
@DisplayName("Connection Pool Tests")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@ExtendWith(ConnectionPoolExtension.class)
@Tag("connection-pool")
class ConnectionPoolIT extends ConfigurableTestBase {

    private static final String TEST_FILE = "dataset1_utf8.csv";

    private String datasetUrl;
    private String infoUrl;

    @Override
    protected String getExtensionBaseUrl() {
        return ConnectionPoolExtension.getBaseUrl();
    }

    @AfterAll
    void cleanup() {
        if (datasetUrl != null) {
            given().delete(datasetUrl);
        }
    }

    @Test
    @Order(1)
    @DisplayName("TC-POOL-001: Server starts and status endpoint works with connection pool")
    void statusEndpoint_worksWithPool() {
        given()
                .spec(jsonSpec)
        .when()
                .get("/status")
        .then()
                .statusCode(200)
                .body("service", equalTo("RowStore"));
    }

    @Test
    @Order(2)
    @DisplayName("TC-POOL-002: Dataset lifecycle works with connection pool")
    void datasetLifecycle_worksWithPool() {
        byte[] csvData = loadTestData(TEST_FILE);
        DatasetUrls urls = createDatasetAndWait(csvData);
        datasetUrl = urls.datasetUrl;
        infoUrl = urls.infoUrl;

        // Verify dataset is available
        given()
                .spec(jsonSpec)
        .when()
                .get(infoUrl)
        .then()
                .statusCode(200)
                .body("status", equalTo(ETL_STATUS_AVAILABLE));
    }

    @Test
    @Order(3)
    @DisplayName("TC-POOL-003: Query works with connection pool")
    void query_worksWithPool() {
        given()
                .spec(jsonSpec)
                .queryParam("Name", "Åkesson")
        .when()
                .get(datasetUrl)
        .then()
                .statusCode(200)
                .body("results", hasSize(1))
                .body("results[0].name", equalTo("Åkesson"));
    }

    @Test
    @Order(4)
    @DisplayName("TC-POOL-004: HikariCP metrics are available via actuator")
    void hikariMetrics_available() {
        Response response = given()
                .spec(jsonSpec)
        .when()
                .get("/actuator/metrics/hikaricp.connections");

        response.then()
                .statusCode(200)
                .body("name", equalTo("hikaricp.connections"));
    }

    @Test
    @Order(5)
    @DisplayName("TC-POOL-005: Pool max size matches configuration")
    void poolMaxSize_matchesConfig() {
        given()
                .spec(jsonSpec)
        .when()
                .get("/actuator/metrics/hikaricp.connections.max")
        .then()
                .statusCode(200)
                .body("measurements[0].value", greaterThanOrEqualTo(3.0f));
    }
}
