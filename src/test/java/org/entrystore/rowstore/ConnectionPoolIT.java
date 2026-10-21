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

import com.zaxxer.hikari.HikariDataSource;
import io.restassured.response.Response;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.context.ApplicationContext;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Integration tests verifying that HikariCP connection pooling works correctly
 * when explicitly enabled via connectionPoolMax in the database configuration.
 *
 * The default (no pool) path is covered by all other integration tests since
 * RowStoreTestEnvironment does not set connectionPoolMax.
 *
 * The environment configures a separate queryDatabase (same container), ssl=false and socketTimeout=60.
 * Tests TC-POOL-006 to TC-POOL-008 inspect the DataSource beans of the in-process server and are skipped
 * when running against an external instance.
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

    @Test
    @Order(6)
    @DisplayName("TC-POOL-006: SSL mode and driver timeouts are passed to the HikariCP pools")
    void hikariPools_haveDriverSettings() {
        for (HikariDataSource pool : List.of(primaryPool(), queryPool())) {
            assertThat(pool.getDataSourceProperties())
                    .as("data source properties of pool %s", pool.getPoolName())
                    .containsEntry("sslmode", "disable")
                    .containsEntry("connectTimeout", "5")
                    .containsEntry("loginTimeout", "5")
                    .containsEntry("socketTimeout", "60");
        }
    }

    @Test
    @Order(7)
    @DisplayName("TC-POOL-007: Query pool is read-only, also in auto-commit mode")
    void queryPool_isReadOnly() throws SQLException {
        HikariDataSource primaryPool = primaryPool();
        HikariDataSource queryPool = queryPool();
        assertThat(queryPool).isNotSameAs(primaryPool);

        assertThat(queryPool.getDataSourceProperties()).containsEntry("readOnlyMode", "always");
        assertThat(primaryPool.getDataSourceProperties()).doesNotContainKey("readOnlyMode");

        // readOnlyMode=always makes the driver run auto-commit statements in read-only transactions
        assertThat(querySingleValue(queryPool, "SHOW transaction_read_only")).isEqualTo("on");
        assertThat(querySingleValue(primaryPool, "SHOW transaction_read_only")).isEqualTo("off");
    }

    @Test
    @Order(8)
    @DisplayName("TC-POOL-008: Connections do not use SSL when ssl is false")
    void connections_doNotUseSsl() throws SQLException {
        // Note: the Testcontainers PostgreSQL image runs without SSL, so this cannot distinguish
        // sslmode=disable from sslmode=prefer; TC-POOL-006 asserts the configured mode.
        for (HikariDataSource pool : List.of(primaryPool(), queryPool())) {
            assertThat(querySingleValue(pool, "SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()"))
                    .as("SSL in use by pool %s", pool.getPoolName())
                    .isEqualTo("f");
        }
    }

    private HikariDataSource primaryPool() {
        return dataSourceBean("dataSource");
    }

    private HikariDataSource queryPool() {
        return dataSourceBean("queryDataSource");
    }

    private HikariDataSource dataSourceBean(String name) {
        ApplicationContext context = ConnectionPoolExtension.getApplicationContext();
        Assumptions.assumeTrue(context != null, "DataSource beans are only accessible for the in-process server");
        DataSource ds = context.getBean(name, DataSource.class);
        assertThat(ds).as("bean %s", name).isInstanceOf(HikariDataSource.class);
        return (HikariDataSource) ds;
    }

    private static String querySingleValue(DataSource ds, String sql) throws SQLException {
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            assertThat(rs.next()).as("result row for: %s", sql).isTrue();
            return rs.getString(1);
        }
    }
}
