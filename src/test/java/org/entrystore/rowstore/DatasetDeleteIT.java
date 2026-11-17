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
import org.entrystore.rowstore.store.impl.PgRowStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration tests for deleting datasets (API-3.07, ETL-12): the dataset and its aliases are deleted right away,
 * its data table is dropped in the background once no reader holds it, and data tables and aliases left behind by
 * earlier deletes are removed at startup.
 * Specification: 11-etl-processing.md
 */
@DisplayName("Dataset Delete Tests")
class DatasetDeleteIT extends BaseIntegrationTest {

    /**
     * Rows of about 1 KB each, far more than the server's write buffer and the socket buffers can hold, so that an
     * export whose client pauses reading keeps its cursor on the data table open.
     */
    private static final int PADDING_ROWS = 50000;

    /**
     * Well below queryLockTimeout of the test server (15 s, half of connectionWaitTimeout), after which a DELETE
     * waiting for the lock of the data table got 503 before ROWS-174.
     */
    private static final Duration FAST = Duration.ofSeconds(5);

    /**
     * Longer than the lock timeout of a single attempt to lock the data table (3 seconds).
     */
    private static final Duration LONGER_THAN_SWAP_LOCK_TIMEOUT = Duration.ofSeconds(5);

    @Test
    @DisplayName("TC-ETL-075: A dataset whose data table is read by an export is deleted right away and its table dropped after the export")
    void deleteDuringExport_returns204AndDropsTableAfterExport() throws Exception {
        DatasetUrls urls = createDatasetAndWait(csv("A", 3));
        String dataTable = dataTable(urls.datasetId);
        try (Connection db = openDatabaseConnection()) {
            execute(db, "INSERT INTO " + dataTable + " (data) SELECT jsonb_build_object('name', 'pad' || g, " +
                    "'value', repeat('x', 1000)) FROM generate_series(1, " + PADDING_ROWS + ") g");
            String aliasName = "deleted" + UUID.randomUUID().toString().replace("-", "");
            setAlias(urls.datasetUrl, aliasName);

            HttpRequest request = HttpRequest.newBuilder(URI.create(urls.datasetUrl + "/export"))
                    .header("Accept", "application/json")
                    .build();
            try (HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()) {
                HttpResponse<InputStream> response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
                assertThat(response.statusCode()).isEqualTo(200);
                try (InputStream in = response.body()) {
                    byte[] start = in.readNBytes(1024);

                    long before = System.nanoTime();
                    given().delete(urls.datasetUrl).then().statusCode(204);
                    assertThat(Duration.ofNanos(System.nanoTime() - before)).isLessThan(FAST);

                    given().spec(jsonSpec).get(urls.datasetUrl).then().statusCode(404);
                    given().spec(jsonSpec).get(urls.infoUrl).then().statusCode(404);
                    given().spec(jsonSpec).get(urls.datasetUrl + "/aliases").then().statusCode(404);
                    given().spec(jsonSpec).get(urls.datasetUrl + "/export").then().statusCode(404);
                    given().spec(jsonSpec).get("/dataset/" + aliasName).then().statusCode(404);
                    given().contentType("text/csv").body(csv("B", 1)).put(urls.datasetUrl).then().statusCode(404);
                    // the drop waits for the export, beyond its first attempt to lock the data table (3 s)
                    Thread.sleep(LONGER_THAN_SWAP_LOCK_TIMEOUT.toMillis());
                    assertThat(tableExists(db, dataTable)).isTrue();

                    String body = new String(start, StandardCharsets.UTF_8) + new String(in.readAllBytes(), StandardCharsets.UTF_8);
                    assertThat(body.startsWith("[{") && body.endsWith("}]"))
                            .as("Export of %d bytes must be a complete JSON array, but ends with: %s",
                                    body.length(), body.substring(Math.max(0, body.length() - 40)))
                            .isTrue();
                    assertThat(body.split(",\n", -1).length).isEqualTo(3 + PADDING_ROWS);
                }
            }

            // the next attempt to lock the data table follows within the maximum delay between attempts (30 s)
            await().atMost(Duration.ofSeconds(60)).pollInterval(POLL_INTERVAL)
                    .until(() -> !tableExists(db, dataTable));
        }
    }

    @Test
    @DisplayName("TC-ETL-076: Deleting a dataset deletes its aliases, so that they can be set on another dataset")
    void deleteDataset_releasesAliases() {
        DatasetUrls first = createDatasetAndWait(csv("A", 1));
        DatasetUrls second = createDatasetAndWait(csv("B", 1));
        String aliasName = "reused" + UUID.randomUUID().toString().replace("-", "");
        try {
            setAlias(first.datasetUrl, aliasName);
            assertThat(given().spec(jsonSpec).get("/dataset/" + aliasName + "/info")
                    .then().statusCode(200).extract().jsonPath().getString("identifier")).isEqualTo(first.datasetId);

            deleteDatasetAndVerify(first.datasetUrl);
            given().spec(jsonSpec).get("/dataset/" + aliasName).then().statusCode(404);

            setAlias(second.datasetUrl, aliasName);
            assertThat(given().spec(jsonSpec).get("/dataset/" + aliasName + "/info")
                    .then().statusCode(200).extract().jsonPath().getString("identifier")).isEqualTo(second.datasetId);
        } finally {
            deleteDataset(second.datasetUrl);
        }
    }

    @Test
    @DisplayName("TC-ETL-077: Leftover data tables of deleted datasets are dropped at startup, tables of existing datasets are kept")
    void leftoverDataTables_areDroppedAtStartup() throws SQLException {
        DatasetUrls urls = createDatasetAndWait(csv("A", 3));
        String leftover = dataTable(UUID.randomUUID().toString());
        String otherTable = leftover + "_copy";
        // rows as 1.x may have stored them: without a data table name, and with the name in upper case, which 1.x
        // served, since PostgreSQL folds unquoted identifiers to lower case
        String nullNameId = UUID.randomUUID().toString();
        String upperCaseNameId = UUID.randomUUID().toString();
        try (Connection db = openDatabaseConnection()) {
            execute(db, "CREATE TABLE " + leftover + " (rownr BIGSERIAL PRIMARY KEY, data JSONB NOT NULL)");
            execute(db, "CREATE TABLE " + otherTable + " (id INT)");
            execute(db, "INSERT INTO datasets VALUES ('" + nullNameId + "', 3, now(), NULL)");
            execute(db, "CREATE TABLE " + dataTable(nullNameId) + " (rownr SERIAL PRIMARY KEY, data JSONB NOT NULL)");
            execute(db, "INSERT INTO datasets VALUES ('" + upperCaseNameId + "', 3, now(), '" +
                    dataTable(upperCaseNameId).toUpperCase() + "')");
            execute(db, "CREATE TABLE " + dataTable(upperCaseNameId) + " (rownr SERIAL PRIMARY KEY, data JSONB NOT NULL)");
            try {
                // invoked by PgRowStore's constructor at startup
                List<String> dropped = RowStoreTestEnvironment.getInstance().getBean(PgRowStore.class)
                        .dropLeftoverDataTables();

                assertThat(dropped).contains(leftover).doesNotContain(otherTable, dataTable(urls.datasetId),
                        dataTable(nullNameId), dataTable(upperCaseNameId));
                assertThat(tableExists(db, leftover)).isFalse();
                assertThat(tableExists(db, otherTable)).isTrue();
                assertThat(tableExists(db, dataTable(nullNameId))).isTrue();
                assertThat(tableExists(db, dataTable(upperCaseNameId))).isTrue();
                assertThat(given().spec(jsonSpec).get(urls.datasetUrl).then().statusCode(200)
                        .extract().jsonPath().getList("results.name", String.class)).containsExactly("A1", "A2", "A3");
            } finally {
                execute(db, "DELETE FROM datasets WHERE id IN ('" + nullNameId + "', '" + upperCaseNameId + "')");
                execute(db, "DROP TABLE IF EXISTS " + leftover);
                execute(db, "DROP TABLE IF EXISTS " + otherTable);
                execute(db, "DROP TABLE IF EXISTS " + dataTable(nullNameId));
                execute(db, "DROP TABLE IF EXISTS " + dataTable(upperCaseNameId));
            }
        } finally {
            deleteDataset(urls.datasetUrl);
        }
    }

    @Test
    @DisplayName("TC-ETL-082: A leftover table that cannot be dropped at startup is skipped, the other leftover tables are dropped")
    void leftoverTableThatCannotBeDropped_isSkipped_othersAreDropped() throws SQLException {
        PgRowStore rowStore = RowStoreTestEnvironment.getInstance().getBean(PgRowStore.class);
        String blocked = dataTable(UUID.randomUUID().toString());
        String leftover = dataTable(UUID.randomUUID().toString());
        String blockedShadow = dataTable(UUID.randomUUID().toString()) + "_new";
        String leftoverShadow = dataTable(UUID.randomUUID().toString()) + "_new";
        List<String> tables = List.of(blocked, leftover, blockedShadow, leftoverShadow);
        try (Connection db = openDatabaseConnection()) {
            for (String table : tables) {
                execute(db, "CREATE TABLE " + table + " (rownr BIGSERIAL PRIMARY KEY, data JSONB NOT NULL)");
            }
            // a dependent view makes DROP TABLE fail (2BP01)
            execute(db, "CREATE VIEW " + blocked + "_view AS SELECT * FROM " + blocked);
            execute(db, "CREATE VIEW " + blockedShadow + "_view AS SELECT * FROM " + blockedShadow);
            try {
                // invoked by PgRowStore's constructor at startup
                assertThat(rowStore.dropLeftoverShadowTables()).contains(leftoverShadow).doesNotContain(blockedShadow);
                assertThat(rowStore.dropLeftoverDataTables()).contains(leftover).doesNotContain(blocked);

                assertThat(tableExists(db, leftover)).isFalse();
                assertThat(tableExists(db, leftoverShadow)).isFalse();
                assertThat(tableExists(db, blocked)).isTrue();
                assertThat(tableExists(db, blockedShadow)).isTrue();
            } finally {
                execute(db, "DROP VIEW IF EXISTS " + blocked + "_view");
                execute(db, "DROP VIEW IF EXISTS " + blockedShadow + "_view");
                for (String table : tables) {
                    execute(db, "DROP TABLE IF EXISTS " + table);
                }
            }
        }
    }

    @Test
    @DisplayName("TC-ETL-078: The upgrade of a 1.x database removes aliases and data tables of deleted datasets")
    void upgradeOf1xDatabase_removesLeftoversOfDeletedDatasets() throws Exception {
        RowStoreTestEnvironment env = RowStoreTestEnvironment.getInstance();
        // skipped against an external server, like other tests with database access
        openDatabaseConnection().close();
        String database = "rowstore_delete_upgrade";
        String existingId = UUID.randomUUID().toString();
        String deletedId = UUID.randomUUID().toString();
        // a separate database, so that the start does not affect the shared RowStore server
        executeMaintenance("DROP DATABASE IF EXISTS " + database);
        executeMaintenance("CREATE DATABASE " + database);
        try {
            try (Connection db = env.openDatabaseConnection(database)) {
                // the schema of 1.x, without Flyway's history
                execute(db, "CREATE TABLE datasets (id UUID PRIMARY KEY, status INT NOT NULL, created TIMESTAMP NOT NULL, data_table CHAR(37))");
                execute(db, "CREATE TABLE aliases (id SERIAL PRIMARY KEY, dataset_id UUID NOT NULL, alias TEXT NOT NULL)");
                execute(db, "INSERT INTO datasets VALUES ('" + existingId + "', 3, now(), '" + dataTable(existingId) + "')");
                execute(db, "CREATE TABLE " + dataTable(existingId) + " (rownr SERIAL PRIMARY KEY, data JSONB NOT NULL)");
                execute(db, "INSERT INTO " + dataTable(existingId) + " (data) VALUES ('{\"name\": \"A1\"}')");
                execute(db, "INSERT INTO aliases (dataset_id, alias) VALUES ('" + existingId + "', 'kept')");
                // left behind by a delete in 1.x
                execute(db, "INSERT INTO aliases (dataset_id, alias) VALUES ('" + deletedId + "', 'orphan')");
                // left behind by a delete in 2.0 whose background drop did not happen
                execute(db, "CREATE TABLE " + dataTable(deletedId) + " (rownr BIGSERIAL PRIMARY KEY, data JSONB NOT NULL)");
            }

            int port = RowStoreTestEnvironment.findAvailablePort();
            try (ConfigurableApplicationContext ignored = env.startAdditionalRowStore(database, port);
                 Connection db = env.openDatabaseConnection(database)) {
                assertThat(queryStrings(db, "SELECT alias || ':' || dataset_id FROM aliases"))
                        .containsExactly("kept:" + existingId);
                assertThat(tableExists(db, dataTable(deletedId))).isFalse();
                assertThat(tableExists(db, dataTable(existingId))).isTrue();

                String baseUrl = "http://localhost:" + port;
                given().get(baseUrl + "/dataset/orphan").then().statusCode(404);
                given().contentType(ContentType.JSON).body("[\"kept\", \"orphan\"]")
                        .put(baseUrl + "/dataset/" + existingId + "/aliases").then().statusCode(204);
                assertThat(given().accept(ContentType.JSON).get(baseUrl + "/dataset/orphan/json").then().statusCode(200)
                        .extract().jsonPath().getList("results.name", String.class)).containsExactly("A1");
            }
        } finally {
            // like DROP DATABASE ... WITH (FORCE), which requires PostgreSQL 13 (the tests also run with 9.6, DEPL-4.01)
            executeMaintenance("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '" + database + "'");
            executeMaintenance("DROP DATABASE IF EXISTS " + database);
        }
    }

    private static void setAlias(String datasetUrl, String aliasName) {
        given().contentType(ContentType.JSON).body("[\"" + aliasName + "\"]")
                .put(datasetUrl + "/aliases").then().statusCode(204);
    }

    private static String dataTable(String datasetId) {
        return "data_" + datasetId.replace("-", "");
    }

    private static byte[] csv(String prefix, int rows) {
        StringBuilder sb = new StringBuilder("name,value\n");
        for (int i = 1; i <= rows; i++) {
            sb.append(prefix).append(i).append(',').append(i).append('\n');
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static void execute(Connection conn, String sql) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    private static void executeMaintenance(String sql) throws SQLException {
        try (Connection conn = RowStoreTestEnvironment.getInstance().openMaintenanceDatabaseConnection()) {
            execute(conn, sql);
        }
    }

    private static List<String> queryStrings(Connection conn, String sql) throws SQLException {
        List<String> result = new ArrayList<>();
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                result.add(rs.getString(1));
            }
        }
        return result;
    }

    private static boolean tableExists(Connection conn, String table) throws SQLException {
        return !queryStrings(conn, "SELECT relname FROM pg_class WHERE relname = '" + table + "'").isEmpty();
    }

}
