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

import org.awaitility.core.ConditionTimeoutException;
import org.entrystore.rowstore.store.impl.PgRowStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration tests for replacing the data of a dataset (PUT): the new data is loaded into a shadow table that
 * replaces the data table only if the load succeeded, so that the previous data is served until then.
 * Specification: 11-etl-processing.md
 */
@DisplayName("Dataset Replace Tests")
class DatasetReplaceIT extends BaseIntegrationTest {

    /**
     * Longer than the lock timeout of a single attempt to replace the data table (3 seconds).
     */
    private static final Duration LONGER_THAN_SWAP_LOCK_TIMEOUT = Duration.ofSeconds(5);

    @Test
    @DisplayName("TC-ETL-019: A replace that fails on a corrupt row late in the file keeps the previous data")
    void replaceWithCorruptRow_keepsPreviousData() {
        DatasetUrls urls = createDatasetAndWait(csv("A", 3));
        try {
            StringBuilder corrupt = new StringBuilder(new String(csv("B", 1000), StandardCharsets.UTF_8));
            corrupt.append("B1001,1001,too many values\n");
            given().spec(csvSpec).body(corrupt.toString()).put(urls.datasetUrl).then().statusCode(202);

            waitForDatasetError(urls.infoUrl);
            assertThat(names(urls.datasetUrl)).containsExactly("A1", "A2", "A3");
            assertThat(rowCount(urls.infoUrl)).isEqualTo(3);
        } finally {
            deleteDataset(urls.datasetUrl);
        }
    }

    @Test
    @DisplayName("TC-ETL-020: A replace whose database connection is lost keeps the previous data")
    void replaceWithLostConnection_keepsPreviousData() throws SQLException {
        DatasetUrls urls = createDatasetAndWait(csv("A", 3));
        String shadowTable = dataTable(urls) + "_new";
        try (Connection db = openDatabaseConnection(); Connection blocker = openDatabaseConnection()) {
            blockShadowTableCreation(blocker, shadowTable);
            given().spec(csvSpec).body(csv("B", 2)).put(urls.datasetUrl).then().statusCode(202);
            int loaderPid = await().atMost(MAX_WAIT)
                    .until(() -> waitingBackendPid(db, "CREATE TABLE IF NOT EXISTS " + shadowTable), pid -> pid > 0);

            execute(db, "SELECT pg_terminate_backend(" + loaderPid + ")");
            blocker.rollback();

            waitForDatasetError(urls.infoUrl);
            assertThat(names(urls.datasetUrl)).containsExactly("A1", "A2", "A3");

            // nothing is left behind that prevents a subsequent replace
            given().spec(csvSpec).body(csv("C", 2)).put(urls.datasetUrl).then().statusCode(202);
            waitForDatasetAvailable(urls.infoUrl);
            assertThat(names(urls.datasetUrl)).containsExactly("C1", "C2");
        } finally {
            awaitLoadFinishedAndDelete(urls);
        }
    }

    @Test
    @DisplayName("TC-ETL-021: Queries and exports during a replace return the previous data")
    void queriesDuringReplace_returnPreviousData() throws SQLException {
        DatasetUrls urls = createDatasetAndWait(csv("A", 3));
        String shadowTable = dataTable(urls) + "_new";
        try (Connection db = openDatabaseConnection(); Connection blocker = openDatabaseConnection()) {
            // the replace waits for the creation of its shadow table, i.e., after it has started
            blockShadowTableCreation(blocker, shadowTable);
            given().spec(csvSpec).body(csv("B", 2)).put(urls.datasetUrl).then().statusCode(202);
            await().atMost(MAX_WAIT)
                    .until(() -> waitingBackendPid(db, "CREATE TABLE IF NOT EXISTS " + shadowTable), pid -> pid > 0);

            try {
                assertThat(status(urls.infoUrl)).isEqualTo(ETL_STATUS_PROCESSING);
                assertThat(names(urls.datasetUrl)).containsExactly("A1", "A2", "A3");
                assertThat(rowCount(urls.infoUrl)).isEqualTo(3);
                assertThat(given().spec(jsonSpec).get(urls.datasetUrl + "/export")
                        .then().statusCode(200)
                        .extract().jsonPath().getList("name", String.class))
                        .containsExactlyInAnyOrder("A1", "A2", "A3");
            } finally {
                blocker.rollback();
            }

            waitForDatasetAvailable(urls.infoUrl);
            assertThat(names(urls.datasetUrl)).containsExactly("B1", "B2");
        } finally {
            awaitLoadFinishedAndDelete(urls);
        }
    }

    @Test
    @DisplayName("TC-ETL-022: Consecutive replaces succeed and leave the data table with canonical names")
    void consecutiveReplaces_leaveCanonicalCatalogState() throws SQLException {
        DatasetUrls urls = createDatasetAndWait(csv("A", 3));
        String dataTable = dataTable(urls);
        try (Connection db = openDatabaseConnection()) {
            // a data table as created before 2.0, with a 32-bit row number
            execute(db, "DROP TABLE " + dataTable);
            execute(db, "CREATE TABLE " + dataTable + " (rownr SERIAL PRIMARY KEY, data JSONB NOT NULL)");
            execute(db, "INSERT INTO " + dataTable + " (data) VALUES ('{\"name\": \"S1\", \"value\": \"1\"}')");

            for (String prefix : List.of("B", "C", "D")) {
                given().spec(csvSpec).body(csv(prefix, 2)).put(urls.datasetUrl).then().statusCode(202);
                waitForDatasetAvailable(urls.infoUrl);
                assertThat(names(urls.datasetUrl)).containsExactly(prefix + "1", prefix + "2");
            }

            assertThat(queryStrings(db, "SELECT relname FROM pg_class WHERE relname LIKE '" + dataTable + "%' " +
                    "AND relkind IN ('r', 'p') ORDER BY relname")).containsExactly(dataTable);
            assertThat(queryStrings(db, "SELECT indexname FROM pg_indexes WHERE tablename = '" + dataTable + "'"))
                    .containsExactlyInAnyOrder(dataTable + "_pkey",
                            dataTable + "_jsonidx_" + md5("name").substring(0, 8),
                            dataTable + "_jsonidx_" + md5("value").substring(0, 8));
            assertThat(queryStrings(db, "SELECT conname FROM pg_constraint WHERE conrelid = '" + dataTable +
                    "'::regclass AND contype = 'p'")).containsExactly(dataTable + "_pkey");
            assertThat(queryStrings(db, "SELECT pg_get_serial_sequence('" + dataTable + "', 'rownr')"))
                    .containsExactly("public." + dataTable + "_rownr_seq");
            assertThat(queryStrings(db, "SELECT data_type FROM information_schema.columns WHERE table_name = '" +
                    dataTable + "' AND column_name = 'rownr'")).containsExactly("bigint");
            assertThat(queryStrings(db, "SELECT data_type FROM information_schema.sequences WHERE sequence_name = '" +
                    dataTable + "_rownr_seq'")).containsExactly("bigint");
        } finally {
            deleteDataset(urls.datasetUrl);
        }
    }

    @Test
    @DisplayName("TC-ETL-023: A replace waits for a reader of the data table, e.g., an export, and then succeeds")
    void replaceDuringRead_isDeferredAndSucceeds() throws SQLException, InterruptedException {
        DatasetUrls urls = createDatasetAndWait(csv("A", 3));
        String dataTable = dataTable(urls);
        try (Connection db = openDatabaseConnection(); Connection reader = openDatabaseConnection()) {
            // holds an ACCESS SHARE lock on the data table like a streaming export
            reader.setAutoCommit(false);
            queryStrings(reader, "SELECT data->>'name' FROM " + dataTable);

            given().spec(csvSpec).body(csv("B", 2)).put(urls.datasetUrl).then().statusCode(202);
            try {
                await().atMost(MAX_WAIT).until(() -> waitingBackendPid(db,
                        "LOCK TABLE " + dataTable + " IN ACCESS EXCLUSIVE MODE"), pid -> pid > 0);
                // the first attempt to replace the data table times out meanwhile, the load must not fail
                Thread.sleep(LONGER_THAN_SWAP_LOCK_TIMEOUT.toMillis());

                assertThat(queryStrings(db, "SELECT status FROM datasets WHERE id = '" + urls.datasetId + "'"))
                        .containsExactly(String.valueOf(ETL_STATUS_PROCESSING));
                assertThat(names(urls.datasetUrl)).containsExactly("A1", "A2", "A3");
            } finally {
                reader.rollback();
            }

            waitForDatasetAvailable(urls.infoUrl);
            assertThat(names(urls.datasetUrl)).containsExactly("B1", "B2");
        } finally {
            awaitLoadFinishedAndDelete(urls);
        }
    }

    @Test
    @DisplayName("TC-ETL-024: A leftover shadow table is dropped at the start of a replace")
    void leftoverShadowTable_isDroppedByReplace() throws SQLException {
        DatasetUrls urls = createDatasetAndWait(csv("A", 3));
        String shadowTable = dataTable(urls) + "_new";
        try (Connection db = openDatabaseConnection()) {
            createLeftoverShadowTable(db, shadowTable);

            given().spec(csvSpec).body(csv("B", 2)).put(urls.datasetUrl).then().statusCode(202);

            waitForDatasetAvailable(urls.infoUrl);
            assertThat(names(urls.datasetUrl)).containsExactly("B1", "B2");
            assertThat(tableExists(db, shadowTable)).isFalse();
        } finally {
            deleteDataset(urls.datasetUrl);
        }
    }

    @Test
    @DisplayName("TC-ETL-025: Leftover shadow tables are dropped at startup")
    void leftoverShadowTables_areDroppedAtStartup() throws SQLException {
        DatasetUrls urls = createDatasetAndWait(csv("A", 3));
        String shadowTable = dataTable(urls) + "_new";
        String otherTable = dataTable(urls) + "_newer";
        try (Connection db = openDatabaseConnection()) {
            createLeftoverShadowTable(db, shadowTable);
            execute(db, "CREATE TABLE " + otherTable + " (id INT)");
            try {
                // invoked by PgRowStore's constructor at startup
                List<String> dropped = RowStoreTestEnvironment.getInstance().getBean(PgRowStore.class)
                        .dropLeftoverShadowTables();

                assertThat(dropped).contains(shadowTable).doesNotContain(otherTable, dataTable(urls));
                assertThat(tableExists(db, shadowTable)).isFalse();
                assertThat(tableExists(db, otherTable)).isTrue();
                assertThat(names(urls.datasetUrl)).containsExactly("A1", "A2", "A3");
            } finally {
                execute(db, "DROP TABLE IF EXISTS " + otherTable);
            }
        } finally {
            deleteDataset(urls.datasetUrl);
        }
    }

    @Test
    @DisplayName("TC-ETL-026: Deleting a dataset drops a leftover shadow table")
    void deleteDataset_dropsLeftoverShadowTable() throws SQLException {
        DatasetUrls urls = createDatasetAndWait(csv("A", 3));
        String shadowTable = dataTable(urls) + "_new";
        try (Connection db = openDatabaseConnection()) {
            createLeftoverShadowTable(db, shadowTable);

            deleteDatasetAndVerify(urls.datasetUrl);

            assertThat(tableExists(db, shadowTable)).isFalse();
            assertThat(tableExists(db, dataTable(urls))).isFalse();
        }
    }

    /**
     * Creates the shadow table in an open transaction, so that the creation of the same table by a load waits
     * until the transaction is rolled back.
     */
    private static void blockShadowTableCreation(Connection blocker, String shadowTable) throws SQLException {
        blocker.setAutoCommit(false);
        execute(blocker, "CREATE TABLE " + shadowTable + " (rownr BIGSERIAL PRIMARY KEY, data JSONB NOT NULL)");
    }

    private static void createLeftoverShadowTable(Connection db, String shadowTable) throws SQLException {
        execute(db, "CREATE TABLE " + shadowTable + " (rownr BIGSERIAL PRIMARY KEY, data JSONB NOT NULL)");
        execute(db, "INSERT INTO " + shadowTable + " (data) VALUES ('{\"name\": \"leftover\", \"value\": \"0\"}')");
    }

    /**
     * Deletes a dataset once it is deletable, so that a failure of the test is not masked by a failing cleanup.
     */
    private void awaitLoadFinishedAndDelete(DatasetUrls urls) throws SQLException {
        try {
            await().atMost(Duration.ofSeconds(30)).pollInterval(POLL_INTERVAL).until(() -> {
                int status = status(urls.infoUrl);
                return status == ETL_STATUS_AVAILABLE || status == ETL_STATUS_ERROR;
            });
        } catch (ConditionTimeoutException e) {
            try (Connection db = openDatabaseConnection()) {
                execute(db, "UPDATE datasets SET status = " + ETL_STATUS_ERROR + " WHERE id = '" + urls.datasetId + "'");
            }
        }
        deleteDataset(urls.datasetUrl);
    }

    private static String dataTable(DatasetUrls urls) {
        return "data_" + urls.datasetId.replace("-", "");
    }

    private static byte[] csv(String prefix, int rows) {
        StringBuilder sb = new StringBuilder("name,value\n");
        for (int i = 1; i <= rows; i++) {
            sb.append(prefix).append(i).append(',').append(i).append('\n');
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static List<String> names(String datasetUrl) {
        return given().spec(jsonSpec)
                .queryParam("_limit", 100)
                .get(datasetUrl)
                .then().statusCode(200)
                .extract().jsonPath().getList("results.name", String.class);
    }

    private static int status(String infoUrl) {
        return given().spec(jsonSpec).get(infoUrl)
                .then().statusCode(200)
                .extract().jsonPath().getInt("status");
    }

    private static int rowCount(String infoUrl) {
        return given().spec(jsonSpec).get(infoUrl)
                .then().statusCode(200)
                .extract().jsonPath().getInt("rowcount");
    }

    private static void execute(Connection conn, String sql) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
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

    /**
     * @return The PID of a backend whose statement starts with the given text and waits for a lock, or 0 if there
     * is none.
     */
    private static int waitingBackendPid(Connection conn, String statementStart) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT pid FROM pg_stat_activity WHERE datname = current_database() " +
                        "AND wait_event_type = 'Lock' AND starts_with(query, ?)")) {
            ps.setString(1, statementStart);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getInt(1) : 0;
            }
        }
    }

    private static String md5(String s) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("MD5").digest(s.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

}
