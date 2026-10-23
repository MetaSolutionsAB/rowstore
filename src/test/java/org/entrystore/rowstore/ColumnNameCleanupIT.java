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

import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import org.entrystore.rowstore.store.impl.PgRowStore;
import org.entrystore.rowstore.util.Hashing;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;

/**
 * Integration tests for the cleanup of column names stored before 2.0 with a byte order mark or soft hyphens.
 * Such datasets are simulated by rewriting the stored keys of a dataset via JDBC and renaming its indexes to the
 * names RowStore would have given them for the broken names.
 * Specification: 11-etl-processing.md (ETL-11)
 */
@DisplayName("Column Name Cleanup Tests")
class ColumnNameCleanupIT extends BaseIntegrationTest {

    private static final String BOM = "\uFEFF";

    private static final String SHY = "\u00AD";

    /**
     * Broken names as stored by 1.x for the header {@code <BOM>"kopare_id",na<SHY>mn,ort}, by clean name.
     */
    private static final Map<String, String> BROKEN_NAMES = Map.of(
            "kopare_id", BOM + "\"kopare_id\"",
            "namn", "na" + SHY + "mn");

    private static final String CSV = "kopare_id,namn,ort\n1,Anna,Göteborg\n2,Bo,Mölndal\n";

    private static final DatabaseConnector TEST_DATABASE = BaseIntegrationTest::openDatabaseConnection;

    @Test
    @DisplayName("TC-ETL-031: The cleanup renames broken column names and their indexes and keeps the data and the status")
    void cleanup_renamesColumnsAndIndexes() throws Exception {
        DatasetUrls urls = createDatasetAndWait(bytes(CSV));
        try {
            breakColumnNames(TEST_DATABASE, urls.datasetId, BROKEN_NAMES);
            assertThat(storedColumnNames(TEST_DATABASE, urls.datasetId))
                    .containsExactlyInAnyOrder(BOM + "\"kopare_id\"", "na" + SHY + "mn", "ort");
            assertThat(indexNames(TEST_DATABASE, urls.datasetId)).isEqualTo(
                    expectedIndexNames(urls.datasetId, BOM + "\"kopare_id\"", "na" + SHY + "mn", "ort"));

            assertThat(rowStore().cleanColumnNames()).contains(urls.datasetId);

            given().spec(jsonSpec).get(urls.infoUrl).then()
                    .body("status", equalTo(ETL_STATUS_AVAILABLE))
                    .body("rowcount", equalTo(2));
            assertClean(TEST_DATABASE, urls.datasetId);
            assertQueriesUseCleanNames(urls);
        } finally {
            deleteDataset(urls.datasetUrl);
        }
    }

    @Test
    @DisplayName("TC-ETL-032: A repeated cleanup changes nothing")
    void repeatedCleanup_changesNothing() throws Exception {
        DatasetUrls urls = createDatasetAndWait(bytes(CSV));
        try {
            breakColumnNames(TEST_DATABASE, urls.datasetId, BROKEN_NAMES);
            assertThat(rowStore().cleanColumnNames()).contains(urls.datasetId);
            String rowVersions = rowVersions(urls.datasetId);

            assertThat(rowStore().cleanColumnNames()).doesNotContain(urls.datasetId);

            assertThat(rowVersions(urls.datasetId)).isEqualTo(rowVersions);
            assertClean(TEST_DATABASE, urls.datasetId);
        } finally {
            deleteDataset(urls.datasetUrl);
        }
    }

    @Test
    @DisplayName("TC-ETL-033: A dataset whose cleaned column names would collide is skipped, other datasets are cleaned")
    void collidingNames_datasetSkipped() throws Exception {
        DatasetUrls colliding = createDatasetAndWait(bytes(CSV));
        DatasetUrls other = createDatasetAndWait(bytes(CSV));
        try {
            // <BOM>"namn" would become namn, which exists
            breakColumnNames(TEST_DATABASE, colliding.datasetId, Map.of("ort", BOM + "\"namn\""));
            breakColumnNames(TEST_DATABASE, other.datasetId, BROKEN_NAMES);
            String rowVersions = rowVersions(colliding.datasetId);

            List<String> cleaned = rowStore().cleanColumnNames();

            assertThat(cleaned).contains(other.datasetId).doesNotContain(colliding.datasetId);
            assertThat(rowVersions(colliding.datasetId)).isEqualTo(rowVersions);
            assertThat(storedColumnNames(TEST_DATABASE, colliding.datasetId))
                    .containsExactlyInAnyOrder("kopare_id", "namn", BOM + "\"namn\"");
            assertThat(indexNames(TEST_DATABASE, colliding.datasetId))
                    .isEqualTo(expectedIndexNames(colliding.datasetId, "kopare_id", "namn", BOM + "\"namn\""));
            given().spec(jsonSpec).get(colliding.infoUrl).then().body("status", equalTo(ETL_STATUS_AVAILABLE));
            assertClean(TEST_DATABASE, other.datasetId);
        } finally {
            deleteDataset(colliding.datasetUrl);
            deleteDataset(other.datasetUrl);
        }
    }

    @Test
    @DisplayName("TC-ETL-034: An append to a dataset with broken column names renames the columns with the append")
    void appendBeforeCleanup_renamesColumnsAndLoads() throws Exception {
        DatasetUrls urls = createDatasetAndWait(bytes(CSV));
        try {
            breakColumnNames(TEST_DATABASE, urls.datasetId, BROKEN_NAMES);

            given().spec(csvSpec).body(bytes(BOM + "\"kopare_id\",namn,ort\n3,Cecilia,Partille\n"))
                    .post(urls.datasetUrl).then().statusCode(202);

            waitForDatasetAvailable(urls.infoUrl);
            given().spec(jsonSpec).get(urls.infoUrl).then().body("rowcount", equalTo(3));
            assertClean(TEST_DATABASE, urls.datasetId);
            assertQueriesUseCleanNames(urls);
            assertThat(rows(urls.datasetUrl, "kopare_id", "3"))
                    .containsExactly(Map.of("kopare_id", "3", "namn", "Cecilia", "ort", "Partille"));
            assertThat(rowStore().cleanColumnNames()).doesNotContain(urls.datasetId);
        } finally {
            deleteDataset(urls.datasetUrl);
        }
    }

    @Test
    @DisplayName("TC-ETL-035: The cleanup runs in the background after startup until a run has completed")
    void cleanup_runsAfterStartup() throws Exception {
        RowStoreTestEnvironment env = RowStoreTestEnvironment.getInstance();
        String database = "rowstore_cleanup_startup";
        DatabaseConnector connector = () -> env.openDatabaseConnection(database);
        // a separate database, so that the restarts do not affect the shared RowStore server
        executeMaintenance("DROP DATABASE IF EXISTS " + database);
        executeMaintenance("CREATE DATABASE " + database);
        try {
            String datasetId;
            int port = RowStoreTestEnvironment.findAvailablePort();
            try (ConfigurableApplicationContext ignored = env.startAdditionalRowStore(database, port)) {
                datasetId = TestUtils.createDatasetAndWait(bytes(CSV), spec(port, "text/csv"), spec(port, null)).datasetId;
                // the first start found nothing to clean and recorded the cleanup as completed
                await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(200)).until(() ->
                        cleanupCompleted(connector));
            }
            breakColumnNames(connector, datasetId, BROKEN_NAMES);
            // like a database of 1.x, which has no record of the cleanup
            execute(connector, "DELETE FROM maintenance_tasks");

            try (ConfigurableApplicationContext ignored = env.startAdditionalRowStore(database, RowStoreTestEnvironment.findAvailablePort())) {
                await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(200)).untilAsserted(() -> {
                    assertClean(connector, datasetId);
                    assertThat(cleanupCompleted(connector)).isTrue();
                });
            }

            // a completed cleanup is skipped at later startups
            breakColumnNames(connector, datasetId, BROKEN_NAMES);
            try (ConfigurableApplicationContext context = env.startAdditionalRowStore(database, RowStoreTestEnvironment.findAvailablePort())) {
                assertThat(context.getBean(PgRowStore.class).startColumnNameCleanup()).isNull();
                assertThat(storedColumnNames(connector, datasetId)).containsAll(BROKEN_NAMES.values());
            }
        } finally {
            // like DROP DATABASE ... WITH (FORCE), which requires PostgreSQL 13 (the tests also run with 9.6, DEPL-4.01)
            executeMaintenance("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '" + database + "'");
            executeMaintenance("DROP DATABASE IF EXISTS " + database);
        }
    }

    @Test
    @DisplayName("TC-ETL-052: A cleanup in which a dataset fails is not recorded as completed")
    void failedDataset_cleanupNotRecordedAsCompleted() throws Exception {
        DatasetUrls failing = createDatasetAndWait(bytes(CSV));
        String table = dataTable(failing.datasetId);
        try {
            execute(TEST_DATABASE, "DELETE FROM maintenance_tasks");
            // reading the column names of this dataset fails with an SQL error that is not a connection failure
            execute(TEST_DATABASE, "ALTER TABLE " + table + " RENAME COLUMN data TO renamed_data");
            rowStore().cleanColumnNames();
            assertThat(cleanupCompleted(TEST_DATABASE)).isFalse();

            execute(TEST_DATABASE, "ALTER TABLE " + table + " RENAME COLUMN renamed_data TO data");
            rowStore().cleanColumnNames();
            assertThat(cleanupCompleted(TEST_DATABASE)).isTrue();
        } finally {
            execute(TEST_DATABASE, "DO $$ BEGIN ALTER TABLE " + table + " RENAME COLUMN renamed_data TO data; " +
                    "EXCEPTION WHEN undefined_column THEN NULL; END $$");
            deleteDataset(failing.datasetUrl);
        }
    }

    @Test
    @DisplayName("TC-ETL-036: A dataset whose data table is missing is skipped without failing the cleanup")
    void missingDataTable_datasetSkipped() throws Exception {
        DatasetUrls missing = createDatasetAndWait(bytes(CSV));
        DatasetUrls other = createDatasetAndWait(bytes(CSV));
        try {
            breakColumnNames(TEST_DATABASE, other.datasetId, BROKEN_NAMES);
            try (Connection conn = openDatabaseConnection(); Statement stmt = conn.createStatement()) {
                stmt.execute("DROP TABLE " + dataTable(missing.datasetId));
            }

            List<String> cleaned = rowStore().cleanColumnNames();

            assertThat(cleaned).contains(other.datasetId).doesNotContain(missing.datasetId);
            assertClean(TEST_DATABASE, other.datasetId);
        } finally {
            try (Connection conn = openDatabaseConnection();
                 PreparedStatement ps = conn.prepareStatement("DELETE FROM datasets WHERE id = ?::uuid")) {
                ps.setString(1, missing.datasetId);
                ps.executeUpdate();
            }
            deleteDataset(other.datasetUrl);
        }
    }

    @Test
    @DisplayName("TC-ETL-037: The cleanup creates no index for a renamed column with values longer than 256 characters")
    void cleanup_skipsIndexForLargeValues() throws Exception {
        DatasetUrls urls = createDatasetAndWait(bytes("kopare_id,beskrivning\n1," + "x".repeat(300) + "\n2,kort\n"));
        try {
            assertThat(indexNames(TEST_DATABASE, urls.datasetId)).isEqualTo(expectedIndexNames(urls.datasetId, "kopare_id"));
            breakColumnNames(TEST_DATABASE, urls.datasetId, Map.of("beskrivning", BOM + "\"beskrivning\""));

            assertThat(rowStore().cleanColumnNames()).contains(urls.datasetId);

            assertThat(storedColumnNames(TEST_DATABASE, urls.datasetId)).containsExactlyInAnyOrder("kopare_id", "beskrivning");
            assertThat(indexNames(TEST_DATABASE, urls.datasetId)).isEqualTo(expectedIndexNames(urls.datasetId, "kopare_id"));
            assertThat(rows(urls.datasetUrl, "beskrivning", "kort")).containsExactly(Map.of("kopare_id", "2", "beskrivning", "kort"));
        } finally {
            deleteDataset(urls.datasetUrl);
        }
    }

    @Test
    @DisplayName("TC-ETL-045: A dataset with a row that contains both an old and a new name is skipped by the cleanup and fails an append")
    void collisionInLaterRow_datasetSkippedAndAppendFails() throws Exception {
        DatasetUrls urls = createDatasetAndWait(bytes("namn,ort\nfirst,1\nleft,2\n"));
        String table = dataTable(urls.datasetId);
        try {
            // as stored by 1.x for <BOM>namn,namn\nfirst\nleft,right: the first row only has the broken name
            try (Connection conn = openDatabaseConnection();
                 PreparedStatement first = conn.prepareStatement("UPDATE " + table + " SET data = jsonb_build_object(?::text, 'first') WHERE rownr = 1");
                 PreparedStatement second = conn.prepareStatement("UPDATE " + table + " SET data = jsonb_build_object(?::text, 'left', 'namn', 'right') WHERE rownr = 2")) {
                first.setString(1, BOM + "namn");
                first.executeUpdate();
                second.setString(1, BOM + "namn");
                second.executeUpdate();
            }
            String rowVersions = rowVersions(urls.datasetId);
            List<String> rows = queryList(TEST_DATABASE, "SELECT data::text FROM " + table + " ORDER BY rownr");

            assertThat(rowStore().cleanColumnNames()).doesNotContain(urls.datasetId);
            assertThat(rowVersions(urls.datasetId)).isEqualTo(rowVersions);

            given().spec(csvSpec).body(bytes("namn\nnew\n")).post(urls.datasetUrl).then().statusCode(202);
            waitForDatasetError(urls.infoUrl);
            assertThat(queryList(TEST_DATABASE, "SELECT data::text FROM " + table + " ORDER BY rownr")).isEqualTo(rows);
        } finally {
            deleteDataset(urls.datasetUrl);
        }
    }

    @Test
    @DisplayName("TC-ETL-046: Queries are served while the cleanup waits for a reader of the data table to drop the old indexes")
    void queriesServedWhileCleanupWaitsForReader() throws Exception {
        DatasetUrls urls = createDatasetAndWait(bytes(CSV));
        String table = dataTable(urls.datasetId);
        try (Connection db = openDatabaseConnection(); Connection reader = openDatabaseConnection()) {
            breakColumnNames(TEST_DATABASE, urls.datasetId, BROKEN_NAMES);
            // holds an ACCESS SHARE lock on the data table like a streaming export
            reader.setAutoCommit(false);
            try (Statement stmt = reader.createStatement()) {
                stmt.executeQuery("SELECT data FROM " + table).close();
            }
            CompletableFuture<List<String>> cleanup = CompletableFuture.supplyAsync(() -> rowStore().cleanColumnNames());
            try {
                await().atMost(MAX_WAIT).until(() -> waitingForLock(db, "LOCK TABLE " + table + " IN ACCESS EXCLUSIVE MODE"));
                // the first attempt to lock the data table times out meanwhile
                Thread.sleep(Duration.ofSeconds(4).toMillis());

                long before = System.currentTimeMillis();
                List<Map<String, Object>> rows = rows(urls.datasetUrl, "ort", "Göteborg");
                assertThat(System.currentTimeMillis() - before).isLessThan(Duration.ofSeconds(10).toMillis());
                // the renames are not committed yet
                assertThat(rows).containsExactly(Map.of(BOM + "\"kopare_id\"", "1", "na" + SHY + "mn", "Anna", "ort", "Göteborg"));
                assertThat(cleanup).isNotDone();
            } finally {
                reader.rollback();
            }
            assertThat(cleanup.get(30, TimeUnit.SECONDS)).contains(urls.datasetId);
            assertClean(TEST_DATABASE, urls.datasetId);
            assertQueriesUseCleanNames(urls);
        } finally {
            deleteDataset(urls.datasetUrl);
        }
    }

    @Test
    @DisplayName("TC-ETL-049: An index of a new name keeps its canonical name if an index of an old name had it, by the cleanup and by an append")
    void indexNameCollision_replacementIndexGetsCanonicalName() throws Exception {
        String oldName = "field_" + SHY + "11246";
        String newName = "field_100652";
        // index names only contain the first 8 hex digits of the MD5 of the column name
        assertThat(Hashing.md5(oldName).substring(0, 8)).isEqualTo(Hashing.md5(newName).substring(0, 8));
        Map<String, String> brokenNames = new LinkedHashMap<>();
        // in this order, so that the index of field_100652 is renamed before its name is taken
        brokenNames.put("field_100652", "field_" + SHY + "100652");
        brokenNames.put("field_11246", oldName);
        String csv = "field_11246,field_100652\na,b\nc,d\n";
        DatasetUrls cleaned = createDatasetAndWait(bytes(csv));
        DatasetUrls appended = createDatasetAndWait(bytes(csv));
        try {
            breakColumnNames(TEST_DATABASE, cleaned.datasetId, brokenNames);
            assertThat(indexNames(TEST_DATABASE, cleaned.datasetId))
                    .isEqualTo(expectedIndexNames(cleaned.datasetId, oldName, "field_" + SHY + "100652"));
            assertThat(rowStore().cleanColumnNames()).contains(cleaned.datasetId);

            // broken after the cleanup, renamed by the append
            breakColumnNames(TEST_DATABASE, appended.datasetId, brokenNames);
            given().spec(csvSpec).body(bytes("field_11246,field_100652\ne,f\n")).post(appended.datasetUrl).then().statusCode(202);
            waitForDatasetAvailable(appended.infoUrl);

            for (DatasetUrls urls : List.of(cleaned, appended)) {
                String table = dataTable(urls.datasetId);
                assertThat(storedColumnNames(TEST_DATABASE, urls.datasetId)).containsExactlyInAnyOrder("field_11246", "field_100652");
                assertThat(indexNames(TEST_DATABASE, urls.datasetId))
                        .isEqualTo(expectedIndexNames(urls.datasetId, "field_11246", "field_100652"));
                assertThat(queryList(TEST_DATABASE, "SELECT indexdef FROM pg_indexes WHERE indexname = '" +
                        indexName(table, newName) + "'")).singleElement().asString().contains("'field_100652'");
                assertThat(queryList(TEST_DATABASE, "SELECT indexdef FROM pg_indexes WHERE indexname = '" +
                        indexName(table, "field_11246") + "'")).singleElement().asString().contains("'field_11246'");
                assertThat(rows(urls.datasetUrl, "field_100652", "d")).containsExactly(Map.of("field_11246", "c", "field_100652", "d"));
            }
        } finally {
            deleteDataset(cleaned.datasetUrl);
            deleteDataset(appended.datasetUrl);
        }
    }

    private static boolean waitingForLock(Connection conn, String statementStart) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT count(*) > 0 FROM pg_stat_activity WHERE datname = current_database() " +
                        "AND wait_event_type = 'Lock' AND position(?::text in query) = 1")) {
            ps.setString(1, statementStart);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() && rs.getBoolean(1);
            }
        }
    }

    private static PgRowStore rowStore() {
        return RowStoreTestEnvironment.getInstance().getBean(PgRowStore.class);
    }

    /**
     * Asserts that the dataset created from {@link #CSV} has clean column names and indexes and intact data.
     */
    private static void assertClean(DatabaseConnector db, String datasetId) throws SQLException {
        assertThat(storedColumnNames(db, datasetId)).containsExactlyInAnyOrder("kopare_id", "namn", "ort");
        assertThat(indexNames(db, datasetId)).isEqualTo(expectedIndexNames(datasetId, "kopare_id", "namn", "ort"));
        assertThat(querySet(db, "SELECT data::text FROM " + dataTable(datasetId) + " WHERE rownr <= 2")).containsExactlyInAnyOrder(
                "{\"ort\": \"Göteborg\", \"namn\": \"Anna\", \"kopare_id\": \"1\"}",
                "{\"ort\": \"Mölndal\", \"namn\": \"Bo\", \"kopare_id\": \"2\"}");
    }

    private void assertQueriesUseCleanNames(DatasetUrls urls) {
        List<String> infoColumns = given().spec(jsonSpec).get(urls.infoUrl).then().statusCode(200)
                .extract().jsonPath().getList("columnnames", String.class);
        assertThat(infoColumns).containsExactlyInAnyOrder("kopare_id", "namn", "ort");
        assertThat(rows(urls.datasetUrl, "kopare_id", "1"))
                .containsExactly(Map.of("kopare_id", "1", "namn", "Anna", "ort", "Göteborg"));
        assertThat(rows(urls.datasetUrl, "namn", "Bo"))
                .containsExactly(Map.of("kopare_id", "2", "namn", "Bo", "ort", "Mölndal"));
    }

    private List<Map<String, Object>> rows(String datasetUrl, String column, String value) {
        return given().spec(jsonSpec)
                .queryParam(column, value)
                .get(datasetUrl)
                .then().statusCode(200)
                .extract().jsonPath().getList("results");
    }

    /**
     * Rewrites the stored keys of a dataset and renames their indexes to the names that RowStore would have created
     * for the broken names.
     *
     * @param brokenNames The broken names by clean name.
     */
    private static void breakColumnNames(DatabaseConnector db, String datasetId, Map<String, String> brokenNames) throws SQLException {
        String table = dataTable(datasetId);
        try (Connection conn = db.open()) {
            for (Map.Entry<String, String> e : brokenNames.entrySet()) {
                try (PreparedStatement ps = conn.prepareStatement("UPDATE " + table +
                        " SET data = (data - ?::text) || jsonb_build_object(?::text, data -> ?::text) WHERE data ?? ?::text")) {
                    ps.setString(1, e.getKey());
                    ps.setString(2, e.getValue());
                    ps.setString(3, e.getKey());
                    ps.setString(4, e.getKey());
                    ps.executeUpdate();
                }
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("ALTER INDEX IF EXISTS " + indexName(table, e.getKey()) + " RENAME TO " + indexName(table, e.getValue()));
                }
            }
        }
    }

    /**
     * @return Returns the system column xmin of all rows, which changes when a row is updated.
     */
    private static String rowVersions(String datasetId) throws SQLException {
        return String.join(",", queryList(TEST_DATABASE, "SELECT xmin::text FROM " + dataTable(datasetId) + " ORDER BY rownr"));
    }

    private static Set<String> storedColumnNames(DatabaseConnector db, String datasetId) throws SQLException {
        String table = dataTable(datasetId);
        return querySet(db, "SELECT jsonb_object_keys(data) FROM " + table +
                " WHERE rownr = (SELECT min(rownr) FROM " + table + ")");
    }

    private static Set<String> indexNames(DatabaseConnector db, String datasetId) throws SQLException {
        return querySet(db, "SELECT indexname FROM pg_indexes WHERE tablename = '" + dataTable(datasetId) +
                "' AND indexname LIKE '%\\_jsonidx\\_%'");
    }

    private static Set<String> expectedIndexNames(String datasetId, String... columns) {
        Set<String> result = new HashSet<>();
        for (String column : columns) {
            result.add(indexName(dataTable(datasetId), column));
        }
        return result;
    }

    private static Set<String> querySet(DatabaseConnector db, String sql) throws SQLException {
        return new HashSet<>(queryList(db, sql));
    }

    private static List<String> queryList(DatabaseConnector db, String sql) throws SQLException {
        List<String> result = new ArrayList<>();
        try (Connection conn = db.open();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                result.add(rs.getString(1));
            }
        }
        return result;
    }

    private static boolean cleanupCompleted(DatabaseConnector db) throws SQLException {
        return !queryList(db, "SELECT name FROM maintenance_tasks WHERE name = '" + PgRowStore.COLUMN_NAME_CLEANUP_TASK + "'").isEmpty();
    }

    private static void execute(DatabaseConnector db, String sql) throws SQLException {
        try (Connection conn = db.open(); Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    private static void executeMaintenance(String sql) throws SQLException {
        try (Connection conn = RowStoreTestEnvironment.getInstance().openMaintenanceDatabaseConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    private static RequestSpecification spec(int port, String contentType) {
        RequestSpecBuilder builder = new RequestSpecBuilder()
                .setBaseUri("http://localhost:" + port)
                .setAccept(ContentType.JSON);
        if (contentType != null) {
            builder.setContentType(contentType);
        }
        return builder.build();
    }

    private static String dataTable(String datasetId) {
        return "data_" + datasetId.replace("-", "");
    }

    private static String indexName(String table, String column) {
        return table + "_jsonidx_" + Hashing.md5(column).substring(0, 8);
    }

    private static byte[] bytes(String csv) {
        return csv.getBytes(StandardCharsets.UTF_8);
    }

    @FunctionalInterface
    private interface DatabaseConnector {
        Connection open() throws SQLException;
    }

}
