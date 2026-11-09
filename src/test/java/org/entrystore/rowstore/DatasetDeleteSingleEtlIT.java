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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test for the background drop of a deleted dataset's data table (ETL-12.01) with a single ETL permit
 * ({@code maxetlprocesses} 1): while the drop waits for a reader of the table, it holds the permit and a connection
 * only during its attempts (at most 3 seconds each), so that uploads of other datasets are loaded between them.
 * Specification: 11-etl-processing.md
 */
@DisplayName("Dataset Delete Tests with a single ETL permit")
class DatasetDeleteSingleEtlIT extends ConfigurableTestBase {

    private static final ConfigurableTestEnvironment ENV = ConfigurableTestEnvironment.builder()
            .maxEtlProcesses(1)
            .buildEnvironment();

    @Override
    protected String getExtensionBaseUrl() {
        ENV.start();
        return ENV.getBaseUrl();
    }

    @AfterAll
    void stopEnvironment() {
        ENV.stop();
    }

    @Test
    @DisplayName("TC-ETL-079: While the drop of a deleted dataset's table waits for a reader, uploads of other datasets are loaded")
    void dropWaitingForReader_doesNotBlockUploads() throws SQLException, InterruptedException {
        DatasetUrls deleted = createDatasetAndWait(csv("A", 3));
        DatasetUrls other = createDatasetAndWait(csv("B", 1));
        String dataTable = "data_" + deleted.datasetId.replace("-", "");
        try (Connection db = ENV.openDatabaseConnection(); Connection reader = ENV.openDatabaseConnection()) {
            // holds an ACCESS SHARE lock on the data table like a long streaming export or pg_dump
            reader.setAutoCommit(false);
            try (Statement stmt = reader.createStatement()) {
                stmt.executeQuery("SELECT data FROM " + dataTable).close();
            }
            try {
                given().delete(deleted.datasetUrl).then().statusCode(204);
                // the first attempt of the drop finds the data table in use and holds the only ETL permit while it
                // waits for the lock (at most 3 s), the upload gets the permit between the attempts
                Thread.sleep(2000);

                given().spec(csvSpec).body(csv("C", 2)).put(other.datasetUrl).then().statusCode(202);
                waitForDatasetAvailable(other.infoUrl);
                assertThat(given().spec(jsonSpec).get(other.datasetUrl).then().statusCode(200)
                        .extract().jsonPath().getList("results.name", String.class)).containsExactly("C1", "C2");
                assertThat(tableExists(db, dataTable)).isTrue();
            } finally {
                reader.rollback();
            }

            // the next attempt follows within the maximum delay between attempts (30 s)
            await().atMost(Duration.ofSeconds(60)).pollInterval(POLL_INTERVAL).until(() -> !tableExists(db, dataTable));
        } finally {
            deleteDataset(other.datasetUrl);
        }
    }

    private static byte[] csv(String prefix, int rows) {
        StringBuilder sb = new StringBuilder("name,value\n");
        for (int i = 1; i <= rows; i++) {
            sb.append(prefix).append(i).append(',').append(i).append('\n');
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static boolean tableExists(Connection conn, String table) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT 1 FROM pg_class WHERE relname = ?")) {
            ps.setString(1, table);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

}
