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

import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.ContentType;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;

import static io.restassured.RestAssured.given;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;

/**
 * Base class for RowStore integration tests.
 * Provides shared configuration and utility methods.
 *
 * Tests extending this class will automatically start a PostgreSQL container
 * and RowStore server via the {@link RowStoreExtension}.
 *
 * To run tests against an external RowStore instance instead of starting
 * a new one, set the system property {@code rowstore.baseUrl}:
 * <pre>
 * mvn verify -Drowstore.baseUrl=http://localhost:8282
 * </pre>
 */
@ExtendWith(RowStoreExtension.class)
public abstract class BaseIntegrationTest {

    protected static final int ETL_STATUS_CREATED = 0;
    protected static final int ETL_STATUS_ACCEPTED_DATA = 1;
    protected static final int ETL_STATUS_PROCESSING = 2;
    protected static final int ETL_STATUS_AVAILABLE = 3;
    protected static final int ETL_STATUS_ERROR = 4;

    protected static final Duration INITIAL_DELAY = Duration.ofSeconds(5);
    protected static final Duration POLL_INTERVAL = Duration.ofMillis(2500);
    protected static final Duration MAX_WAIT = Duration.ofSeconds(15);

    protected static RequestSpecification jsonSpec;
    protected static RequestSpecification csvSpec;

    @BeforeAll
    static void setupRestAssured() {
        String baseUrl = getBaseUrl();
        RestAssured.baseURI = baseUrl;

        jsonSpec = new RequestSpecBuilder()
                .setAccept(ContentType.JSON)
                .build();

        csvSpec = new RequestSpecBuilder()
                .setContentType("text/csv")
                .setAccept(ContentType.JSON)
                .build();
    }

    /**
     * Returns the base URL for the RowStore server.
     * Uses the system property if set, otherwise gets it from the test environment.
     */
    protected static String getBaseUrl() {
        String externalUrl = System.getProperty("rowstore.baseUrl");
        if (externalUrl != null && !externalUrl.isEmpty()) {
            return externalUrl;
        }
        return RowStoreTestEnvironment.getInstance().getBaseUrl();
    }

    /**
     * Loads a test data file from the classpath.
     *
     * @param filename the name of the file in the data directory
     * @return the file contents as a byte array
     */
    protected byte[] loadTestData(String filename) {
        try (InputStream is = getClass().getResourceAsStream("/data/" + filename)) {
            if (is == null) {
                throw new RuntimeException("Test data file not found: " + filename);
            }
            return is.readAllBytes();
        } catch (IOException e) {
            throw new RuntimeException("Failed to load test data: " + filename, e);
        }
    }

    /**
     * Creates a new dataset from a CSV file and returns the response.
     *
     * @param csvData the CSV data as bytes
     * @return the response containing dataset info
     */
    protected Response createDataset(byte[] csvData) {
        return given()
                .spec(csvSpec)
                .body(csvData)
                .when()
                .post("/datasets");
    }

    /**
     * Waits for a dataset to reach the AVAILABLE status.
     *
     * @param infoUrl the URL to the dataset's info endpoint
     */
    protected void waitForDatasetAvailable(String infoUrl) {
        await()
                .atMost(MAX_WAIT)
                .pollInterval(POLL_INTERVAL)
                .untilAsserted(() ->
                        given()
                                .spec(jsonSpec)
                                .get(infoUrl)
                                .then()
                                .body("status", equalTo(ETL_STATUS_AVAILABLE))
                );
    }

    /**
     * Waits for a dataset to reach the ERROR status.
     *
     * @param infoUrl the URL to the dataset's info endpoint
     */
    protected void waitForDatasetError(String infoUrl) {
        await()
                .atMost(MAX_WAIT)
                .pollInterval(POLL_INTERVAL)
                .untilAsserted(() ->
                        given()
                                .spec(jsonSpec)
                                .get(infoUrl)
                                .then()
                                .body("status", equalTo(ETL_STATUS_ERROR))
                );
    }

    /**
     * Extracts the dataset URL from a creation response.
     *
     * @param response the creation response
     * @return the dataset URL
     */
    protected String getDatasetUrl(Response response) {
        return response.jsonPath().getString("url");
    }

    /**
     * Extracts the info URL from a creation response.
     *
     * @param response the creation response
     * @return the info URL
     */
    protected String getInfoUrl(Response response) {
        return response.jsonPath().getString("info");
    }

    /**
     * Deletes a dataset.
     *
     * @param datasetUrl the dataset URL
     */
    protected void deleteDataset(String datasetUrl) {
        given()
                .delete(datasetUrl)
                .then()
                .statusCode(204);
    }

}
