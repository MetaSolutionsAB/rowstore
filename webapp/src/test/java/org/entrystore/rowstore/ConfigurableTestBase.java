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
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;

import static io.restassured.RestAssured.given;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;

/**
 * Base class for integration tests that use a configurable test environment
 * (e.g., regex-disabled, regex-simple, rate-limiting modes).
 *
 * Provides shared utilities similar to {@link BaseIntegrationTest} but allows
 * subclasses to specify their own extension's base URL.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ConfigurableTestBase {

    protected static final int ETL_STATUS_AVAILABLE = 3;

    protected static final Duration POLL_INTERVAL = Duration.ofMillis(2500);
    protected static final Duration MAX_WAIT = Duration.ofSeconds(15);

    protected RequestSpecification jsonSpec;
    protected RequestSpecification csvSpec;

    /**
     * Subclasses must implement this to return the base URL from their extension.
     */
    protected abstract String getExtensionBaseUrl();

    @BeforeAll
    void setupRestAssured() {
        String baseUrl = getExtensionBaseUrl();
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
     * Loads a test data file from the classpath.
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
     * Creates a new dataset from CSV data.
     */
    protected Response createDataset(byte[] csvData) {
        return given()
                .spec(csvSpec)
                .body(csvData)
                .when()
                .post("/datasets");
    }

    /**
     * Extracts the dataset URL from a creation response.
     */
    protected String getDatasetUrl(Response response) {
        return response.jsonPath().getString("url");
    }

    /**
     * Extracts the info URL from a creation response.
     */
    protected String getInfoUrl(Response response) {
        return response.jsonPath().getString("info");
    }

    /**
     * Waits for a dataset to become available.
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
     * Creates a dataset and waits for it to become available.
     */
    protected DatasetUrls createDatasetAndWait(byte[] csvData) {
        Response response = createDataset(csvData);
        response.then().statusCode(202);

        String datasetUrl = getDatasetUrl(response);
        String infoUrl = getInfoUrl(response);
        String datasetId = response.jsonPath().getString("id");

        waitForDatasetAvailable(infoUrl);

        return new DatasetUrls(datasetUrl, infoUrl, datasetId);
    }

    /**
     * Container for dataset URLs.
     */
    protected static class DatasetUrls {
        public final String datasetUrl;
        public final String infoUrl;
        public final String datasetId;

        public DatasetUrls(String datasetUrl, String infoUrl, String datasetId) {
            this.datasetUrl = datasetUrl;
            this.infoUrl = infoUrl;
            this.datasetId = datasetId;
        }
    }
}
