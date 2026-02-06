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

import java.time.Duration;

/**
 * Base class for integration tests that use a configurable test environment
 * (e.g., regex-disabled, regex-simple, rate-limiting modes).
 *
 * Provides shared utilities similar to {@link BaseIntegrationTest} but allows
 * subclasses to specify their own extension's base URL.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ConfigurableTestBase {

    protected static final int ETL_STATUS_CREATED = TestUtils.ETL_STATUS_CREATED;
    protected static final int ETL_STATUS_ACCEPTED_DATA = TestUtils.ETL_STATUS_ACCEPTED_DATA;
    protected static final int ETL_STATUS_PROCESSING = TestUtils.ETL_STATUS_PROCESSING;
    protected static final int ETL_STATUS_AVAILABLE = TestUtils.ETL_STATUS_AVAILABLE;
    protected static final int ETL_STATUS_ERROR = TestUtils.ETL_STATUS_ERROR;

    protected static final Duration POLL_INTERVAL = TestUtils.POLL_INTERVAL;
    protected static final Duration MAX_WAIT = TestUtils.MAX_WAIT;

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

    protected byte[] loadTestData(String filename) {
        return TestUtils.loadTestData(filename, getClass());
    }

    protected Response createDataset(byte[] csvData) {
        return TestUtils.createDataset(csvData, csvSpec);
    }

    protected String getDatasetUrl(Response response) {
        return TestUtils.getDatasetUrl(response);
    }

    protected String getInfoUrl(Response response) {
        return TestUtils.getInfoUrl(response);
    }

    protected void waitForDatasetAvailable(String infoUrl) {
        TestUtils.waitForDatasetAvailable(infoUrl, jsonSpec);
    }

    protected void waitForDatasetError(String infoUrl) {
        TestUtils.waitForDatasetError(infoUrl, jsonSpec);
    }

    protected void waitForStatus(String infoUrl, int expectedStatus) {
        TestUtils.waitForStatus(infoUrl, expectedStatus, jsonSpec);
    }

    protected void deleteDataset(String datasetUrl) {
        TestUtils.deleteDataset(datasetUrl);
    }

    protected void deleteDatasetAndVerify(String datasetUrl) {
        TestUtils.deleteDatasetAndVerify(datasetUrl, jsonSpec);
    }

    protected DatasetUrls createDatasetAndWait(byte[] csvData) {
        TestUtils.DatasetUrls urls = TestUtils.createDatasetAndWait(csvData, csvSpec, jsonSpec);
        return new DatasetUrls(urls.datasetUrl, urls.infoUrl, urls.datasetId);
    }

    protected static class DatasetUrls extends TestUtils.DatasetUrls {
        public DatasetUrls(String datasetUrl, String infoUrl, String datasetId) {
            super(datasetUrl, infoUrl, datasetId);
        }
    }
}
