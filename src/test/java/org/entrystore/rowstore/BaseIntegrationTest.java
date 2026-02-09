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

import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.ContentType;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.List;

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

    protected static final int ETL_STATUS_CREATED = TestUtils.ETL_STATUS_CREATED;
    protected static final int ETL_STATUS_ACCEPTED_DATA = TestUtils.ETL_STATUS_ACCEPTED_DATA;
    protected static final int ETL_STATUS_PROCESSING = TestUtils.ETL_STATUS_PROCESSING;
    protected static final int ETL_STATUS_AVAILABLE = TestUtils.ETL_STATUS_AVAILABLE;
    protected static final int ETL_STATUS_ERROR = TestUtils.ETL_STATUS_ERROR;

    protected static final Duration POLL_INTERVAL = TestUtils.POLL_INTERVAL;
    protected static final Duration MAX_WAIT = TestUtils.MAX_WAIT;

    protected static RequestSpecification jsonSpec;
    protected static RequestSpecification csvSpec;

    @BeforeAll
    static void setupRestAssured() {
        String baseUrl = getBaseUrl();
        RestAssured.baseURI = baseUrl;

        jsonSpec = new RequestSpecBuilder()
                .setBaseUri(baseUrl)
                .setAccept(ContentType.JSON)
                .build();

        csvSpec = new RequestSpecBuilder()
                .setBaseUri(baseUrl)
                .setContentType("text/csv")
                .setAccept(ContentType.JSON)
                .build();
    }

    protected static String getBaseUrl() {
        String externalUrl = System.getProperty("rowstore.baseUrl");
        if (externalUrl != null && !externalUrl.isEmpty()) {
            return externalUrl;
        }
        return RowStoreTestEnvironment.getInstance().getBaseUrl();
    }

    protected byte[] loadTestData(String filename) {
        return TestUtils.loadTestData(filename, getClass());
    }

    protected Response createDataset(byte[] csvData) {
        return TestUtils.createDataset(csvData, csvSpec);
    }

    protected void waitForDatasetAvailable(String infoUrl) {
        TestUtils.waitForDatasetAvailable(infoUrl, jsonSpec);
    }

    protected void waitForDatasetError(String infoUrl) {
        TestUtils.waitForDatasetError(infoUrl, jsonSpec);
    }

    protected String getDatasetUrl(Response response) {
        return TestUtils.getDatasetUrl(response);
    }

    protected String getInfoUrl(Response response) {
        return TestUtils.getInfoUrl(response);
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

    protected void waitForStatus(String infoUrl, int expectedStatus) {
        TestUtils.waitForStatus(infoUrl, expectedStatus, jsonSpec);
    }

    protected String loadTestDataAsString(String filename) {
        return TestUtils.loadTestDataAsString(filename, getClass());
    }

    protected void assertJsonContainsFields(Response response, String... expectedFields) {
        TestUtils.assertJsonContainsFields(response, expectedFields);
    }

    protected int getDatasetCount() {
        return TestUtils.getDatasetCount(jsonSpec);
    }

    protected List<String> getDatasetsList() {
        return TestUtils.getDatasetsList(jsonSpec);
    }

    protected static class DatasetUrls extends TestUtils.DatasetUrls {
        public DatasetUrls(String datasetUrl, String infoUrl, String datasetId) {
            super(datasetUrl, infoUrl, datasetId);
        }
    }

}
