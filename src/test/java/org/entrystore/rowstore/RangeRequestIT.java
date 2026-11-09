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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for requests with a Range header, which the RowStore resources ignore (in 1.x, Restlet answered
 * them with 206 Partial Content).
 * Specification: 03-api.md (API-10.01)
 */
@DisplayName("Range Request Tests")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RangeRequestIT extends BaseIntegrationTest {

    private String datasetUrl;

    @BeforeAll
    void createDataset() {
        datasetUrl = createDatasetAndWait(loadTestData("dataset1_utf8.csv")).datasetUrl;
    }

    @AfterAll
    void cleanup() {
        if (datasetUrl != null) {
            deleteDataset(datasetUrl);
        }
    }

    private static void assertRangeIgnored(Response response) {
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.header("Accept-Ranges")).isNull();
        assertThat(response.header("Content-Range")).isNull();
        assertThat(response.asByteArray().length).isGreaterThan(10);
    }

    @ParameterizedTest(name = "[{index}] /dataset/<id>{0}")
    @ValueSource(strings = {"", "/json", "/info", "/export"})
    @DisplayName("TC-RANGE-001: Range is ignored by the dataset resources")
    void range_isIgnoredByDatasetResources(String subPath) {
        assertRangeIgnored(given().header("Range", "bytes=0-9").get(datasetUrl + subPath));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @ValueSource(strings = {"/datasets", "/status"})
    @DisplayName("TC-RANGE-001: Range is ignored by /datasets and /status")
    void range_isIgnoredByOtherResources(String path) {
        assertRangeIgnored(given().header("Range", "bytes=0-9").get(path));
    }

    @Test
    @DisplayName("TC-RANGE-001: An open-ended Range on the export returns the full export")
    void openEndedRange_onExport_returnsFullBody() {
        byte[] full = given().get(datasetUrl + "/export").then().statusCode(200).extract().asByteArray();
        Response ranged = given().header("Range", "bytes=5-").get(datasetUrl + "/export");
        assertRangeIgnored(ranged);
        assertThat(ranged.asByteArray()).isEqualTo(full);
    }
}
