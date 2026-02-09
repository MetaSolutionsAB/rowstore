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
import io.restassured.response.Response;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Integration tests for the /datasets listing endpoint.
 * Specification: 02-datasets-list.md
 *
 * Note: TC-DATASETS-001 expects an empty instance. In practice, this test
 * verifies the endpoint returns a valid JSON array (may contain datasets
 * from other tests if run in sequence).
 */
@DisplayName("Datasets List Endpoint Tests")
class DatasetsListIT extends BaseIntegrationTest {

    @Test
    @DisplayName("TC-DATASETS-001: GET datasets returns JSON array")
    void getDatasets_returnsJsonArray() {
        given()
                .spec(jsonSpec)
        .when()
                .get("/datasets")
        .then()
                .statusCode(200)
                .contentType(ContentType.JSON)
                .body("", instanceOf(java.util.List.class));
    }

    @Test
    @DisplayName("TC-DATASETS-002: List grows after creating a dataset")
    void getDatasets_listGrowsAfterCreation() {
        // Get initial count
        List<String> initialList = getDatasetsList();
        int initialCount = initialList.size();

        // Create a new dataset
        byte[] csvData = loadTestData("dataset1_utf8.csv");
        DatasetUrls urls = createDatasetAndWait(csvData);

        try {
            // Get updated count
            List<String> updatedList = getDatasetsList();
            assertThat(updatedList.size()).isEqualTo(initialCount + 1);

            // Verify the new dataset ID is in the list
            // Note: The API returns dataset IDs, not full URLs
            assertThat(updatedList).contains(urls.datasetId);
        } finally {
            // Cleanup
            deleteDataset(urls.datasetUrl);
        }
    }

}
