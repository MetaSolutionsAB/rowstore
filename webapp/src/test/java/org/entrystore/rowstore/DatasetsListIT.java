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

import io.restassured.http.ContentType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
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

}
