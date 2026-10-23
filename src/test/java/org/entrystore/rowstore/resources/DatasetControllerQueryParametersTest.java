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

package org.entrystore.rowstore.resources;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the parsing of query parameters in {@link DatasetController}.
 * Specification: QUERY-2.03
 */
@DisplayName("DatasetController query parameter Tests")
class DatasetControllerQueryParametersTest {

    @Test
    @DisplayName("Names and values are trimmed, empty names skipped, the last value of a repeated parameter used")
    void queryParameters_likeOneX() {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/dataset/x");
        request.addParameter("name", "Bo ");
        request.addParameter(" city", " Malmö");
        request.addParameter("_limit", " 1");
        request.addParameter(" ", "x");
        request.addParameter("tel", "1", "2");
        request.addParameter("empty", " ");

        assertThat(DatasetController.queryParameters(request))
                .containsEntry("name", "Bo")
                .containsEntry("city", "Malmö")
                .containsEntry("_limit", "1")
                .containsEntry("tel", "2")
                .containsEntry("empty", "")
                .hasSize(5);
    }

}
