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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletRequest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the selection of a representation by {@link ContentNegotiation}. The expected results were measured
 * against 1.x (ROWS-171).
 * Specification: API-4.03, API-4.04
 */
@DisplayName("ContentNegotiation Tests")
class ContentNegotiationTest {

    private static final MediaType TEXT_CSV = MediaType.parseMediaType("text/csv");

    private static MockHttpServletRequest request(String accept) {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/dataset/x");
        if (accept != null) {
            request.addHeader("Accept", accept);
        }
        return request;
    }

    /**
     * Representations of /dataset/{id}: JSON and the HTML view (a redirect). Representations of /dataset/{id}/export:
     * JSON and CSV.
     */
    @ParameterizedTest(name = "[{index}] Accept: {0} -> {1}, export {2}")
    @CsvSource(delimiter = '|', nullValues = "null", value = {
            // the first representation (JSON) without preference or match
            "null                                                              | application/json | application/json",
            "*/*                                                               | application/json | application/json",
            "*/*;q=0.8                                                         | application/json | application/json",
            "*;q=.2                                                            | application/json | application/json",
            "not a media type                                                  | application/json | application/json",
            "image/png                                                         | application/json | application/json",
            "image/*                                                           | application/json | application/json",
            "image/png, */*;q=0.2                                              | application/json | application/json",
            "image/gif, */*; q=.2                                              | application/json | application/json",
            // exact media type or type/*
            "application/json                                                  | application/json | application/json",
            "application/*                                                     | application/json | application/json",
            "text/html                                                         | text/html        | text/csv",
            "text/csv                                                          | text/html        | text/csv",
            "text/*                                                            | text/html        | text/csv",
            "application/*+json, text/plain;q=0.5                              | application/json | application/json",
            // only the same top-level type (e.g., text/plain for text/html and text/csv)
            "text/plain                                                        | text/html        | text/csv",
            "text/json                                                         | text/html        | text/csv",
            "text/javascript                                                   | text/html        | text/csv",
            "application/javascript                                            | application/json | application/json",
            "application/xml                                                   | application/json | application/json",
            "application/ld+json                                               | application/json | application/json",
            "text/html;level=1                                                 | text/html        | text/csv",
            "application/json;charset=utf-8                                    | application/json | application/json",
            // the more specific match wins, */* never decides
            "text/html, */*                                                    | text/html        | text/csv",
            "text/html;q=0.5, */*                                              | text/html        | text/csv",
            "text/html;q=0.001, */*                                            | text/html        | text/csv",
            "text/html, */*;q=0.2                                              | text/html        | text/csv",
            "text/xml, */*;q=0.5                                               | text/html        | text/csv",
            "text/plain;q=0.3, */*;q=0.5                                       | text/html        | text/csv",
            "application/xml, */*;q=0.5                                        | application/json | application/json",
            "application/xml;q=0.3, */*;q=0.5                                  | application/json | application/json",
            "text/html, application/json;q=0.9                                 | text/html        | application/json",
            "application/json;q=0.9, text/html                                 | text/html        | application/json",
            "text/*;q=0.5, application/xml                                     | text/html        | text/csv",
            "text/html, image/gif, image/jpeg, *; q=.2, */*; q=.2              | text/html        | text/csv",
            "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8   | text/html        | application/json",
            "application/json, text/plain, */*                                 | application/json | application/json",
            "application/json, text/javascript, */*; q=0.01                    | application/json | application/json",
            "application/json;charset=utf-8;q=0.9, text/html;q=0.5             | text/html        | application/json",
            // the higher quality wins between matches of the same kind
            "text/html;q=0.9, application/json                                 | application/json | application/json",
            "text/csv;q=0.5, application/json                                  | application/json | application/json",
            "text/plain;q=0.9, application/json;q=0.5                          | application/json | application/json",
            "application/json;q=0.1, text/*;q=0.5                              | text/html        | text/csv",
            "text/plain;q=0.6, application/xml;q=0.5                           | text/html        | text/csv",
            "text/plain;q=0.5, application/xml;q=0.6                           | application/json | application/json",
            // on equal quality, the first representation wins
            "text/html, application/json                                       | application/json | application/json",
            "application/json, text/html                                       | application/json | application/json",
            "text/html;q=0.5, application/json;q=0.5                           | application/json | application/json",
            "text/plain, application/json                                      | application/json | application/json",
            "application/xml, text/plain                                       | application/json | application/json",
            // quality 0 still matches, but its media type is not added as the parent of another range
            "text/html;q=0                                                     | text/html        | text/csv",
            "text/html;q=0, */*                                                | text/html        | text/csv",
            "text/html;q=0, application/xml                                    | application/json | application/json",
            "application/json;q=0, text/plain                                  | text/html        | text/csv",
            "text/html;q=0, text/*;q=0, */*                                    | application/json | application/json",
            "application/*;q=0, text/plain;q=0.001                             | text/html        | text/csv",
            // unparseable ranges are skipped
            "text/html, garbage                                                | text/html        | text/csv",
            "text/html;q=2                                                     | application/json | application/json"
    })
    @DisplayName("Dataset resource: JSON or HTML; export: JSON or CSV")
    void select(String accept, String dataset, String export) {
        assertThat(ContentNegotiation.select(request(accept), MediaType.APPLICATION_JSON, MediaType.TEXT_HTML))
                .isEqualTo(MediaType.parseMediaType(dataset));
        assertThat(ContentNegotiation.select(request(accept), MediaType.APPLICATION_JSON, TEXT_CSV))
                .isEqualTo(MediaType.parseMediaType(export));
    }

}
