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
 * Unit tests for the selection of a representation by {@link ContentNegotiation}.
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
     * Representations of /dataset/{id}: the HTML view (a redirect) and JSON. The expected results are those of 1.x.
     */
    @ParameterizedTest(name = "[{index}] Accept: {0} -> {1}")
    @CsvSource(delimiter = '|', nullValues = "null", value = {
            "null|                                                                         text/html",
            "*/*|                                                                          text/html",
            "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8|               text/html",
            "text/html, image/gif, image/jpeg, *; q=.2, */*; q=.2|                          text/html",
            "application/json|                                                             application/json",
            "application/json, text/javascript, */*; q=0.01|                               application/json",
            "application/*|                                                                application/json",
            "text/*|                                                                       text/html",
            "text/html;q=0, */*|                                                           application/json",
            // nothing acceptable: the representation with the same top-level type, as Restlet did
            "text/plain|                                                                   text/html",
            "text/csv|                                                                     text/html",
            "text/json|                                                                    text/html",
            "text/javascript|                                                              text/html",
            "application/ld+json|                                                          application/json",
            "application/xml|                                                              application/json",
            "image/png|                                                                    text/html",
            "not a media type|                                                             text/html"
    })
    @DisplayName("Dataset resource: HTML or JSON")
    void dataset(String accept, String expected) {
        assertThat(ContentNegotiation.select(request(accept), MediaType.TEXT_HTML, MediaType.APPLICATION_JSON))
                .isEqualTo(MediaType.parseMediaType(expected));
    }

    /**
     * Representations of /dataset/{id}/export: JSON (the default) and CSV. The expected results are those of 1.x.
     */
    @ParameterizedTest(name = "[{index}] Accept: {0} -> {1}")
    @CsvSource(delimiter = '|', nullValues = "null", value = {
            "null|                                application/json",
            "*/*|                                 application/json",
            "application/json|                    application/json",
            "text/csv|                            text/csv",
            "text/csv, application/json;q=0.5|    text/csv",
            "text/csv;q=0.5, application/json|    application/json",
            "text/*|                              text/csv",
            "text/html|                           text/csv",
            "text/plain|                          text/csv",
            "application/xml|                     application/json"
    })
    @DisplayName("Export resource: JSON or CSV")
    void export(String accept, String expected) {
        assertThat(ContentNegotiation.select(request(accept), MediaType.APPLICATION_JSON, TEXT_CSV))
                .isEqualTo(MediaType.parseMediaType(expected));
    }

}
