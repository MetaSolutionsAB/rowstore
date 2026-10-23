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

package org.entrystore.rowstore.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the Content-Type check of CSV uploads in {@link DatasetUtil}.
 * Specification: API-3.03, SEC-4.04
 */
@DisplayName("DatasetUtil Content-Type Tests")
class DatasetUtilContentTypeTest {

    @ParameterizedTest(name = "[{index}] ''{0}'' -> {1}")
    @CsvSource(delimiter = '|', nullValues = "null", value = {
            "null|                             true",
            "''|                               true",
            "text/csv|                         true",
            "text/csv; charset=UTF-8|          true",
            "TEXT/CSV|                         true",
            "text/plain|                       false",
            "application/octet-stream|         false",
            "application/json|                 false",
            "multipart/form-data; boundary=x|  false",
            "text|                             false"
    })
    void isCsvContentType(String contentType, boolean expected) {
        assertThat(DatasetUtil.isCsvContentType(contentType)).isEqualTo(expected);
    }

}
