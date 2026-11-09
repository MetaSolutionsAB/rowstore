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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for loads whose CSV yields no columns (ROWS-180) with the default (RFC 4180) parser.
 * Specification: ETL-4.10, test specification specs/tests/04-csv-formats.md
 */
@DisplayName("CSV Without Columns Tests (default parser)")
class CsvNoColumnsIT extends BaseIntegrationTest {

    private CsvNoColumnsTestCases testCases() {
        return new CsvNoColumnsTestCases(csvSpec, jsonSpec);
    }

    @Test
    @DisplayName("TC-CSV-047: A replace without a header line fails and keeps the previous data")
    void replaceWithoutHeaderLine_failsAndKeepsPreviousData() {
        testCases().replaceWithoutHeaderLine_failsAndKeepsPreviousData();
    }

    @Test
    @DisplayName("TC-CSV-048: A replace with a gzip or zip body sent without Content-Encoding fails and keeps the previous data")
    void replaceWithUndecodedGzip_failsAndKeepsPreviousData() {
        testCases().replaceWithUndecodedGzip_failsAndKeepsPreviousData();
    }

    @Test
    @DisplayName("TC-CSV-049: An append without columns fails and adds nothing")
    void appendWithoutColumns_failsAndAddsNothing() {
        testCases().appendWithoutColumns_failsAndAddsNothing();
    }

    @Test
    @DisplayName("TC-CSV-050: A new dataset whose header has no label fails")
    void headerWithoutLabel_fails() {
        testCases().headerWithoutLabel_fails();
    }

    @Test
    @DisplayName("TC-CSV-NEW1: A replace whose header contains a control character fails and keeps the previous data")
    void replaceWithControlCharacterInHeader_failsAndKeepsPreviousData() {
        testCases().replaceWithControlCharacterInHeader_failsAndKeepsPreviousData();
    }

    @Test
    @DisplayName("TC-CSV-051: A header-only CSV with column names is available with 0 rows")
    void headerOnly_isAvailableWithColumns() {
        testCases().headerOnly_isAvailableWithColumns();
    }

}
