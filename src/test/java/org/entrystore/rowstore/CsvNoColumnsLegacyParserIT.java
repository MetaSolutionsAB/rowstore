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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Integration tests for loads whose CSV yields no columns with the legacy parser ({@code legacyparser: true}), which
 * must behave like the default parser (see {@link CsvNoColumnsIT}).
 * Specification: ETL-4.10, test specification specs/tests/04-csv-formats.md
 */
@ExtendWith(LegacyParserExtension.class)
@Tag("legacy-parser")
@DisplayName("CSV Without Columns Tests (legacy parser)")
class CsvNoColumnsLegacyParserIT extends ConfigurableTestBase {

    @Override
    protected String getExtensionBaseUrl() {
        return LegacyParserExtension.getBaseUrl();
    }

    private CsvNoColumnsTestCases testCases() {
        return new CsvNoColumnsTestCases(csvSpec, jsonSpec);
    }

    @Test
    @DisplayName("TC-CSV-052: Legacy parser: a replace without a header line fails and keeps the previous data")
    void replaceWithoutHeaderLine_failsAndKeepsPreviousData() {
        testCases().replaceWithoutHeaderLine_failsAndKeepsPreviousData();
    }

    @Test
    @DisplayName("TC-CSV-053: Legacy parser: a replace with a gzip or zip body sent without Content-Encoding fails and keeps the previous data")
    void replaceWithUndecodedGzip_failsAndKeepsPreviousData() {
        testCases().replaceWithUndecodedGzip_failsAndKeepsPreviousData();
    }

    @Test
    @DisplayName("TC-CSV-054: Legacy parser: an append without columns fails and adds nothing")
    void appendWithoutColumns_failsAndAddsNothing() {
        testCases().appendWithoutColumns_failsAndAddsNothing();
    }

    @Test
    @DisplayName("TC-CSV-055: Legacy parser: a new dataset whose header has no label fails")
    void headerWithoutLabel_fails() {
        testCases().headerWithoutLabel_fails();
    }

    @Test
    @DisplayName("TC-CSV-058: Legacy parser: a replace whose header contains a control character fails and keeps the previous data")
    void replaceWithControlCharacterInHeader_failsAndKeepsPreviousData() {
        testCases().replaceWithControlCharacterInHeader_failsAndKeepsPreviousData();
    }

    @Test
    @DisplayName("TC-CSV-056: Legacy parser: a header-only CSV with column names is available with 0 rows")
    void headerOnly_isAvailableWithColumns() {
        testCases().headerOnly_isAvailableWithColumns();
    }

}
