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
 * Integration tests for empty lines in CSV files with the legacy parser ({@code legacyparser: true}), which must
 * behave like the default parser (see {@link CsvEmptyLineIT}).
 * Specification: 04-csv-formats.md
 */
@ExtendWith(LegacyParserExtension.class)
@Tag("legacy-parser")
@DisplayName("CSV Empty Line Tests (legacy parser)")
class CsvEmptyLineLegacyParserIT extends ConfigurableTestBase {

    @Override
    protected String getExtensionBaseUrl() {
        return LegacyParserExtension.getBaseUrl();
    }

    private CsvEmptyLineTestCases testCases() {
        return new CsvEmptyLineTestCases(csvSpec, jsonSpec);
    }

    @Test
    @DisplayName("TC-CSV-017: Legacy parser: an empty line between rows fails the load and a failed replace keeps the previous data")
    void innerEmptyLine_failsAndKeepsPreviousData() {
        testCases().innerEmptyLine_failsAndKeepsPreviousData();
    }

    @Test
    @DisplayName("TC-CSV-018: Legacy parser: an empty line directly after the header fails the load")
    void emptyLineAfterHeader_fails() {
        testCases().emptyLineAfterHeader_fails();
    }

    @Test
    @DisplayName("TC-CSV-019: Legacy parser: empty lines at the end of the file are ignored")
    void trailingEmptyLines_areIgnored() {
        testCases().trailingEmptyLines_areIgnored();
    }

    @Test
    @DisplayName("TC-CSV-020: Legacy parser: an empty line inside a quoted value is part of the value")
    void emptyLineInQuotedValue_isPartOfValue() {
        testCases().emptyLineInQuotedValue_isPartOfValue();
    }

    @Test
    @DisplayName("TC-CSV-021: Legacy parser: a line consisting of whitespace only is a data row")
    void whitespaceOnlyLine_isDataRow() {
        testCases().whitespaceOnlyLine_isDataRow();
    }

    @Test
    @DisplayName("TC-CSV-022: Legacy parser: a quoted empty value in a single-column CSV is a data row")
    void quotedEmptyValueInSingleColumn_isDataRow() {
        testCases().quotedEmptyValueInSingleColumn_isDataRow();
    }

}
