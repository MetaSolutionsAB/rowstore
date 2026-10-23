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
 * Integration tests for byte order marks and soft hyphens in CSV headers with the legacy parser
 * ({@code legacyparser: true}), which must behave like the default parser (see {@link CsvBomIT}).
 * Specification: 04-csv-formats.md
 */
@ExtendWith(LegacyParserExtension.class)
@Tag("legacy-parser")
@DisplayName("CSV Byte Order Mark Tests (legacy parser)")
class CsvBomLegacyParserIT extends ConfigurableTestBase {

    @Override
    protected String getExtensionBaseUrl() {
        return LegacyParserExtension.getBaseUrl();
    }

    private CsvBomTestCases testCases() {
        return new CsvBomTestCases(csvSpec, jsonSpec);
    }

    @Test
    @DisplayName("TC-CSV-030: Legacy parser: a UTF-8 byte order mark before a quoted first label yields clean column names")
    void utf8BomWithQuotedFirstLabel_yieldsCleanColumnNames() {
        testCases().utf8BomWithQuotedFirstLabel_yieldsCleanColumnNames();
    }

    @Test
    @DisplayName("TC-CSV-031: Legacy parser: a UTF-8 byte order mark before an unquoted first label yields clean column names")
    void utf8BomWithUnquotedFirstLabel_yieldsCleanColumnNames() {
        testCases().utf8BomWithUnquotedFirstLabel_yieldsCleanColumnNames();
    }

    @Test
    @DisplayName("TC-CSV-032: Legacy parser: a semicolon-separated CSV with a UTF-8 byte order mark is parsed with the detected separator")
    void utf8BomWithSemicolons_detectsSeparator() {
        testCases().utf8BomWithSemicolons_detectsSeparator();
    }

    @Test
    @DisplayName("TC-CSV-033: Legacy parser: a UTF-16LE byte order mark before a quoted first label yields clean column names")
    void utf16LeBomWithQuotedFirstLabel_yieldsCleanColumnNames() {
        testCases().utf16LeBomWithQuotedFirstLabel_yieldsCleanColumnNames();
    }

    @Test
    @DisplayName("TC-CSV-034: Legacy parser: soft hyphens are removed from labels and kept in values")
    void softHyphensInLabels_areRemoved() {
        testCases().softHyphensInLabels_areRemoved();
    }

    @Test
    @DisplayName("TC-CSV-035: Legacy parser: an append with byte order mark and quoted first label matches the column names")
    void appendWithBom_matchesColumnNames() {
        testCases().appendWithBom_matchesColumnNames();
    }

    @Test
    @DisplayName("TC-CSV-036: Legacy parser: a first line consisting of a byte order mark only is an empty line")
    void bomOnlyFirstLine_isEmptyLine() {
        testCases().bomOnlyFirstLine_isEmptyLine();
    }

    @Test
    @DisplayName("TC-CSV-044: Legacy parser: a first label consisting of a byte order mark only skips its column without shifting the others")
    void bomOnlyFirstLabel_skipsColumnWithoutShifting() {
        testCases().bomOnlyFirstLabel_skipsColumnWithoutShifting();
    }

    @Test
    @DisplayName("TC-CSV-045: Legacy parser: columns with an empty label are skipped without shifting the others")
    void emptyLabels_skipColumnsWithoutShifting() {
        testCases().emptyLabels_skipColumnsWithoutShifting();
    }

    @Test
    @DisplayName("TC-CSV-046: Legacy parser: of labels that are equal after normalization, the first column is kept and the others are skipped")
    void duplicateLabelsAfterNormalization_firstColumnKept() {
        testCases().duplicateLabelsAfterNormalization_firstColumnKept();
    }

}
