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
 * Integration tests for byte order marks and soft hyphens in CSV headers with the default (RFC 4180) parser.
 * Specification: 04-csv-formats.md
 */
@DisplayName("CSV Byte Order Mark Tests (default parser)")
class CsvBomIT extends BaseIntegrationTest {

    private CsvBomTestCases testCases() {
        return new CsvBomTestCases(csvSpec, jsonSpec);
    }

    @Test
    @DisplayName("TC-CSV-023: A UTF-8 byte order mark before a quoted first label yields clean column names")
    void utf8BomWithQuotedFirstLabel_yieldsCleanColumnNames() {
        testCases().utf8BomWithQuotedFirstLabel_yieldsCleanColumnNames();
    }

    @Test
    @DisplayName("TC-CSV-024: A UTF-8 byte order mark before an unquoted first label yields clean column names")
    void utf8BomWithUnquotedFirstLabel_yieldsCleanColumnNames() {
        testCases().utf8BomWithUnquotedFirstLabel_yieldsCleanColumnNames();
    }

    @Test
    @DisplayName("TC-CSV-025: A semicolon-separated CSV with a UTF-8 byte order mark is parsed with the detected separator")
    void utf8BomWithSemicolons_detectsSeparator() {
        testCases().utf8BomWithSemicolons_detectsSeparator();
    }

    @Test
    @DisplayName("TC-CSV-026: A UTF-16LE byte order mark before a quoted first label yields clean column names")
    void utf16LeBomWithQuotedFirstLabel_yieldsCleanColumnNames() {
        testCases().utf16LeBomWithQuotedFirstLabel_yieldsCleanColumnNames();
    }

    @Test
    @DisplayName("TC-CSV-027: Soft hyphens are removed from labels and kept in values")
    void softHyphensInLabels_areRemoved() {
        testCases().softHyphensInLabels_areRemoved();
    }

    @Test
    @DisplayName("TC-CSV-028: An append with byte order mark and quoted first label matches the column names")
    void appendWithBom_matchesColumnNames() {
        testCases().appendWithBom_matchesColumnNames();
    }

    @Test
    @DisplayName("TC-CSV-029: A first line consisting of a byte order mark only is an empty line")
    void bomOnlyFirstLine_isEmptyLine() {
        testCases().bomOnlyFirstLine_isEmptyLine();
    }

    @Test
    @DisplayName("TC-CSV-041: A first label consisting of a byte order mark only skips its column without shifting the others")
    void bomOnlyFirstLabel_skipsColumnWithoutShifting() {
        testCases().bomOnlyFirstLabel_skipsColumnWithoutShifting();
    }

    @Test
    @DisplayName("TC-CSV-042: Columns with an empty label are skipped without shifting the others")
    void emptyLabels_skipColumnsWithoutShifting() {
        testCases().emptyLabels_skipColumnsWithoutShifting();
    }

    @Test
    @DisplayName("TC-CSV-043: Of labels that are equal after normalization, the first column is kept and the others are skipped")
    void duplicateLabelsAfterNormalization_firstColumnKept() {
        testCases().duplicateLabelsAfterNormalization_firstColumnKept();
    }

}
