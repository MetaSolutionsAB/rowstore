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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

/**
 * Tests the normalization of column labels and the cleaning of stored column names (ETL-4.04, ETL-4.07, ETL-11.02).
 * In the test data, {@code <BOM>} stands for U+FEFF, {@code <SHY>} for U+00AD and {@code <EM>} for U+2003.
 */
class ColumnNamesTest {

    @ParameterizedTest(name = "[{index}] \"{0}\" -> \"{1}\"")
    @DisplayName("TC-ETL-038: A label is normalized by removing byte order marks and soft hyphens, stripping and lower-casing")
    @CsvSource(delimiter = '|', quoteCharacter = '\'', value = {
            "Name|name",
            "'  Name\t'|name",
            "<BOM>Name|name",
            "Na<SHY>me|name",
            "<SHY>Na<BOM>me<SHY>|name",
            "<EM>Name<EM>|name",
            "' <BOM> '|''",
            "<BOM>\"name\"|\"name\"",
            "'a b'|a b",
    })
    void normalize(String label, String expected) {
        assertThat(ColumnNames.normalize(decode(label))).isEqualTo(decode(expected));
    }

    @ParameterizedTest(name = "[{index}] \"{0}\" -> \"{1}\"")
    @DisplayName("TC-ETL-039: A stored column name is cleaned like the label is parsed now")
    @CsvSource(delimiter = '|', quoteCharacter = '\'', value = {
            // byte order mark and quoted first label, as stored by the default parser of 1.x
            "<BOM>\"kopare_id\"|kopare_id",
            // byte order mark and unquoted first label, or quoted first label with the legacy parser of 1.x
            "<BOM>kopare_id|kopare_id",
            // doubled quotes in a quoted label stand for one quote
            "<BOM>\"a\"\"b\"|a\"b",
            "<BOM>\"\"\"x\"\"\"|\"x\"",
            // whitespace inside the quotes is stripped like by the label normalization
            "<BOM>\" x \"|x",
            "<BOM>\"ko<SHY>pare\"|kopare",
            // a lone quote is not a quoted label
            "<BOM>\"|\"",
            "<BOM>\"\"|''",
            "na<SHY>mn|namn",
            "<SHY>namn|namn",
            "<EM>namn|namn",
            // names without byte order mark that look quoted are not unquoted: the header """x""" yields "x"
            "\"x\"|\"x\"",
            "<SHY>\"x\"|\"x\"",
            // a byte order mark that is not at the beginning does not unquote either
            "\"x\"<BOM>|\"x\"",
            // clean names are unchanged
            "kopare_id|kopare_id",
            "a b|a b",
            "''|''",
    })
    void cleanStoredName(String name, String expected) {
        String cleaned = ColumnNames.cleanStoredName(decode(name));
        assertThat(cleaned).isEqualTo(decode(expected));
        // a clean name stays as it is, so a repeated cleanup changes nothing
        assertThat(ColumnNames.cleanStoredName(cleaned)).isEqualTo(cleaned);
    }

    @Test
    @DisplayName("TC-ETL-040: Renames are determined for unclean names only, and not at all if clean names would collide or be empty")
    void renamesForStoredNames() {
        assertThat(ColumnNames.renamesForStoredNames(List.of("kopare_id", "namn"))).isEmpty();
        assertThat(ColumnNames.renamesForStoredNames(List.of())).isEmpty();
        assertThat(ColumnNames.renamesForStoredNames(List.of(decode("<BOM>\"kopare_id\""), decode("na<SHY>mn"), "ort")))
                .containsOnly(entry(decode("<BOM>\"kopare_id\""), "kopare_id"), entry(decode("na<SHY>mn"), "namn"));
        // collision with an existing name
        assertThat(ColumnNames.renamesForStoredNames(List.of(decode("<BOM>\"namn\""), "namn"))).isNull();
        // collision of two renamed names
        assertThat(ColumnNames.renamesForStoredNames(List.of(decode("<BOM>namn"), decode("na<SHY>mn")))).isNull();
        // empty clean name
        assertThat(ColumnNames.renamesForStoredNames(List.of(decode("<BOM>"), "namn"))).isNull();
    }

    @Test
    @DisplayName("TC-ETL-041: Byte order marks and soft hyphens are escaped for logging")
    void escapeForLog() {
        assertThat(ColumnNames.escapeForLog(decode("<BOM>\"na<SHY>mn\""))).isEqualTo("\\uFEFF\"na\\u00ADmn\"");
    }

    private static String decode(String s) {
        return s.replace("<BOM>", "\uFEFF").replace("<SHY>", "\u00AD").replace("<EM>", "\u2003");
    }

}
