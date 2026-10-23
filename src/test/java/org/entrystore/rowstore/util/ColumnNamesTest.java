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

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.ICSVParser;
import com.opencsv.RFC4180ParserBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.StringReader;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

/**
 * Tests the normalization of column labels and the cleaning of stored column names (ETL-4.04, ETL-4.07, ETL-11.02).
 * In the test data, {@code <BOM>} stands for U+FEFF, {@code <SHY>} for U+00AD, {@code <EM>} for U+2003 (em space),
 * {@code <NBSP>} for U+00A0 (no-break space) and {@code <C1>} for U+0001.
 */
class ColumnNamesTest {

    private static final String BOM = String.valueOf((char) 0xFEFF);

    @ParameterizedTest(name = "[{index}] \"{0}\" -> \"{1}\"")
    @DisplayName("TC-ETL-038: A label is normalized by removing byte order marks and soft hyphens, trimming and lower-casing")
    @CsvSource(delimiter = '|', quoteCharacter = '\'', value = {
            "Name|name",
            "'  Name\t'|name",
            "<BOM>Name|name",
            "Na<SHY>me|name",
            "<SHY>Na<BOM>me<SHY>|name",
            "<EM>Name<EM>|name",
            // control characters are removed at the ends like by trim() in 1.x, also next to Unicode whitespace
            "<C1>Name<C1>|name",
            "<EM><C1>Name<C1><EM>|name",
            "Na<C1>me|na<C1>me",
            // no-break spaces are not whitespace for trim() or strip() and are kept, like in 1.x
            "<NBSP>Name<NBSP>|<NBSP>name<NBSP>",
            "' <BOM> '|''",
            "<BOM>\"name\"|\"name\"",
            "'a b'|a b",
    })
    void normalize(String label, String expected) {
        assertThat(ColumnNames.normalize(decode(label))).isEqualTo(decode(expected));
    }

    /**
     * For each raw first label, the name stored by 1.x (byte order mark in front of the label, label trimmed and
     * lower-cased) and the name of a fresh import by 2.0 (byte order mark skipped, label normalized) are determined
     * with the actual parsers, configured as in PgDataset. The cleaned stored name must equal the name of 2.0.
     */
    @ParameterizedTest(name = "[{index}] {0}: {1} stored as {2} -> {3}")
    @DisplayName("TC-ETL-039: A stored column name is cleaned to the name a fresh import of the same header yields with the configured parser")
    @CsvSource(delimiter = '|', quoteCharacter = '\'', value = {
            // parser | raw first label | stored by 1.x | name in 2.0
            "rfc|\"kopare_id\"|<BOM>\"kopare_id\"|kopare_id",
            "rfc|\"Kopare_ID\"|<BOM>\"kopare_id\"|kopare_id",
            "rfc|kopare_id|<BOM>kopare_id|kopare_id",
            "rfc|\"a \"\"b\"\"\"|<BOM>\"a \"b\"\"|a \"b\"",
            "rfc|\"a\"\"\"\"b\"|<BOM>\"a\"\"b\"|a\"\"b",
            "rfc|\"a\"\"b\"|<BOM>\"a\"b\"|a\"b",
            "rfc|\"\"\"x\"\"\"|<BOM>\"\"x\"\"|\"x\"",
            "rfc|\"\"\"x\"|<BOM>\"\"x\"|\"x",
            "rfc|\"\"\"a\"\"b\"\"\"|<BOM>\"\"a\"b\"\"|\"a\"b\"",
            "rfc|\"x\"\"\"|<BOM>\"x\"\"|x\"",
            "rfc|\" x \"|<BOM>\" x \"|x",
            "rfc|\"ko<SHY>pare\"|<BOM>\"ko<SHY>pare\"|kopare",
            "rfc|' \"x\"'|<BOM> \"x\"|\"x\"",
            "rfc|x\"|<BOM>x\"|x\"",
            "rfc|\"\"|<BOM>\"|''",
            "legacy|\"kopare_id\"|<BOM>kopare_id|kopare_id",
            "legacy|kopare_id|<BOM>kopare_id|kopare_id",
            "legacy|\"a \"\"b\"\"\"|<BOM>a \"b\"|a \"b\"",
            "legacy|\"a\"\"\"\"b\"|<BOM>a\"\"b|a\"\"b",
            "legacy|\"a\"\"b\"|<BOM>a\"b|a\"b",
            "legacy|\"\"\"x\"\"\"|<BOM>\"\"x\"|\"x\"",
            "legacy|\"\"\"x\"|<BOM>\"\"x|\"x",
            "legacy|\"\"\"a\"\"b\"\"\"|<BOM>\"\"a\"b\"|\"a\"b\"",
            "legacy|\"\"\"\"|<BOM>\"\"|\"",
            "legacy|\"\"\"\"\"\"|<BOM>\"\"\"|\"\"",
            "legacy|\"x\"\"\"|<BOM>x\"|x\"",
            "legacy|' \"x\"'|<BOM> x|x",
            "legacy|\" x \"|<BOM> x|x",
            "legacy|\"\"|<BOM>\"|''",
    })
    void cleanStoredName_matchesFreshImport(String parser, String rawLabel, String stored, String expected) throws Exception {
        boolean legacyParser = parser.equals("legacy");
        String raw = decode(rawLabel);
        assertThat(storedBy1x(raw, legacyParser)).isEqualTo(decode(stored));
        assertThat(importedBy20(raw, legacyParser)).isEqualTo(decode(expected));

        String cleaned = ColumnNames.cleanStoredName(decode(stored), legacyParser);
        assertThat(cleaned).isEqualTo(decode(expected));
        // a clean name stays as it is, so a repeated cleanup changes nothing
        assertThat(ColumnNames.cleanStoredName(cleaned, legacyParser)).isEqualTo(cleaned);
    }

    /**
     * Stored names that do not determine the raw header: the result is the name of the more likely header, which
     * differs from the name of the less likely header given here (documented in ETL-11.02).
     */
    @ParameterizedTest(name = "[{index}] {0}: {1} stored as {2} -> {3}, not {4}")
    @DisplayName("TC-ETL-042: Ambiguous stored column names are cleaned to the name of the more likely header")
    @CsvSource(delimiter = '|', quoteCharacter = '\'', value = {
            // parser | less likely raw first label | stored by 1.x | cleaned name | name of that header in 2.0
            // <BOM>"x" stems from the plain quoted label "x" (stored by the default parser) in the common case
            "rfc|\"\"x\"\"|<BOM>\"x\"|x|\"x\"",
            "legacy|\"\"x\"\"|<BOM>\"x\"|x|x\"",
            // quote-only names: the cleaned name has one quote less than the name of the header in 2.0; an empty
            // name skips the dataset, the other one is accepted
            "rfc|\"\"\"\"|<BOM>\"\"|''|\"",
            "rfc|\"\"\"\"\"\"|<BOM>\"\"\"|\"|\"\"",
            // <BOM>x" stems from "x""" (x" in 2.0) as well
            "legacy|'\"x\" '|<BOM>x\"|x\"|x",
    })
    void ambiguousStoredName_cleanedToMoreLikelyHeader(String parser, String rawLabel, String stored, String cleaned,
                                                       String lessLikely) throws Exception {
        boolean legacyParser = parser.equals("legacy");
        String raw = decode(rawLabel);
        assertThat(storedBy1x(raw, legacyParser)).isEqualTo(decode(stored));
        assertThat(importedBy20(raw, legacyParser)).isEqualTo(decode(lessLikely));
        assertThat(ColumnNames.cleanStoredName(decode(stored), legacyParser)).isEqualTo(decode(cleaned));
    }

    /**
     * A dataset loaded by the default parser of 1.x, whose instance is configured with the legacy parser now.
     */
    @ParameterizedTest(name = "[{index}] {0} stored as {1} -> {2}")
    @DisplayName("TC-ETL-048: A plain quoted label stored by the default parser is unquoted also if the legacy parser is configured")
    @CsvSource(delimiter = '|', quoteCharacter = '\'', value = {
            // raw first label | stored by 1.x with the default parser | name in 2.0 with the legacy parser
            "\"kopare_id\"|<BOM>\"kopare_id\"|kopare_id",
            "\"a \"\"b\"\"\"|<BOM>\"a \"b\"\"|a \"b\"",
            "\"a\"\"b\"|<BOM>\"a\"b\"|a\"b",
            "\" x \"|<BOM>\" x \"|x",
    })
    void plainQuotedLabelOfDefaultParser_unquotedWithLegacyParser(String rawLabel, String stored, String expected) throws Exception {
        String raw = decode(rawLabel);
        assertThat(storedBy1x(raw, false)).isEqualTo(decode(stored));
        assertThat(importedBy20(raw, true)).isEqualTo(decode(expected));
        assertThat(ColumnNames.cleanStoredName(decode(stored), true)).isEqualTo(decode(expected));
    }

    @ParameterizedTest(name = "[{index}] \"{0}\" -> \"{1}\"")
    @DisplayName("TC-ETL-043: Stored column names without a byte order mark directly followed by a quote are only cleaned of byte order marks and soft hyphens")
    @CsvSource(delimiter = '|', quoteCharacter = '\'', value = {
            "na<SHY>mn|namn",
            "<SHY>namn|namn",
            // trimmed like by 1.x once the soft hyphen is removed
            "'<SHY> namn'|namn",
            // names without byte order mark and soft hyphen are not changed (ETL-11.08)
            "<EM>namn|<EM>namn",
            "<C1>namn|<C1>namn",
            "'namn '|'namn '",
            "Namn|Namn",
            // Unicode whitespace is not removed, upper case is kept
            "<EM>na<SHY>mn|<EM>namn",
            "Na<SHY>mn|Namn",
            // names without byte order mark that look quoted are not unquoted: the header """x""" yields "x"
            "\"x\"|\"x\"",
            "<SHY>\"x\"|\"x\"",
            "\"x\"<BOM>|\"x\"",
            "<SHY><BOM>\"x\"|\"x\"",
            // clean names are unchanged
            "kopare_id|kopare_id",
            "'a b'|a b",
            "''|''",
            // a name can be cleaned to an empty name (ETL-11.09)
            "<BOM>|''",
            "' <SHY>'|''",
    })
    void cleanStoredName_withoutBomQuote(String name, String expected) {
        for (boolean legacyParser : new boolean[]{false, true}) {
            assertThat(ColumnNames.cleanStoredName(decode(name), legacyParser)).isEqualTo(decode(expected));
        }
    }

    @Test
    @DisplayName("TC-ETL-040: Renames are determined for unclean names only, not for names that clean to empty, and not at all if clean names would collide")
    void renamesForStoredNames() {
        assertThat(ColumnNames.renamesForStoredNames(List.of("kopare_id", "namn"), false)).isEmpty();
        assertThat(ColumnNames.renamesForStoredNames(List.of(), false)).isEmpty();
        assertThat(ColumnNames.renamesForStoredNames(List.of(decode("<BOM>\"kopare_id\""), decode("na<SHY>mn"), "ort"), false))
                .containsOnly(entry(decode("<BOM>\"kopare_id\""), "kopare_id"), entry(decode("na<SHY>mn"), "namn"));
        assertThat(ColumnNames.renamesForStoredNames(List.of(decode("<BOM>kopare_id"), "ort"), true))
                .containsOnly(entry(decode("<BOM>kopare_id"), "kopare_id"));
        // collision with an existing name
        assertThat(ColumnNames.renamesForStoredNames(List.of(decode("<BOM>\"namn\""), "namn"), false)).isNull();
        // collision of two renamed names
        assertThat(ColumnNames.renamesForStoredNames(List.of(decode("<BOM>namn"), decode("na<SHY>mn")), false)).isNull();
        // a name that cleans to empty is kept, the other names are cleaned (ETL-11.09)
        assertThat(ColumnNames.renamesForStoredNames(List.of(decode("<BOM>\""), "namn"), false)).isEmpty();
        assertThat(ColumnNames.renamesForStoredNames(List.of(decode("<BOM>"), decode("na<SHY>mn")), false))
                .containsOnly(entry(decode("na<SHY>mn"), "namn"));
        // surrounding whitespace, Unicode whitespace and upper case are not cleaned (ETL-11.08)
        assertThat(ColumnNames.renamesForStoredNames(List.of("namn ", " ort", decode("zon<EM>"), "Kommun"), false)).isEmpty();
    }

    @Test
    @DisplayName("TC-ETL-044: An append matches each label with the stored name that is cleaned to it with exactly one candidate")
    void forAppend() {
        // names equal to the labels need no renames
        assertThat(ColumnNames.forAppend(List.of("kopare_id", "namn"), Set.of("kopare_id", "namn")).renames()).isEmpty();
        // the name of either parser is accepted
        assertThat(ColumnNames.forAppend(List.of(decode("<BOM>\"kopare_id\""), decode("na<SHY>mn")), Set.of("kopare_id", "namn")).renames())
                .containsOnly(entry(decode("<BOM>\"kopare_id\""), "kopare_id"), entry(decode("na<SHY>mn"), "namn"));
        // <BOM>""x"" is "x" with the default parser and "x"" with the legacy parser
        assertThat(ColumnNames.forAppend(List.of(decode("<BOM>\"\"x\"\""), "namn"), Set.of("\"x\"\"", "namn")).renames())
                .containsOnly(entry(decode("<BOM>\"\"x\"\""), "\"x\"\""));
        assertThat(ColumnNames.forAppend(List.of(decode("<BOM>\"\"x\"\""), "namn"), Set.of("\"x\"", "namn")).renames())
                .containsOnly(entry(decode("<BOM>\"\"x\"\""), "\"x\""));
        // both candidates are labels: ambiguous
        assertThat(ColumnNames.forAppend(List.of(decode("<BOM>\"\"x\"\"")), Set.of("\"x\"", "\"x\"\""))).isNull();
        // no candidate is a label
        assertThat(ColumnNames.forAppend(List.of(decode("<BOM>\"kopare_id\""), "namn"), Set.of("id", "namn"))).isNull();
        // different number of names and labels
        assertThat(ColumnNames.forAppend(List.of(decode("<BOM>kopare_id")), Set.of("kopare_id", "namn"))).isNull();
        assertThat(ColumnNames.forAppend(List.of("kopare_id", "namn", "ort"), Set.of("kopare_id", "namn"))).isNull();
        // two names renamed to the same label, or renamed to an existing name
        assertThat(ColumnNames.forAppend(List.of(decode("<BOM>namn"), decode("na<SHY>mn")), Set.of("namn", "ort"))).isNull();
        assertThat(ColumnNames.forAppend(List.of(decode("<BOM>namn"), "namn"), Set.of("namn", "ort"))).isNull();
    }

    @Test
    @DisplayName("TC-ETL-060: An append keeps stored names that the cleanup does not change and ignores stored names that are empty as a label")
    void forAppend_keepsUncleanedNamesAndIgnoresEmptyNames() {
        // stored names that only differ from the labels in whitespace or case are kept (ETL-11.08)
        ColumnNames.AppendNames kept = ColumnNames.forAppend(List.of(decode("zon<EM>"), "namn ", "Kommun"), Set.of("zon", "namn", "kommun"));
        assertThat(kept.renames()).isEmpty();
        assertThat(kept.namesByLabel()).containsOnly(entry("zon", decode("zon<EM>")), entry("namn", "namn "), entry("kommun", "Kommun"));
        // a renamed name gets the clean name, not the label (case and Unicode whitespace kept)
        ColumnNames.AppendNames renamed = ColumnNames.forAppend(List.of(decode("Na<SHY>mn")), Set.of("namn"));
        assertThat(renamed.renames()).containsOnly(entry(decode("Na<SHY>mn"), "Namn"));
        assertThat(renamed.namesByLabel()).containsOnly(entry("namn", "Namn"));
        // stored names that are empty as a label match no label (ETL-11.09)
        for (String empty : new String[]{"<BOM>", "<BOM>\"", "<EM>"}) {
            ColumnNames.AppendNames ignored = ColumnNames.forAppend(List.of(decode(empty), "a", "b"), Set.of("a", "b"));
            assertThat(ignored).as(empty).isNotNull();
            assertThat(ignored.renames()).isEmpty();
            assertThat(ignored.namesByLabel()).containsOnly(entry("a", "a"), entry("b", "b"));
        }
        // stored names that normalize to the same label are ambiguous
        assertThat(ColumnNames.forAppend(List.of("namn", "namn "), Set.of("namn"))).isNull();
    }

    @Test
    @DisplayName("TC-ETL-041: Byte order marks and soft hyphens are escaped for logging")
    void escapeForLog() {
        assertThat(ColumnNames.escapeForLog(decode("<BOM>\"na<SHY>mn\""))).isEqualTo("\\" + "uFEFF\"na\\" + "u00ADmn\"");
    }

    /**
     * @return Returns the column name that 1.x stored for the first label of a file with byte order mark.
     */
    private static String storedBy1x(String rawLabel, boolean legacyParser) throws Exception {
        return firstLabel(BOM + rawLabel + ",b\n1,2\n", legacyParser).trim().toLowerCase();
    }

    /**
     * @return Returns the column name that 2.0 stores for the first label (the byte order mark is skipped).
     */
    private static String importedBy20(String rawLabel, boolean legacyParser) throws Exception {
        return ColumnNames.normalize(firstLabel(rawLabel + ",b\n1,2\n", legacyParser));
    }

    private static String firstLabel(String csv, boolean legacyParser) throws Exception {
        ICSVParser parser = legacyParser
                ? new CSVParserBuilder().withSeparator(',').withQuoteChar('"').build()
                : new RFC4180ParserBuilder().withSeparator(',').withQuoteChar('"').build();
        try (CSVReader reader = new CSVReaderBuilder(new StringReader(csv)).withSkipLines(0).withCSVParser(parser).build()) {
            return reader.readNext()[0];
        }
    }

    private static String decode(String s) {
        return s.replace("<BOM>", BOM)
                .replace("<SHY>", String.valueOf((char) 0x00AD))
                .replace("<EM>", String.valueOf((char) 0x2003))
                .replace("<NBSP>", String.valueOf((char) 0x00A0))
                .replace("<C1>", String.valueOf((char) 0x0001));
    }

}
