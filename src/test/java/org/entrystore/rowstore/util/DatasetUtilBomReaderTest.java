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
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.MalformedInputException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests {@link DatasetUtil#newReaderSkippingBom(Path, Charset)} (ETL-4.06).
 */
class DatasetUtilBomReaderTest {

    private static final byte[] UTF8_BOM = {(byte) 0xEF, (byte) 0xBB, (byte) 0xBF};

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("TC-CSV-037: A leading byte order mark is skipped for UTF-8, UTF-16LE and UTF-16BE")
    void leadingBom_isSkipped() throws IOException {
        assertThat(read(concat(UTF8_BOM, utf8("\"a\",b\n1,2\n")), StandardCharsets.UTF_8)).isEqualTo("\"a\",b\n1,2\n");
        assertThat(read(concat(new byte[]{(byte) 0xFF, (byte) 0xFE}, "a,b\n".getBytes(StandardCharsets.UTF_16LE)),
                StandardCharsets.UTF_16LE)).isEqualTo("a,b\n");
        assertThat(read(concat(new byte[]{(byte) 0xFE, (byte) 0xFF}, "a,b\n".getBytes(StandardCharsets.UTF_16BE)),
                StandardCharsets.UTF_16BE)).isEqualTo("a,b\n");
    }

    @Test
    @DisplayName("TC-CSV-038: Text without a leading byte order mark is read unchanged, including later byte order marks")
    void textWithoutLeadingBom_isUnchanged() throws IOException {
        assertThat(read(utf8("a,b\n1,2\n"), StandardCharsets.UTF_8)).isEqualTo("a,b\n1,2\n");
        assertThat(read(utf8("x"), StandardCharsets.UTF_8)).isEqualTo("x");
        assertThat(read(utf8("a\uFEFF,b"), StandardCharsets.UTF_8)).isEqualTo("a\uFEFF,b");
        // only one byte order mark is skipped
        assertThat(read(concat(UTF8_BOM, UTF8_BOM), StandardCharsets.UTF_8)).isEqualTo("\uFEFF");
    }

    @Test
    @DisplayName("TC-CSV-039: An empty file and a file consisting of a byte order mark only are read as empty text")
    void emptyAndBomOnlyFile_areEmpty() throws IOException {
        assertThat(read(new byte[0], StandardCharsets.UTF_8)).isEmpty();
        assertThat(read(UTF8_BOM, StandardCharsets.UTF_8)).isEmpty();
        assertThat(read(concat(UTF8_BOM, utf8("\n")), StandardCharsets.UTF_8)).isEqualTo("\n");
    }

    @Test
    @DisplayName("TC-CSV-040: Malformed input is reported, also in the first character")
    void malformedInput_isReported() throws IOException {
        Path first = write(new byte[]{(byte) 0xC3, (byte) 0x28});
        assertThatThrownBy(() -> DatasetUtil.newReaderSkippingBom(first, StandardCharsets.UTF_8))
                .isInstanceOf(MalformedInputException.class);
        Path later = write(concat(concat(UTF8_BOM, utf8("a".repeat(10000))), new byte[]{(byte) 0xFF}));
        assertThatThrownBy(() -> read(Files.readAllBytes(later), StandardCharsets.UTF_8))
                .isInstanceOf(MalformedInputException.class);
    }

    private String read(byte[] content, Charset charset) throws IOException {
        Path file = write(content);
        StringBuilder sb = new StringBuilder();
        try (Reader reader = DatasetUtil.newReaderSkippingBom(file, charset)) {
            char[] buf = new char[3];
            int n;
            while ((n = reader.read(buf)) != -1) {
                sb.append(buf, 0, n);
            }
        }
        return sb.toString();
    }

    private Path write(byte[] content) throws IOException {
        return Files.write(Files.createTempFile(tempDir, "bom", ".csv"), content);
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] concat(byte[] a, byte[] b) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(a);
        out.writeBytes(b);
        return out.toByteArray();
    }

}
