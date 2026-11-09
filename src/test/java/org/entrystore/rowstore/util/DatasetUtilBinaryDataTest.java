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

import org.entrystore.rowstore.ContentCodings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the detection of binary data in uploads, {@link DatasetUtil#isCompressed(java.io.File)} and
 * {@link DatasetUtil#containsControlCharacter(String)}, and that text files in the charsets that the charset detection
 * recognizes do not yield control characters in their header (ETL-4.10).
 */
class DatasetUtilBinaryDataTest {

    private static final String CSV = "name,värde,größe\nAnna,1,x\n";

    @TempDir
    Path tempDir;

    @Test
    void compressedData_isDetectedBySignature() throws IOException {
        byte[] csv = CSV.getBytes(StandardCharsets.UTF_8);

        assertThat(isCompressed(ContentCodings.encode(csv, "gzip"))).isTrue();
        assertThat(isCompressed(ContentCodings.encode(csv, "zip"))).isTrue();
        assertThat(isCompressed(csv)).isFalse();
        assertThat(isCompressed(ContentCodings.encode(csv, "deflate"))).isFalse();
        assertThat(isCompressed("PK,name\n1,2\n".getBytes(StandardCharsets.UTF_8))).isFalse();
        assertThat(isCompressed(new byte[]{0x1f})).isFalse();
        assertThat(isCompressed(new byte[0])).isFalse();
    }

    @Test
    void controlCharacters_otherThanTabAndLineBreaks() {
        assertThat(DatasetUtil.containsControlCharacter("a\0b")).isTrue();
        assertThat(DatasetUtil.containsControlCharacter("name\u001b")).isTrue();
        assertThat(DatasetUtil.containsControlCharacter("a\tb\r\n")).isFalse();
        // C1 control characters, e.g., from the bytes of Windows-1252 that are not assigned
        assertThat(DatasetUtil.containsControlCharacter("a\u0081\u008db")).isFalse();
        assertThat(DatasetUtil.containsControlCharacter("﻿namn,größe­")).isFalse();
    }

    @Test
    void headerOfDetectedCharsets_hasNoControlCharacters() throws IOException {
        assertThat(header(concat(new byte[]{(byte) 0xFF, (byte) 0xFE}, CSV.getBytes(StandardCharsets.UTF_16LE))))
                .isEqualTo("name,värde,größe");
        assertThat(header(CSV.getBytes(StandardCharsets.UTF_16))).isEqualTo("name,värde,größe");
        // without byte order mark, UTF-16 is recognized for ASCII text only
        assertThat(header("name,value\nA,1\n".getBytes(StandardCharsets.UTF_16LE))).isEqualTo("name,value");
        assertThat(header("name,value\nA,1\n".getBytes(StandardCharsets.UTF_16BE))).isEqualTo("name,value");
        assertThat(header("pris €,namn\n1,Ö\n".getBytes(Charset.forName("windows-1252")))).isEqualTo("pris €,namn");
        assertThat(header("Name\tCity\nÅsa\tLund\n".getBytes(StandardCharsets.ISO_8859_1))).isEqualTo("Name\tCity");
    }

    private boolean isCompressed(byte[] data) throws IOException {
        return DatasetUtil.isCompressed(write(data).toFile());
    }

    /**
     * @return Returns the first line as read by the load, after asserting that it has no control characters.
     */
    private String header(byte[] data) throws IOException {
        Path file = write(data);
        Charset charset = DatasetUtil.detectCharset(file.toFile());
        try (BufferedReader reader = new BufferedReader(DatasetUtil.newReaderSkippingBom(file, charset))) {
            String header = reader.readLine();
            assertThat(DatasetUtil.containsControlCharacter(header)).as(charset.name()).isFalse();
            return header;
        }
    }

    private Path write(byte[] data) throws IOException {
        return Files.write(Files.createTempFile(tempDir, "upload", ".csv"), data);
    }

    private static byte[] concat(byte[] a, byte[] b) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(a);
        out.writeBytes(b);
        return out.toByteArray();
    }

}
