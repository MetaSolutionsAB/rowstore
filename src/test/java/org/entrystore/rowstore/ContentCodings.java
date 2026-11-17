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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Encodes request bodies with the content codings that RowStore decodes (API-3.17).
 */
public final class ContentCodings {

    private ContentCodings() {
    }

    /**
     * Encodes data with the given codings, in the order of the Content-Encoding header.
     */
    public static byte[] encode(byte[] data, String contentEncoding) throws IOException {
        byte[] result = data;
        for (String coding : contentEncoding.split(",")) {
            result = encodeOnce(result, coding.trim().toLowerCase());
        }
        return result;
    }

    private static byte[] encodeOnce(byte[] data, String coding) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        switch (coding) {
            case "gzip", "x-gzip" -> {
                try (GZIPOutputStream out = new GZIPOutputStream(bytes)) {
                    out.write(data);
                }
            }
            case "deflate" -> {
                try (DeflaterOutputStream out = new DeflaterOutputStream(bytes)) {
                    out.write(data);
                }
            }
            case "deflate-no-wrap" -> {
                try (DeflaterOutputStream out = new DeflaterOutputStream(bytes, new Deflater(Deflater.DEFAULT_COMPRESSION, true))) {
                    out.write(data);
                }
            }
            case "zip" -> {
                try (ZipOutputStream out = new ZipOutputStream(bytes)) {
                    out.putNextEntry(new ZipEntry("data.csv"));
                    out.write(data);
                    out.closeEntry();
                    // a second entry is ignored, as in 1.x
                    out.putNextEntry(new ZipEntry("other.csv"));
                    out.write("other\n".getBytes(StandardCharsets.UTF_8));
                    out.closeEntry();
                }
            }
            case "identity" -> bytes.write(data);
            default -> throw new IllegalArgumentException(coding);
        }
        return bytes.toByteArray();
    }

}
