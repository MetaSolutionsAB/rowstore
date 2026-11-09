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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link DatasetUtil#writeTempFile(InputStream, long)}: the temp file of an upload that cannot be
 * written completely is deleted. The path of the temp file is taken from the log, since the method throws instead of
 * returning it. Also checks that the charset detection of an empty temp file does not fail.
 * Specification: ETL-9.01, ETL-9.02
 */
@DisplayName("DatasetUtil Temp File Tests")
class DatasetUtilTempFileTest {

    private static final String PATH_PREFIX = "Writing request body to temporary file at ";

    private CapturingAppender appender;

    private Level previousLevel;

    @BeforeEach
    void setUp() {
        appender = new CapturingAppender();
        appender.start();
        previousLevel = logger().getLevel();
        Configurator.setLevel(logger().getName(), Level.INFO);
        logger().addAppender(appender);
    }

    @AfterEach
    void tearDown() {
        logger().removeAppender(appender);
        Configurator.setLevel(logger().getName(), previousLevel);
        appender.stop();
    }

    @Test
    void completeUpload_isWritten() throws IOException {
        File file = DatasetUtil.writeTempFile(new ByteArrayInputStream(new byte[100]), 100);
        try {
            assertThat(file).hasSize(100);
            assertThat(tempFile()).isEqualTo(file.toPath());
        } finally {
            Files.deleteIfExists(file.toPath());
        }
    }

    @Test
    void tooLargeUpload_leavesNoTempFile() {
        assertThatThrownBy(() -> DatasetUtil.writeTempFile(new ByteArrayInputStream(new byte[101]), 100))
                .isInstanceOf(DatasetUtil.PayloadTooLargeException.class);
        assertThat(tempFile()).doesNotExist();
    }

    @Test
    void failedRead_leavesNoTempFile() {
        IOException failure = new IOException("Unexpected end of ZLIB input stream");
        InputStream failing = new InputStream() {
            private int count;

            @Override
            public int read() throws IOException {
                if (++count > 10) {
                    throw failure;
                }
                return 'a';
            }
        };

        assertThatThrownBy(() -> DatasetUtil.writeTempFile(failing, 100)).isSameAs(failure);
        assertThat(tempFile()).doesNotExist();
    }

    @Test
    void charsetOfEmptyFile_isUtf8() throws IOException {
        // e.g., an upload that is empty after decoding its Content-Encoding; this threw NegativeArraySizeException
        Path empty = Files.createTempFile("RowStoreTest", ".csv");
        try {
            assertThat(DatasetUtil.detectCharset(empty.toFile())).isEqualTo(StandardCharsets.UTF_8);
        } finally {
            Files.delete(empty);
        }
    }

    private Path tempFile() {
        List<String> paths = appender.events.stream().map(e -> e.getMessage().getFormattedMessage())
                .filter(m -> m.startsWith(PATH_PREFIX)).map(m -> m.substring(PATH_PREFIX.length())).toList();
        assertThat(paths).hasSize(1);
        return Path.of(paths.getFirst());
    }

    private static Logger logger() {
        return (Logger) LogManager.getLogger(DatasetUtil.class);
    }

    private static final class CapturingAppender extends AbstractAppender {
        final List<LogEvent> events = new CopyOnWriteArrayList<>();

        CapturingAppender() {
            super("capturing", null, null, true, Property.EMPTY_ARRAY);
        }

        @Override
        public void append(LogEvent event) {
            events.add(event.toImmutable());
        }
    }

}
