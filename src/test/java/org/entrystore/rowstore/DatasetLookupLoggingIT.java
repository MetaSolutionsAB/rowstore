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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Requests for unknown or malformed dataset IDs are client errors that are answered with 404 and recorded by the
 * access log; they must not produce log messages at WARN or ERROR level (ROWS-169). The tests read the log of the
 * in-process server and are skipped when testing an external instance.
 * Specification: API-6.06, test specification specs/tests/08-error-handling.md
 */
@DisplayName("Dataset Lookup Logging Tests")
class DatasetLookupLoggingIT extends BaseIntegrationTest {

    /**
     * Name prefix of the threads that handle requests in the in-process server (Tomcat with virtual threads).
     */
    private static final String REQUEST_THREAD_PREFIX = "tomcat-handler-";

    private CapturingAppender appender;

    @BeforeEach
    void setUp() {
        String externalUrl = System.getProperty("rowstore.baseUrl");
        assumeTrue(externalUrl == null || externalUrl.isEmpty(), "The log of an external RowStore cannot be read");
        appender = new CapturingAppender();
        appender.start();
        rootLogger().addAppender(appender);
    }

    @AfterEach
    void tearDown() {
        if (appender != null) {
            rootLogger().removeAppender(appender);
            appender.stop();
        }
    }

    @Test
    @DisplayName("TC-ERROR-022: Unknown and malformed dataset IDs return 404 without log messages at WARN or ERROR level")
    void unknownAndMalformedIds_return404WithoutWarnings() {
        String unknown = UUID.randomUUID().toString();
        List<String> paths = List.of(
                "/dataset/not-a-uuid",
                "/dataset/wp-login.php",
                "/dataset/a%2Fb",
                "/dataset/" + unknown,
                "/dataset/" + unknown.toUpperCase(),
                "/dataset/" + unknown + "/json",
                "/dataset/" + unknown + "/info",
                "/dataset/" + unknown + "/export",
                "/dataset/" + unknown + "/aliases",
                "/dataset/" + unknown + "/swagger",
                "/dataset/" + unknown + "/html",
                "/dataset/" + unknown.substring(0, 35) + "x",
                "/dataset/%C3%A5%C3%A4%C3%B6");
        for (String path : paths) {
            // raw requests, since REST Assured would re-encode the path
            assertThat(TestUtils.rawGet(getBaseUrl(), path, "Accept: application/json").status()).as("GET " + path)
                    .isEqualTo(404);
        }
        assertThat(TestUtils.rawRequest(getBaseUrl(), "DELETE", "/dataset/" + unknown, List.of(), null).status())
                .as("DELETE /dataset/" + unknown).isEqualTo(404);

        // only messages of request threads, so that background work of earlier test classes (e.g., ETL jobs) cannot
        // make the test fail; the access log lines show that the messages of request threads are captured
        List<LogEvent> requestEvents = appender.events.stream()
                .filter(e -> e.getThreadName() != null && e.getThreadName().startsWith(REQUEST_THREAD_PREFIX)).toList();
        assertThat(requestEvents).as("log messages of request threads").isNotEmpty();
        assertThat(requestEvents.stream().filter(e -> e.getLevel().isMoreSpecificThan(Level.WARN))
                .map(e -> e.getLevel() + " " + e.getLoggerName() + " - " + e.getMessage().getFormattedMessage()).toList())
                .as("log messages at WARN or ERROR level").isEmpty();
    }

    private static Logger rootLogger() {
        return (Logger) LogManager.getRootLogger();
    }

    private static final class CapturingAppender extends AbstractAppender {
        final List<LogEvent> events = new CopyOnWriteArrayList<>();

        CapturingAppender() {
            super("capturing-lookups", null, null, true, Property.EMPTY_ARRAY);
        }

        @Override
        public void append(LogEvent event) {
            events.add(event.toImmutable());
        }
    }

}
