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

import io.restassured.response.Response;
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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration tests for the access log against the embedded Tomcat: response sizes, streamed (asynchronous)
 * responses and requests rejected by the first filters. They read the log of the in-process server and are skipped
 * when testing an external instance.
 * Specification: DEPL-6.02, test specification specs/tests/13-deployment.md
 */
@DisplayName("Access Log Tests")
class AccessLogIT extends BaseIntegrationTest {

    private static final String CLIENT = "203.0.113.7";

    private CapturingAppender appender;

    private Level previousLevel;

    @BeforeEach
    void setUp() {
        String externalUrl = System.getProperty("rowstore.baseUrl");
        assumeTrue(externalUrl == null || externalUrl.isEmpty(), "The log of an external RowStore cannot be read");
        appender = new CapturingAppender();
        appender.start();
        previousLevel = logger().getLevel();
        Configurator.setLevel(logger().getName(), Level.INFO);
        logger().addAppender(appender);
    }

    @AfterEach
    void tearDown() {
        if (appender != null) {
            logger().removeAppender(appender);
            Configurator.setLevel(logger().getName(), previousLevel);
            appender.stop();
        }
    }

    @Test
    @DisplayName("TC-DEPL-010: Requests are logged with the client address, status and the size of the response body")
    void requests_areLoggedWithClientAddressStatusAndSize() {
        Response status = given().header("X-Forwarded-For", "6.6.6.6, " + CLIENT).header("User-Agent", "access-log-test")
                .get("/status");
        assertThat(status.statusCode()).isEqualTo(200);
        int statusBytes = status.asByteArray().length;

        // rejected by PathParameterFilter, which runs before TrustedProxyFilter
        int rejected = given().urlEncodingEnabled(false).header("X-Forwarded-For", CLIENT).get("/status;x").statusCode();
        assertThat(rejected).isEqualTo(400);

        await().atMost(MAX_WAIT).pollInterval(POLL_INTERVAL).untilAsserted(() -> assertThat(lines())
                .anySatisfy(l -> assertThat(l).isEqualTo(CLIENT + " \"GET /status \" 200 " + statusBytes + " 0 " +
                        getBaseUrl().replaceAll("/+$", "") + " access-log-test "))
                .anySatisfy(l -> assertThat(l).startsWith(CLIENT + " \"GET /status;x \" 400 ")));
    }

    @Test
    @DisplayName("TC-DEPL-011: A streamed export is logged when it is complete, with the size of the streamed body")
    void streamedExport_isLoggedWhenComplete() {
        DatasetUrls urls = createDatasetAndWait(loadTestData("dataset1_utf8.csv"));
        try {
            Response export = given().header("X-Forwarded-For", CLIENT).accept("text/csv").get(urls.datasetUrl + "/export");
            assertThat(export.statusCode()).isEqualTo(200);
            int exportBytes = export.asByteArray().length;
            assertThat(exportBytes).isGreaterThan(0);
            String path = urls.datasetUrl.substring(getBaseUrl().replaceAll("/+$", "").length()) + "/export";

            await().atMost(MAX_WAIT).pollInterval(POLL_INTERVAL).untilAsserted(() -> assertThat(lines())
                    .anySatisfy(l -> assertThat(l).startsWith(CLIENT + " \"GET " + path + " \" 200 " + exportBytes + " 0 ")));
        } finally {
            deleteDataset(urls.datasetUrl);
        }
    }

    private List<String> lines() {
        return appender.events.stream().map(e -> e.getMessage().getFormattedMessage()).toList();
    }

    private static Logger logger() {
        return (Logger) LogManager.getLogger("org.entrystore.rowstore.LogService");
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
