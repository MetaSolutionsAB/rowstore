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

package org.entrystore.rowstore.filters;

import jakarta.servlet.AsyncContext;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.Property;
import org.entrystore.rowstore.store.RowStoreConfig;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link AccessLogFilter}. Mock responses do not report the number of bytes written, so the response
 * size is empty here; it is checked against Tomcat in AccessLogIT.
 * Specification: DEPL-6.02, CFG-3.12, test specification specs/tests/13-deployment.md
 */
class AccessLogFilterTest {

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
    @DisplayName("TC-DEPL-006: A request is logged in the format of 1.x with the client address from X-Forwarded-For")
    void request_isLoggedInFormatOf1x() throws Exception {
        MockHttpServletRequest request = request("GET", "/dataset/abc", "name=Bo&_limit=10");
        request.addHeader("X-Forwarded-For", "6.6.6.6, 203.0.113.7");
        request.addHeader("User-Agent", "curl/8.5.0");
        request.addHeader("Referer", "https://example.com/page");

        filter(true).doFilter(request, new MockHttpServletResponse(), (req, res) -> ((HttpServletResponse) res).setStatus(404));

        assertThat(lines()).containsExactly("203.0.113.7 \"GET /dataset/abc name=Bo&_limit=10\" 404  0 " +
                "http://rowstore.example.com curl/8.5.0 https://example.com/page");
    }

    @Test
    @DisplayName("TC-DEPL-007: Missing query, headers and body give empty fields, a request body its size")
    void missingParts_giveEmptyFields() throws Exception {
        MockHttpServletRequest get = request("GET", "/status", null);
        filter(true).doFilter(get, new MockHttpServletResponse(), (req, res) -> { });
        MockHttpServletRequest post = request("POST", "/datasets", null);
        post.setContent("a,b\n1,2\n".getBytes());
        filter(true).doFilter(post, new MockHttpServletResponse(), (req, res) -> ((HttpServletResponse) res).setStatus(202));
        MockHttpServletRequest chunked = request("POST", "/datasets", null);
        chunked.addHeader("Transfer-Encoding", "chunked");
        filter(true).doFilter(chunked, new MockHttpServletResponse(), (req, res) -> ((HttpServletResponse) res).setStatus(202));

        assertThat(lines()).containsExactly(
                "127.0.0.1 \"GET /status \" 200  0 http://rowstore.example.com  ",
                "127.0.0.1 \"POST /datasets \" 202  8 http://rowstore.example.com  ",
                "127.0.0.1 \"POST /datasets \" 202  - http://rowstore.example.com  ");
    }

    @Test
    @DisplayName("TC-DEPL-008: An asynchronous response is logged when it is complete")
    void asyncResponse_isLoggedWhenComplete() throws Exception {
        MockHttpServletRequest request = request("GET", "/dataset/abc/export", null);
        request.setAsyncSupported(true);
        MockHttpServletResponse response = new MockHttpServletResponse();

        filter(true).doFilter(request, response, (req, res) -> req.startAsync());
        assertThat(lines()).isEmpty();

        AsyncContext asyncContext = request.getAsyncContext();
        response.setStatus(200);
        asyncContext.complete();
        assertThat(lines()).containsExactly("127.0.0.1 \"GET /dataset/abc/export \" 200  0 http://rowstore.example.com  ");
    }

    @Test
    @DisplayName("TC-DEPL-009: A failing request is logged with 500, a disabled access log logs nothing")
    void failingRequest_isLoggedWith500_disabledLogsNothing() throws Exception {
        assertThatThrownBy(() -> filter(true).doFilter(request("GET", "/datasets", null), new MockHttpServletResponse(),
                (req, res) -> { throw new ServletException("simulated failure"); }))
                .isInstanceOf(ServletException.class);
        assertThat(lines()).containsExactly("127.0.0.1 \"GET /datasets \" 500  0 http://rowstore.example.com  ");

        filter(false).doFilter(request("GET", "/datasets", null), new MockHttpServletResponse(), (req, res) -> { });
        assertThat(lines()).hasSize(1);
    }

    private static MockHttpServletRequest request(String method, String uri, String query) {
        MockHttpServletRequest request = new MockHttpServletRequest(method, uri);
        request.setQueryString(query);
        request.setRemoteAddr("127.0.0.1");
        request.addHeader("Host", "rowstore.example.com");
        return request;
    }

    private static AccessLogFilter filter(boolean enabled) {
        RowStoreConfig config = new RowStoreConfig(new JSONObject()
                .put("baseurl", "http://localhost/")
                .put("accesslog", enabled)
                .put("database", new JSONObject()
                        .put("host", "localhost")
                        .put("database", "unused")
                        .put("user", "unused")
                        .put("password", "unused")));
        return new AccessLogFilter(config, new TrustedProxyFilter(config));
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
