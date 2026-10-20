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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.Property;
import org.entrystore.rowstore.store.RowStoreConfig;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for client address resolution from X-Forwarded-For in {@link TrustedProxyFilter}.
 * Specification: SEC-5.05, SEC-5.06
 */
@DisplayName("TrustedProxyFilter Tests")
class TrustedProxyFilterTest {

    /**
     * @param trustedProxies "default" omits the key, "none" configures an empty array, otherwise space-separated entries
     * @param xForwardedFor X-Forwarded-For header values separated by "|", each sent as a separate header line
     */
    @ParameterizedTest(name = "[{index}] trusted=''{0}'', peer={1}, XFF=''{2}'' -> {3}")
    @CsvSource(delimiter = ';', nullValues = "null", value = {
            // Default trust list (loopback, private ranges, IPv6 loopback/ULA/link-local)
            "default;            127.0.0.1;   203.0.113.7;                      203.0.113.7",
            "default;            10.1.2.3;    203.0.113.7;                      203.0.113.7",
            "default;            172.31.0.1;  203.0.113.7;                      203.0.113.7",
            "default;            ::1;         203.0.113.7;                      203.0.113.7",
            "default;            fd00::5;     2001:db8::1;                      2001:db8::1",
            "default;            172.32.0.1;  203.0.113.7;                      172.32.0.1",
            "default;            198.51.100.1;203.0.113.7;                      198.51.100.1",
            // Explicit empty array: X-Forwarded-For is always ignored
            "none;               127.0.0.1;   203.0.113.7;                      127.0.0.1",
            // Untrusted peer: header ignored, even if it claims a trusted address
            "192.0.2.10;         192.0.2.11;  203.0.113.7;                      192.0.2.11",
            // Client-supplied left part is ignored, the entry appended by the trusted proxy wins
            "192.0.2.10;         192.0.2.10;  6.6.6.6, 203.0.113.7;             203.0.113.7",
            // Trusted hops (CIDR) are skipped from the right until the first untrusted address
            "192.0.2.0/24;       192.0.2.10;  6.6.6.6, 203.0.113.7, 192.0.2.20; 203.0.113.7",
            // Several header lines are combined in order
            "192.0.2.10;         192.0.2.10;  6.6.6.6|203.0.113.7;              203.0.113.7",
            // All hops trusted: the leftmost one is the client
            "192.0.2.0/24;       192.0.2.10;  192.0.2.30, 192.0.2.20;           192.0.2.30",
            // Non-IP entries are never resolved and count as untrusted
            "192.0.2.10;         192.0.2.10;  unknown;                          unknown",
            "default;            127.0.0.1;   6.6.6.6, localhost;               localhost",
            // IPv6 zone IDs are not parsed (no interface lookup, no exception) and count as untrusted
            "default;            127.0.0.1;   ::1%zz;                           ::1%zz",
            "default;            127.0.0.1;   203.0.113.9, ::1%zz;              ::1%zz",
            "default;            127.0.0.1;   203.0.113.9, fe80::1%lo;          fe80::1%lo",
            // Entries with a port are not IP literals and count as untrusted
            "default;            127.0.0.1;   203.0.113.9, 10.0.0.1:4711;       10.0.0.1:4711",
            // Trusted peer without header
            "192.0.2.10;         192.0.2.10;  null;                             192.0.2.10",
            "192.0.2.10;         192.0.2.10;  ' , ';                            192.0.2.10"
    })
    @DisplayName("TC-RATE-011: Client address is resolved from X-Forwarded-For only via trusted proxies")
    void resolvesClientAddress(String trustedProxies, String peer, String xForwardedFor, String expected)
            throws Exception {
        TrustedProxyFilter filter = new TrustedProxyFilter(config(trustedProxies));
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/datasets");
        request.setRemoteAddr(peer);
        if (xForwardedFor != null) {
            for (String value : xForwardedFor.split("\\|")) {
                request.addHeader("X-Forwarded-For", value);
            }
        }
        MockFilterChain chain = new MockFilterChain();

        filter.doFilter(request, new MockHttpServletResponse(), chain);

        assertThat(chain.getRequest().getRemoteAddr()).isEqualTo(expected);
        assertThat(chain.getRequest().getRemoteHost()).isEqualTo(
                expected.equals(peer) ? request.getRemoteHost() : expected);
    }

    @Test
    @DisplayName("TC-RATE-012: X-Forwarded-For from an untrusted peer is logged as a warning once per peer")
    void untrustedPeerWithForwardedFor_warnsOncePerPeer() throws Exception {
        List<String> warnings = new ArrayList<>();
        AbstractAppender appender = new AbstractAppender("capture", null, null, true, Property.EMPTY_ARRAY) {
            @Override
            public void append(LogEvent event) {
                if (event.getLevel() == Level.WARN) {
                    warnings.add(event.getMessage().getFormattedMessage());
                }
            }
        };
        appender.start();
        Logger logger = (Logger) LogManager.getLogger(TrustedProxyFilter.class);
        Level previousLevel = logger.getLevel();
        Configurator.setLevel(logger.getName(), Level.WARN);
        logger.addAppender(appender);
        try {
            TrustedProxyFilter filter = new TrustedProxyFilter(config("192.0.2.10"));
            send(filter, "198.51.100.1", "203.0.113.7");
            send(filter, "198.51.100.1", "203.0.113.8");
            send(filter, "198.51.100.2", null);
            send(filter, "192.0.2.10", "203.0.113.7");
            assertThat(warnings).hasSize(1);
            assertThat(warnings.getFirst()).contains("198.51.100.1", "trustedproxies");

            send(filter, "198.51.100.2", "203.0.113.7");
            assertThat(warnings).hasSize(2);

            // No warnings when nobody is trusted on purpose
            send(new TrustedProxyFilter(config("none")), "198.51.100.3", "203.0.113.7");
            assertThat(warnings).hasSize(2);
        } finally {
            logger.removeAppender(appender);
            Configurator.setLevel(logger.getName(), previousLevel);
        }
    }

    private static void send(TrustedProxyFilter filter, String peer, String xForwardedFor) throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/datasets");
        request.setRemoteAddr(peer);
        if (xForwardedFor != null) {
            request.addHeader("X-Forwarded-For", xForwardedFor);
        }
        filter.doFilter(request, new MockHttpServletResponse(), new MockFilterChain());
    }

    private static RowStoreConfig config(String trustedProxies) {
        JSONObject json = new JSONObject("""
                {
                    "baseurl": "http://localhost:8282/",
                    "database": { "host": "localhost", "database": "rowstore", "user": "rowstore", "password": "pw" }
                }
                """);
        if ("none".equals(trustedProxies)) {
            json.put("trustedproxies", new JSONArray());
        } else if (!"default".equals(trustedProxies)) {
            json.put("trustedproxies", new JSONArray(Arrays.asList(trustedProxies.split(" +"))));
        }
        return new RowStoreConfig(json);
    }

}
