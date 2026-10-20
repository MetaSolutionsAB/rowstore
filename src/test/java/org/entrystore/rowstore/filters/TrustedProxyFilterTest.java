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

import org.entrystore.rowstore.store.RowStoreConfig;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.util.Arrays;

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
