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

import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the method and media type tunnel of {@link TunnelFilter}.
 * Specification: API-4.05, API-4.06
 */
@DisplayName("TunnelFilter Tests")
class TunnelFilterTest {

    private static HttpServletRequest filter(MockHttpServletRequest request, MockHttpServletResponse response) throws Exception {
        MockFilterChain chain = new MockFilterChain();
        new TunnelFilter().doFilter(request, response, chain);
        return (HttpServletRequest) chain.getRequest();
    }

    private static MockHttpServletRequest request(String method, String query) {
        MockHttpServletRequest request = new MockHttpServletRequest(method, "/dataset/x");
        request.setQueryString(query);
        return request;
    }

    @ParameterizedTest(name = "[{index}] {0} ?{1} with header {2} -> {3}")
    @CsvSource(nullValues = "null", value = {
            "POST, method=PUT,          null,   PUT",
            "POST, method=put,          null,   PUT",
            "POST, method=DELETE,       null,   DELETE",
            "POST, method=GET,          null,   GET",
            "POST, method=POST,         null,   POST",
            "POST, null,                DELETE, DELETE",
            // the header takes precedence over the query parameter, as in 1.x
            "POST, method=PUT,          DELETE, DELETE",
            "POST, a=1&method=PUT&b=2,  null,   PUT",
            "POST, method=%50UT,        null,   PUT",
            "POST, null,                null,   POST",
            "POST, method=,             null,   POST",
            // other methods are never tunnelled: a GET must not become a DELETE behind the proxy (SEC-9.02)
            "GET,  method=DELETE,       null,   GET",
            "GET,  null,                DELETE, GET",
            "PUT,  method=DELETE,       DELETE, PUT",
            "HEAD, null,                DELETE, HEAD"
    })
    @DisplayName("Method tunnel applies to POST only")
    void methodTunnel(String method, String query, String header, String expected) throws Exception {
        MockHttpServletRequest request = request(method, query);
        if (header != null) {
            request.addHeader(TunnelFilter.METHOD_HEADER, header);
        }
        MockHttpServletResponse response = new MockHttpServletResponse();
        HttpServletRequest filtered = filter(request, response);
        assertThat(response.getStatus()).isEqualTo(200);
        assertThat(filtered.getMethod()).isEqualTo(expected);
    }

    @ParameterizedTest(name = "[{index}] ?{0} / header {1}")
    @CsvSource(nullValues = "null", value = {
            "method=PATCH, null",
            "method=FOO,   null",
            "null,         PATCH",
            "null,         TRACE"
    })
    @DisplayName("Unsupported tunnelled methods are rejected with 400 instead of being processed as POST")
    void methodTunnel_unsupportedMethod_isRejected(String query, String header) throws Exception {
        MockHttpServletRequest request = request("POST", query);
        if (header != null) {
            request.addHeader(TunnelFilter.METHOD_HEADER, header);
        }
        MockHttpServletResponse response = new MockHttpServletResponse();
        filter(request, response);
        assertThat(response.getStatus()).isEqualTo(400);
    }

    @Test
    @DisplayName("The method parameter is read from the query string, not from a form body")
    void methodTunnel_ignoresFormBody() throws Exception {
        MockHttpServletRequest request = request("POST", null);
        request.setContentType("application/x-www-form-urlencoded");
        request.addParameter("method", "DELETE");
        HttpServletRequest filtered = filter(request, new MockHttpServletResponse());
        assertThat(filtered.getMethod()).isEqualTo("POST");
    }

    @ParameterizedTest(name = "[{index}] ?{0} -> {1}")
    @CsvSource(nullValues = "null", value = {
            "media=csv,                    text/csv",
            "media=CSV,                    text/csv",
            "media=json,                   application/json",
            "media=html,                   text/html",
            "media=txt,                    text/plain",
            "media=text/csv,               text/csv",
            "media=text%2Fcsv,             text/csv",
            "media=application/ld%2Bjson,  application/ld+json",
            "name=x&media=csv&_limit=2,    text/csv",
            // unknown values leave the Accept header unchanged
            "media=foo,                    application/json",
            "media=,                       application/json",
            "media=a/b/c,                  application/json",
            "null,                         application/json"
    })
    @DisplayName("Media parameter replaces the Accept header")
    void mediaTunnel(String query, String expectedAccept) throws Exception {
        for (String method : new String[]{"GET", "POST"}) {
            MockHttpServletRequest request = request(method, query);
            request.addHeader("Accept", "application/json");
            HttpServletRequest filtered = filter(request, new MockHttpServletResponse());
            assertThat(filtered.getHeader("Accept")).isEqualTo(expectedAccept);
            assertThat(Collections.list(filtered.getHeaders("Accept"))).containsExactly(expectedAccept);
        }
    }

    @Test
    @DisplayName("Media parameter sets the Accept header if the request has none")
    void mediaTunnel_withoutAcceptHeader() throws Exception {
        HttpServletRequest filtered = filter(request("GET", "media=csv"), new MockHttpServletResponse());
        assertThat(filtered.getHeader("accept")).isEqualTo("text/csv");
        assertThat(Collections.list(filtered.getHeaderNames())).contains("Accept");
    }

    @Test
    @DisplayName("firstQueryValue decodes names and values and skips malformed pairs")
    void firstQueryValue() {
        assertThat(TunnelFilter.firstQueryValue("a=1&media=csv&media=json", "media")).isEqualTo("csv");
        assertThat(TunnelFilter.firstQueryValue("med%69a=text%2Fcsv", "media")).isEqualTo("text/csv");
        assertThat(TunnelFilter.firstQueryValue("media", "media")).isEmpty();
        assertThat(TunnelFilter.firstQueryValue("x=100%&media=csv", "media")).isEqualTo("csv");
        assertThat(TunnelFilter.firstQueryValue("media=%zz", "media")).isNull();
        assertThat(TunnelFilter.firstQueryValue(null, "media")).isNull();
    }

}
