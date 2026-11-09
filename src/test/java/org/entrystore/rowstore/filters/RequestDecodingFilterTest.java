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
import org.entrystore.rowstore.ContentCodings;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link RequestDecodingFilter}; the uploads themselves are covered by CompressedUploadIT.
 * Specification: API-3.17
 */
@DisplayName("RequestDecodingFilter Tests")
class RequestDecodingFilterTest {

    static final byte[] CSV = "name,value\nA,1\nB,2\n".getBytes(StandardCharsets.UTF_8);

    @ParameterizedTest(name = "[{index}] {0}")
    @ValueSource(strings = {"gzip", "x-gzip", "GZip", "deflate", "deflate-no-wrap", "zip", "identity", "gzip, identity",
            "identity,gzip", "deflate, gzip"})
    void decodesSupportedCodings(String contentEncoding) throws Exception {
        HttpServletRequest decoded = filter(request(contentEncoding, ContentCodings.encode(CSV, contentEncoding)), 200);

        assertThat(decoded.getInputStream().readAllBytes()).isEqualTo(CSV);
        assertThat(decoded.getHeader("Content-Encoding")).isNull();
        assertThat(decoded.getHeader("content-length")).isNull();
        assertThat(decoded.getContentLength()).isEqualTo(-1);
        assertThat(decoded.getContentLengthLong()).isEqualTo(-1);
        assertThat(Collections.list(decoded.getHeaderNames())).doesNotContain("Content-Encoding", "Content-Length")
                .contains("Content-Type");
        assertThat(Collections.list(decoded.getHeaders("Content-Encoding"))).isEmpty();
    }

    @Test
    void decodesCodingsOfSeveralHeaderLines() throws Exception {
        MockHttpServletRequest request = request("deflate", ContentCodings.encode(CSV, "deflate, gzip"));
        request.addHeader("Content-Encoding", "gzip");

        assertThat(filter(request, 200).getInputStream().readAllBytes()).isEqualTo(CSV);
    }

    @Test
    void readerDecodesBody() throws Exception {
        MockHttpServletRequest request = request("gzip", ContentCodings.encode(CSV, "gzip"));
        request.setCharacterEncoding("UTF-8");

        assertThat(filter(request, 200).getReader().readLine()).isEqualTo("name,value");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @ValueSource(strings = {"br", "zstd", "compress", "gzip, br"})
    void rejectsUnsupportedCodings(String contentEncoding) throws Exception {
        filter(request(contentEncoding, CSV), 415);
    }

    @Test
    void passesRequestWithoutBodyUnchanged() throws Exception {
        MockHttpServletRequest request = request("br", null);

        assertThat(filter(request, 200)).isSameAs(request);
    }

    @Test
    void passesRequestWithoutContentEncodingUnchanged() throws Exception {
        MockHttpServletRequest request = request(null, CSV);

        assertThat(filter(request, 200)).isSameAs(request);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @ValueSource(strings = {"gzip", "deflate", "deflate-no-wrap", "zip"})
    void reportsCorruptDataAsMalformed(String contentEncoding) throws Exception {
        byte[] encoded = ContentCodings.encode(CSV, contentEncoding);
        // cut within the compressed data of the first entry of a zip archive (local header of 38 bytes)
        byte[] truncated = Arrays.copyOf(encoded, Math.min(encoded.length / 2, 45));
        InputStream body = filter(request(contentEncoding, truncated), 200).getInputStream();

        assertThatThrownBy(body::readAllBytes).isInstanceOf(RequestDecodingFilter.MalformedContentException.class);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @ValueSource(strings = {"gzip", "zip"})
    void reportsDataOfAnotherFormatAsMalformed(String contentEncoding) throws Exception {
        InputStream body = filter(request(contentEncoding, CSV), 200).getInputStream();

        assertThatThrownBy(body::readAllBytes).isInstanceOf(RequestDecodingFilter.MalformedContentException.class);
    }

    @Test
    void passesFailuresOfReadingTheRequestOn() throws Exception {
        IOException aborted = new IOException("Connection reset");
        MockHttpServletRequest request = new MockHttpServletRequest("PUT", "/dataset/x") {
            @Override
            public jakarta.servlet.ServletInputStream getInputStream() {
                return new jakarta.servlet.ServletInputStream() {
                    @Override
                    public int read() throws IOException {
                        throw aborted;
                    }

                    @Override
                    public boolean isFinished() {
                        return false;
                    }

                    @Override
                    public boolean isReady() {
                        return true;
                    }

                    @Override
                    public void setReadListener(jakarta.servlet.ReadListener readListener) {
                    }
                };
            }
        };
        request.addHeader("Content-Encoding", "gzip");
        request.addHeader("Transfer-Encoding", "chunked");
        InputStream body = filter(request, 200).getInputStream();

        assertThatThrownBy(body::readAllBytes).isSameAs(aborted);
    }

    @Test
    void emptyBodyStaysEmpty() throws Exception {
        MockHttpServletRequest request = request("gzip", null);
        request.addHeader("Transfer-Encoding", "chunked");

        assertThat(filter(request, 200).getInputStream().readAllBytes()).isEmpty();
    }

    private static MockHttpServletRequest request(String contentEncoding, byte[] body) {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/datasets");
        request.setContentType("text/csv");
        if (contentEncoding != null) {
            request.addHeader("Content-Encoding", contentEncoding);
        }
        if (body != null) {
            request.setContent(body);
            request.addHeader("Content-Length", body.length);
        }
        return request;
    }

    /**
     * @return Returns the request passed on by the filter, or null if it answered the request itself.
     */
    private static HttpServletRequest filter(MockHttpServletRequest request, int expectedStatus) throws Exception {
        MockHttpServletResponse response = new MockHttpServletResponse();
        MockFilterChain chain = new MockFilterChain();
        new RequestDecodingFilter().doFilter(request, response, chain);

        assertThat(response.getStatus()).isEqualTo(expectedStatus);
        assertThat(chain.getRequest() != null).isEqualTo(expectedStatus == 200);
        return (HttpServletRequest) chain.getRequest();
    }

}
