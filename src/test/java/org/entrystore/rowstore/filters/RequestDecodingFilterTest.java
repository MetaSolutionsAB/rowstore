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

import jakarta.servlet.ReadListener;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.http.HttpServletRequest;
import org.entrystore.rowstore.ContentCodings;
import org.entrystore.rowstore.store.RowStoreConfig;
import org.entrystore.rowstore.util.DatasetUtil;
import org.json.JSONObject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link RequestDecodingFilter}; the uploads themselves are covered by CompressedUploadIT.
 * Specification: API-3.17
 */
@DisplayName("RequestDecodingFilter Tests")
class RequestDecodingFilterTest {

    static final byte[] CSV = "name,value\nA,1\nB,2\n".getBytes(StandardCharsets.UTF_8);

    static final long MAX_UPLOAD_SIZE = 10_000;

    @ParameterizedTest(name = "[{index}] {0}")
    @ValueSource(strings = {"gzip", "x-gzip", "GZip", "deflate", "deflate-no-wrap", "zip", "identity", "gzip, identity",
            "identity,gzip", "deflate, gzip", "gzip, identity, identity, identity, identity, identity, identity, gzip"})
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

    @Test
    void decodesZipEntryWithNameThatIsNotUtf8() throws Exception {
        // e.g., an archive created by Windows Explorer, which writes the name in the code page of the system
        ByteArrayOutputStream zip = new ByteArrayOutputStream();
        try (ZipOutputStream out = new ZipOutputStream(zip, StandardCharsets.ISO_8859_1)) {
            out.putNextEntry(new ZipEntry("Äpfel.csv"));
            out.write(CSV);
        }

        assertThat(filter(request("zip", zip.toByteArray()), 200).getInputStream().readAllBytes()).isEqualTo(CSV);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @ValueSource(strings = {"br", "zstd", "compress", "gzip, br",
            // more than two compressions or more than eight codings
            "gzip, gzip, gzip", "deflate, identity, gzip, deflate-no-wrap",
            "gzip, identity, identity, identity, identity, identity, identity, identity, identity",
            "identity, identity, identity, identity, identity, identity, identity, identity, identity"})
    void rejectsUnsupportedCodings(String contentEncoding) throws Exception {
        MockHttpServletResponse response = new MockHttpServletResponse();
        filter(request(contentEncoding, CSV), response, 415);

        assertThat(response.getHeader("Accept-Encoding")).isEqualTo("gzip, x-gzip, deflate, deflate-no-wrap, zip, identity");
    }

    @Test
    void countsCodingsOfAllHeaderLines() throws Exception {
        MockHttpServletRequest request = request("gzip, gzip", CSV);
        request.addHeader("Content-Encoding", "gzip");

        filter(request, 415);
    }

    @Test
    void limitsInputOfEachDecoder() throws Exception {
        // the inner gzip stream (about 20 kB) is larger than the limit, the request body is not
        byte[] nested = ContentCodings.encode(new byte[20_000_000], "gzip, gzip");
        assertThat(nested.length).isLessThan((int) MAX_UPLOAD_SIZE);
        InputStream nestedBody = filter(request("gzip, gzip", nested), 200).getInputStream();

        assertThatThrownBy(() -> nestedBody.transferTo(OutputStream.nullOutputStream()))
                .isInstanceOf(DatasetUtil.PayloadTooLargeException.class);

        // a request body that is larger than the limit, although its decoded size is not
        byte[] random = new byte[(int) MAX_UPLOAD_SIZE];
        new Random(1).nextBytes(random);
        InputStream body = filter(request("gzip", ContentCodings.encode(random, "gzip")), 200).getInputStream();

        assertThatThrownBy(() -> body.transferTo(OutputStream.nullOutputStream()))
                .isInstanceOf(DatasetUtil.PayloadTooLargeException.class);
    }

    @Test
    void closesDecodersWhenDecodingFailsWithError() {
        StackOverflowError error = new StackOverflowError();
        TrackingInputStream source = new TrackingInputStream(new byte[0]) {
            @Override
            public int read() {
                throw error;
            }
        };

        assertThatThrownBy(() -> RequestDecodingFilter.decode(source, List.of("gzip"), MAX_UPLOAD_SIZE)).isSameAs(error);
        assertThat(source.closed).isTrue();
    }

    @Test
    void passesRequestWithoutBodyUnchanged() throws Exception {
        MockHttpServletRequest request = request("br", new byte[0]);

        assertThat(filter(request, 200)).isSameAs(request);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @ValueSource(strings = {"GET", "HEAD", "DELETE", "OPTIONS"})
    void passesRequestsOfMethodsWithoutReadBodyUnchanged(String method) throws Exception {
        MockHttpServletRequest request = request("br", CSV);
        request.setMethod(method);

        assertThat(filter(request, 200)).isSameAs(request);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @ValueSource(strings = {"POST", "PUT"})
    void decodesBodyOfUnknownLength(String method) throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest(method, "/dataset/x") {
            @Override
            public int getContentLength() {
                return -1;
            }

            @Override
            public long getContentLengthLong() {
                return -1;
            }
        };
        request.setContent(ContentCodings.encode(CSV, "gzip"));
        request.addHeader("Content-Encoding", "gzip");

        assertThat(filter(request, 200).getInputStream().readAllBytes()).isEqualTo(CSV);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @ValueSource(strings = {"gzip", "deflate", "deflate-no-wrap", "zip", "gzip, deflate-no-wrap"})
    void closesDecodersWhenRequestIsHandled(String contentEncoding) throws Exception {
        TrackingInputStream source = new TrackingInputStream(ContentCodings.encode(CSV, contentEncoding));
        MockHttpServletRequest request = new MockHttpServletRequest("PUT", "/dataset/x") {
            @Override
            public ServletInputStream getInputStream() {
                return source;
            }
        };
        request.addHeader("Content-Encoding", contentEncoding);
        request.addHeader("Transfer-Encoding", "chunked");
        byte[][] read = new byte[1][];

        new RequestDecodingFilter(config()).doFilter(request, new MockHttpServletResponse(),
                (req, res) -> read[0] = req.getInputStream().readNBytes(5));

        assertThat(read[0]).isEqualTo(Arrays.copyOf(CSV, 5));
        assertThat(source.closed).isTrue();
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
    void emptyBodyOfUnknownLengthStaysEmpty() throws Exception {
        MockHttpServletRequest request = request("gzip", null);

        assertThat(filter(request, 200).getInputStream().readAllBytes()).isEmpty();
    }

    /**
     * A request body that records whether it has been closed.
     */
    private static class TrackingInputStream extends ServletInputStream {

        private final ByteArrayInputStream data;

        private boolean closed;

        TrackingInputStream(byte[] data) {
            this.data = new ByteArrayInputStream(data);
        }

        @Override
        public int read() {
            return data.read();
        }

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public boolean isFinished() {
            return data.available() == 0;
        }

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public void setReadListener(ReadListener readListener) {
        }

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
        return filter(request, new MockHttpServletResponse(), expectedStatus);
    }

    private static HttpServletRequest filter(MockHttpServletRequest request, MockHttpServletResponse response,
                                             int expectedStatus) throws Exception {
        MockFilterChain chain = new MockFilterChain();
        new RequestDecodingFilter(config()).doFilter(request, response, chain);

        assertThat(response.getStatus()).isEqualTo(expectedStatus);
        assertThat(chain.getRequest() != null).isEqualTo(expectedStatus == 200);
        return (HttpServletRequest) chain.getRequest();
    }

    private static RowStoreConfig config() {
        return new RowStoreConfig(new JSONObject()
                .put("baseurl", "http://localhost:8282/")
                .put("maxuploadsize", MAX_UPLOAD_SIZE)
                .put("database", new JSONObject()
                        .put("host", "localhost").put("database", "rowstore").put("user", "rowstore").put("password", "pw")));
    }

}
