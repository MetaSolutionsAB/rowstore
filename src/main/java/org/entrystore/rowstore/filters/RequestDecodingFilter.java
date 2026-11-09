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

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ReadListener;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PushbackInputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;
import java.util.zip.ZipInputStream;

/**
 * Decodes request bodies according to their Content-Encoding header, like the DecoderService of Restlet did in 1.x
 * (API-3.17): {@code gzip} and {@code x-gzip}, {@code deflate} (zlib), {@code deflate-no-wrap} (raw deflate),
 * {@code zip} (the first entry of the archive) and {@code identity}. The codings are case-insensitive and may be
 * stacked (e.g. {@code gzip, identity}); they are listed in the order in which they were applied and are decoded in
 * reverse order. A request body with any other coding (e.g. {@code br}) is rejected with 415 before it is read, since
 * loading compressed bytes as CSV would produce an empty or broken dataset (1.x ignored such codings).
 * <p>
 * The body is decoded while the controllers read it, so that {@code maxuploadsize} limits the decoded bytes, which
 * protects against decompression bombs. Corrupt compressed data is reported as {@link MalformedContentException}
 * while reading, which the controllers answer with 400. The decoded request hides the Content-Encoding and
 * Content-Length headers, which describe the encoded body. Requests without a body are passed on unchanged.
 * <p>
 * Runs after the CORS filter, so that 415 responses carry CORS headers, and after the rate limit, so that rejected
 * requests are not inspected. The body is only read by the controllers, so the order relative to the other filters
 * does not matter otherwise; {@link ParameterParsingFilter} does not read form bodies.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 11)
public class RequestDecodingFilter implements Filter {

	private static final Logger log = LoggerFactory.getLogger(RequestDecodingFilter.class);

	static final String IDENTITY = "identity";

	static final Set<String> SUPPORTED_CODINGS = Set.of("gzip", "x-gzip", "deflate", "deflate-no-wrap", "zip", IDENTITY);

	@Override
	public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain chain)
			throws IOException, ServletException {
		HttpServletRequest request = (HttpServletRequest) servletRequest;
		List<String> codings = codings(request);
		if (codings.isEmpty() || !hasBody(request)) {
			chain.doFilter(servletRequest, servletResponse);
			return;
		}
		for (String coding : codings) {
			if (!SUPPORTED_CODINGS.contains(coding)) {
				log.info("Rejecting request body with unsupported Content-Encoding \"{}\"", coding);
				((HttpServletResponse) servletResponse).sendError(HttpServletResponse.SC_UNSUPPORTED_MEDIA_TYPE);
				return;
			}
		}
		chain.doFilter(new DecodedRequest(request, codings), servletResponse);
	}

	/**
	 * @return Returns the codings of all Content-Encoding header lines in the order in which they were applied,
	 * in lower case, without empty entries.
	 */
	static List<String> codings(HttpServletRequest request) {
		List<String> codings = new ArrayList<>();
		Enumeration<String> headers = request.getHeaders(HttpHeaders.CONTENT_ENCODING);
		if (headers == null) {
			return codings;
		}
		for (String header : Collections.list(headers)) {
			for (String coding : header.split(",")) {
				if (!coding.isBlank()) {
					codings.add(coding.trim().toLowerCase(Locale.ROOT));
				}
			}
		}
		return codings;
	}

	/**
	 * @return Returns whether the request has a body: a Content-Length above 0 or a chunked body.
	 */
	private static boolean hasBody(HttpServletRequest request) {
		return request.getContentLengthLong() > 0 || request.getHeader(HttpHeaders.TRANSFER_ENCODING) != null;
	}

	/**
	 * Wraps a stream into the decoders of the given codings, applied in reverse order. An empty stream stays empty.
	 *
	 * @throws IOException If the start of the encoded data could not be read or is not valid for its coding.
	 */
	static InputStream decode(InputStream encoded, List<String> codings) throws IOException {
		InputStream stream = encoded;
		for (String coding : codings.reversed()) {
			PushbackInputStream pushback = new PushbackInputStream(stream, 1);
			int first = pushback.read();
			if (first == -1) {
				// no body: nothing to decode, as for an empty body without Content-Encoding
				return InputStream.nullInputStream();
			}
			pushback.unread(first);
			stream = switch (coding) {
				case "gzip", "x-gzip" -> new GZIPInputStream(pushback);
				case "deflate" -> new InflaterInputStream(pushback);
				case "deflate-no-wrap" -> new InflaterInputStream(pushback, new Inflater(true));
				case "zip" -> {
					ZipInputStream zip = new ZipInputStream(pushback);
					if (zip.getNextEntry() == null) {
						throw new IOException("The zip archive has no entry");
					}
					yield zip;
				}
				case IDENTITY -> pushback;
				default -> throw new IllegalArgumentException("Unsupported coding: " + coding);
			};
		}
		return stream;
	}

	/**
	 * Signals that a request body could not be decoded according to its Content-Encoding, e.g., because the
	 * compressed data is corrupt or truncated. The request is to be answered with 400.
	 */
	public static class MalformedContentException extends IOException {
		MalformedContentException(Throwable cause) {
			super("Unable to decode the request body according to its Content-Encoding: " + cause.getMessage(), cause);
		}
	}

	/**
	 * A request whose body is decoded and whose headers do not describe the encoded body anymore.
	 */
	private static final class DecodedRequest extends HttpServletRequestWrapper {

		private static final Set<String> HIDDEN_HEADERS = Set.of(
				HttpHeaders.CONTENT_ENCODING.toLowerCase(Locale.ROOT), HttpHeaders.CONTENT_LENGTH.toLowerCase(Locale.ROOT));

		private final List<String> codings;

		private DecodingInputStream inputStream;

		private BufferedReader reader;

		DecodedRequest(HttpServletRequest request, List<String> codings) {
			super(request);
			this.codings = codings;
		}

		@Override
		public ServletInputStream getInputStream() throws IOException {
			if (reader != null) {
				throw new IllegalStateException("getReader() has already been called for this request");
			}
			if (inputStream == null) {
				inputStream = new DecodingInputStream(super.getInputStream(), codings);
			}
			return inputStream;
		}

		@Override
		public BufferedReader getReader() throws IOException {
			if (reader == null) {
				if (inputStream != null) {
					throw new IllegalStateException("getInputStream() has already been called for this request");
				}
				String encoding = getCharacterEncoding();
				Charset charset = encoding != null ? Charset.forName(encoding) : StandardCharsets.ISO_8859_1;
				reader = new BufferedReader(new InputStreamReader(new DecodingInputStream(super.getInputStream(), codings), charset));
			}
			return reader;
		}

		@Override
		public int getContentLength() {
			return -1;
		}

		@Override
		public long getContentLengthLong() {
			return -1;
		}

		@Override
		public String getHeader(String name) {
			return isHidden(name) ? null : super.getHeader(name);
		}

		@Override
		public Enumeration<String> getHeaders(String name) {
			return isHidden(name) ? Collections.emptyEnumeration() : super.getHeaders(name);
		}

		@Override
		public Enumeration<String> getHeaderNames() {
			List<String> names = Collections.list(super.getHeaderNames());
			names.removeIf(DecodedRequest::isHidden);
			return Collections.enumeration(names);
		}

		@Override
		public int getIntHeader(String name) {
			return isHidden(name) ? -1 : super.getIntHeader(name);
		}

		private static boolean isHidden(String name) {
			return name != null && HIDDEN_HEADERS.contains(name.toLowerCase(Locale.ROOT));
		}

	}

	/**
	 * Decodes the body lazily on the first read, so that all decoding failures surface while reading. Failures of
	 * the decoders are reported as {@link MalformedContentException}, failures of reading the request (e.g., a client
	 * that aborted the upload) as they are.
	 */
	private static final class DecodingInputStream extends ServletInputStream {

		private final SourceInputStream source;

		private final List<String> codings;

		private InputStream decoded;

		private boolean finished;

		DecodingInputStream(InputStream source, List<String> codings) {
			this.source = new SourceInputStream(source);
			this.codings = codings;
		}

		@Override
		public int read() throws IOException {
			byte[] b = new byte[1];
			int n = read(b, 0, 1);
			return n == -1 ? -1 : b[0] & 0xff;
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			if (finished) {
				return -1;
			}
			try {
				if (decoded == null) {
					decoded = decode(source, codings);
				}
				int n = decoded.read(b, off, len);
				if (n == -1) {
					finished = true;
				}
				return n;
			} catch (IOException e) {
				if (source.failed) {
					throw e;
				}
				throw new MalformedContentException(e);
			}
		}

		@Override
		public void close() throws IOException {
			source.close();
		}

		@Override
		public boolean isFinished() {
			return finished;
		}

		@Override
		public boolean isReady() {
			return true;
		}

		@Override
		public void setReadListener(ReadListener readListener) {
			throw new UnsupportedOperationException("Asynchronous reading of a decoded request body is not supported");
		}

	}

	/**
	 * The encoded request body, which records whether reading it failed, to tell such failures apart from failures
	 * of the decoders.
	 */
	private static final class SourceInputStream extends FilterInputStream {

		private boolean failed;

		SourceInputStream(InputStream in) {
			super(in);
		}

		@Override
		public int read() throws IOException {
			try {
				return super.read();
			} catch (IOException e) {
				failed = true;
				throw e;
			}
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			try {
				return super.read(b, off, len);
			} catch (IOException e) {
				failed = true;
				throw e;
			}
		}

	}

}
