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
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpHeaders;
import org.springframework.http.InvalidMediaTypeException;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Tunnels the HTTP method and the preferred media type through the request, like the TunnelService of Restlet did in
 * 1.x, for clients that cannot set them (e.g., download links or HTML forms):
 * <ul>
 * <li>The query parameter {@code media} replaces the Accept header (API-4.05), e.g., {@code /export?media=csv}. Its
 * value is a file extension (json, csv, html, htm, txt, xml) or a media type; other values are ignored.</li>
 * <li>The query parameter {@code method} and the header {@code X-HTTP-Method-Override} replace the method of a POST
 * request (API-4.06), e.g., {@code POST /dataset/{id}?method=PUT} replaces the data. The header takes precedence. Unlike
 * in 1.x, the method of other requests is never replaced, since the reverse proxy authenticates all requests other than
 * GET and HEAD (SEC-9.02): a GET request must not become a DELETE. A value that is not a method of the API is rejected
 * with 400 instead of appending the data of the POST.</li>
 * </ul>
 * The query parameters {@code media}, {@code charset}, {@code encoding} and {@code language} were consumed by the
 * TunnelService and are therefore not column filters, unless the dataset has a column of that name (QUERY-3.06).
 * Runs before the rate limit, which applies to the tunnelled method.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 7)
public class TunnelFilter implements Filter {

	static final String METHOD_PARAMETER = "method";

	static final String METHOD_HEADER = "X-HTTP-Method-Override";

	static final String MEDIA_PARAMETER = "media";

	/**
	 * The query parameters that the TunnelService of 1.x consumed for content negotiation.
	 */
	public static final Set<String> PREFERENCE_PARAMETERS = Set.of(MEDIA_PARAMETER, "charset", "encoding", "language");

	/**
	 * Methods that a POST request can be tunnelled to (POST itself means no change).
	 */
	private static final Set<String> TUNNEL_METHODS = Set.of("GET", "HEAD", "POST", "PUT", "DELETE", "OPTIONS");

	/**
	 * File extensions accepted as value of the media parameter, as in the MetadataService of Restlet.
	 */
	private static final Map<String, String> MEDIA_EXTENSIONS = Map.of(
			"json", MediaType.APPLICATION_JSON_VALUE,
			"csv", "text/csv",
			"html", MediaType.TEXT_HTML_VALUE,
			"htm", MediaType.TEXT_HTML_VALUE,
			"txt", MediaType.TEXT_PLAIN_VALUE,
			"xml", MediaType.TEXT_XML_VALUE);

	@Override
	public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain chain)
			throws IOException, ServletException {
		HttpServletRequest request = (HttpServletRequest) servletRequest;

		String method = null;
		if ("POST".equals(request.getMethod())) {
			// the query string is parsed here, since the parameters of a form POST would include its body
			String tunnelled = request.getHeader(METHOD_HEADER);
			if (tunnelled == null || tunnelled.isBlank()) {
				tunnelled = firstQueryValue(request.getQueryString(), METHOD_PARAMETER);
			}
			if (tunnelled != null && !tunnelled.isBlank()) {
				method = tunnelled.trim().toUpperCase(Locale.ROOT);
				if (!TUNNEL_METHODS.contains(method)) {
					((HttpServletResponse) servletResponse).sendError(HttpServletResponse.SC_BAD_REQUEST);
					return;
				}
			}
		}

		String accept = mediaType(firstQueryValue(request.getQueryString(), MEDIA_PARAMETER));

		if (method != null || accept != null) {
			servletRequest = new TunnelledRequest(request, method, accept);
		}
		chain.doFilter(servletRequest, servletResponse);
	}

	/**
	 * @return Returns the media type for a value of the media parameter, or null if it is neither a known file
	 * extension nor a media type.
	 */
	static String mediaType(String value) {
		if (value == null || value.isBlank()) {
			return null;
		}
		String v = value.trim().toLowerCase(Locale.ROOT);
		String extension = MEDIA_EXTENSIONS.get(v);
		if (extension != null) {
			return extension;
		}
		if (v.indexOf('/') > 0) {
			try {
				MediaType.parseMediaType(v);
				return v;
			} catch (InvalidMediaTypeException e) {
				return null;
			}
		}
		return null;
	}

	/**
	 * @return Returns the decoded first value of a parameter in a raw query string, or null if there is none or it
	 * cannot be decoded.
	 */
	static String firstQueryValue(String query, String name) {
		if (query == null) {
			return null;
		}
		for (String pair : query.split("&")) {
			int eq = pair.indexOf('=');
			String key = eq >= 0 ? pair.substring(0, eq) : pair;
			try {
				if (name.equals(URLDecoder.decode(key, StandardCharsets.UTF_8))) {
					return eq >= 0 ? URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8) : "";
				}
			} catch (IllegalArgumentException e) {
				// malformed percent-encoding, rejected by ParameterParsingFilter unless the request is a form POST
			}
		}
		return null;
	}

	private static final class TunnelledRequest extends HttpServletRequestWrapper {

		private final String method;

		private final String accept;

		TunnelledRequest(HttpServletRequest request, String method, String accept) {
			super(request);
			this.method = method;
			this.accept = accept;
		}

		@Override
		public String getMethod() {
			return method != null ? method : super.getMethod();
		}

		@Override
		public String getHeader(String name) {
			if (accept != null && HttpHeaders.ACCEPT.equalsIgnoreCase(name)) {
				return accept;
			}
			return super.getHeader(name);
		}

		@Override
		public Enumeration<String> getHeaders(String name) {
			if (accept != null && HttpHeaders.ACCEPT.equalsIgnoreCase(name)) {
				return Collections.enumeration(Set.of(accept));
			}
			return super.getHeaders(name);
		}

		@Override
		public Enumeration<String> getHeaderNames() {
			Set<String> names = new LinkedHashSet<>(Collections.list(super.getHeaderNames()));
			if (accept != null && names.stream().noneMatch(HttpHeaders.ACCEPT::equalsIgnoreCase)) {
				names.add(HttpHeaders.ACCEPT);
			}
			return Collections.enumeration(names);
		}

	}

}
