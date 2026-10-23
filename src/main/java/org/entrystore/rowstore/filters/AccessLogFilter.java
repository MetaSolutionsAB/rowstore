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

import jakarta.servlet.AsyncEvent;
import jakarta.servlet.AsyncListener;
import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.ServletResponseWrapper;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.catalina.connector.ResponseFacade;
import org.entrystore.rowstore.store.RowStoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * Logs one line per request at INFO level, in the format of the access log of 1.x (DEPL-6.02), after the response
 * has been completed, also for streamed (asynchronous) responses:
 * <pre>
 * {client address} "{method} {path} {query}" {status} {response bytes} {request bytes} {scheme://host} {user agent} {referrer}
 * </pre>
 * The client address is resolved like by {@link TrustedProxyFilter}. Runs before all other filters, so that requests
 * rejected by them (e.g., 400 for path parameters, 403 for CORS, 429 for rate limits) are logged too. Disabled with
 * {@code "accesslog": false} in rowstore.json (CFG-3.12).
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class AccessLogFilter implements Filter {

	/**
	 * Logger of the access log; the logger name ends with "LogService", like the one of 1.x, so that log lines look
	 * the same.
	 */
	private static final Logger accessLog = LoggerFactory.getLogger("org.entrystore.rowstore.LogService");

	private final boolean enabled;

	private final TrustedProxyFilter trustedProxyFilter;

	public AccessLogFilter(RowStoreConfig config, TrustedProxyFilter trustedProxyFilter) {
		this.enabled = config.isAccessLogEnabled();
		this.trustedProxyFilter = trustedProxyFilter;
	}

	@Override
	public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain chain)
			throws IOException, ServletException {
		if (!enabled || !accessLog.isInfoEnabled()) {
			chain.doFilter(servletRequest, servletResponse);
			return;
		}
		HttpServletRequest request = (HttpServletRequest) servletRequest;
		HttpServletResponse response = (HttpServletResponse) servletResponse;
		try {
			chain.doFilter(request, response);
		} catch (IOException | ServletException | RuntimeException | Error e) {
			// the container sends 500 unless the response has already been committed
			log(request, response, response.isCommitted() ? response.getStatus() : HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
			throw e;
		}
		if (request.isAsyncStarted()) {
			// e.g., a streamed export: logged when the response is complete
			request.getAsyncContext().addListener(new AsyncListener() {
				@Override
				public void onComplete(AsyncEvent event) {
					log(request, response, response.getStatus());
				}

				@Override
				public void onTimeout(AsyncEvent event) {
				}

				@Override
				public void onError(AsyncEvent event) {
				}

				@Override
				public void onStartAsync(AsyncEvent event) {
				}
			});
		} else {
			log(request, response, response.getStatus());
		}
	}

	private void log(HttpServletRequest request, HttpServletResponse response, int status) {
		accessLog.info(format(trustedProxyFilter.getClientAddress(request), request, status, bytesWritten(response)));
	}

	/**
	 * @param responseBytes The number of bytes of the response body, or -1 if unknown.
	 * @return Returns the access log line of a request.
	 */
	static String format(String clientAddress, HttpServletRequest request, int status, long responseBytes) {
		String query = request.getQueryString();
		return clientAddress + " \"" + request.getMethod() + " " + request.getRequestURI() + " " +
				(query != null ? query : "") + "\" " + status + " " +
				(responseBytes >= 0 ? responseBytes : "") + " " + requestBytes(request) + " " + hostRef(request) + " " +
				header(request, "User-Agent") + " " + header(request, "Referer");
	}

	/**
	 * @return Returns the size of the request body: the Content-Length, 0 without a body, or "-" for a chunked body
	 * of unknown size.
	 */
	private static String requestBytes(HttpServletRequest request) {
		long length = request.getContentLengthLong();
		if (length >= 0) {
			return String.valueOf(length);
		}
		return request.getHeader("Transfer-Encoding") != null ? "-" : "0";
	}

	private static String hostRef(HttpServletRequest request) {
		String host = request.getHeader("Host");
		if (host == null || host.isEmpty()) {
			host = request.getServerName() + ":" + request.getServerPort();
		}
		return request.getScheme() + "://" + host;
	}

	private static String header(HttpServletRequest request, String name) {
		String value = request.getHeader(name);
		return value != null ? value : "";
	}

	/**
	 * @return Returns the number of bytes of the response body that were written, or -1 if unknown.
	 */
	private static long bytesWritten(ServletResponse response) {
		ServletResponse r = response;
		while (r instanceof ServletResponseWrapper wrapper) {
			r = wrapper.getResponse();
		}
		return r instanceof ResponseFacade facade ? facade.getContentWritten() : -1;
	}

}
