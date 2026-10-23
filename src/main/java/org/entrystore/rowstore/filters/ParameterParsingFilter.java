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
import jakarta.servlet.http.HttpServletResponse;
import org.apache.catalina.Globals;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * Rejects requests with 400 whose query parameters Tomcat could not parse, e.g., because of a malformed percent-encoding
 * such as "?column=100%" (QUERY-9.04). Tomcat skips such parameters, so a column filter would be dropped silently and
 * the query would return rows that do not match it. Like Tomcat's FailedRequestFilter, but the parameters of a POST
 * request with a form body are not parsed, since that would consume the body (e.g., a JSON array of aliases sent as
 * application/x-www-form-urlencoded); such requests have no query parameters that RowStore uses as filters. Runs after
 * the CORS filter, so that browsers can read the response.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 3)
public class ParameterParsingFilter implements Filter {

	@Override
	public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain chain)
			throws IOException, ServletException {
		HttpServletRequest request = (HttpServletRequest) servletRequest;
		if (!isFormPost(request)) {
			// parses the query string (and nothing else, since only form bodies of POST requests are parameters)
			request.getParameterNames();
			if (request.getAttribute(Globals.PARAMETER_PARSE_FAILED_ATTR) != null) {
				((HttpServletResponse) servletResponse).sendError(HttpServletResponse.SC_BAD_REQUEST);
				return;
			}
		}
		chain.doFilter(servletRequest, servletResponse);
	}

	/**
	 * @return Returns whether Tomcat would parse the body of the request as parameters (url-encoded or, with the
	 * multipart configuration of the DispatcherServlet, multipart form data).
	 */
	private static boolean isFormPost(HttpServletRequest request) {
		String contentType = request.getContentType();
		if (!"POST".equalsIgnoreCase(request.getMethod()) || contentType == null) {
			return false;
		}
		String type = contentType.toLowerCase();
		return type.startsWith(MediaType.APPLICATION_FORM_URLENCODED_VALUE) || type.startsWith(MediaType.MULTIPART_FORM_DATA_VALUE);
	}

}
