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
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * Rejects requests with path parameters (";" in the path) with 400. RowStore does not use them, but Tomcat and Spring
 * ignore them when routing, so e.g. "/v3;x/api-docs" would bypass path rules of a reverse proxy and
 * "/dataset/{id};1" would get its own per-dataset rate limit bucket. Encoded semicolons ("%3B") are not delimiters
 * and are not affected. Runs first after {@link AccessLogFilter}, so that it also covers Actuator and springdoc
 * endpoints.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 1)
public class PathParameterFilter implements Filter {

	@Override
	public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain chain)
			throws IOException, ServletException {
		// The raw request URI does not contain the query string
		String uri = ((HttpServletRequest) servletRequest).getRequestURI();
		if (uri != null && uri.indexOf(';') >= 0) {
			((HttpServletResponse) servletResponse).sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		chain.doFilter(servletRequest, servletResponse);
	}

}
