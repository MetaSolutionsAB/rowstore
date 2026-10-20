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

import com.google.common.net.InetAddresses;
import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import org.apache.catalina.util.NetMask;
import org.entrystore.rowstore.store.RowStoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Replaces the remote address of requests from trusted reverse proxies ("trustedproxies" in rowstore.json)
 * with the client address from X-Forwarded-For, so that {@link HttpServletRequest#getRemoteAddr()} returns
 * the client address in all subsequent filters (e.g. {@link RateLimitFilter}).
 * <p>
 * Same algorithm as Tomcat's RemoteIpValve: the header is walked from the right, skipping trusted proxies, and the
 * first untrusted address is the client. Entries left of it may have been supplied by the client and are ignored.
 * RemoteIpValve itself is not used because its proxy lists are regular expressions (Tomcat 10.1), not CIDR ranges.
 * Other X-Forwarded-* headers are not evaluated.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 5)
public class TrustedProxyFilter implements Filter {

	private static final Logger log = LoggerFactory.getLogger(TrustedProxyFilter.class);

	private final List<NetMask> trustedProxies;

	public TrustedProxyFilter(RowStoreConfig config) {
		trustedProxies = config.getTrustedProxies().stream().map(NetMask::new).toList();
		if (trustedProxies.isEmpty()) {
			log.info("No trusted proxies configured, X-Forwarded-For is ignored");
		} else {
			log.info("Trusting X-Forwarded-For from proxies in {}", config.getTrustedProxies());
		}
	}

	@Override
	public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain chain)
			throws IOException, ServletException {
		HttpServletRequest httpReq = (HttpServletRequest) servletRequest;
		String clientAddr = resolveClientAddress(httpReq);
		if (clientAddr == null) {
			chain.doFilter(servletRequest, servletResponse);
			return;
		}
		chain.doFilter(new HttpServletRequestWrapper(httpReq) {
			@Override
			public String getRemoteAddr() {
				return clientAddr;
			}

			@Override
			public String getRemoteHost() {
				return clientAddr;
			}
		}, servletResponse);
	}

	/**
	 * @return the client address from X-Forwarded-For, or null if the request did not come from a trusted proxy
	 * or has no X-Forwarded-For header
	 */
	private String resolveClientAddress(HttpServletRequest request) {
		if (!isTrusted(request.getRemoteAddr())) {
			return null;
		}
		List<String> hops = new ArrayList<>();
		for (String header : Collections.list(request.getHeaders("X-Forwarded-For"))) {
			for (String hop : header.split(",")) {
				if (!hop.isBlank()) {
					hops.add(hop.trim());
				}
			}
		}
		String client = null;
		for (int i = hops.size() - 1; i >= 0; i--) {
			client = hops.get(i);
			if (!isTrusted(client)) {
				break;
			}
		}
		return client;
	}

	private boolean isTrusted(String address) {
		// Only IP literals are parsed; host names are never resolved
		if (trustedProxies.isEmpty() || address == null || !InetAddresses.isInetAddress(address)) {
			return false;
		}
		InetAddress inetAddress = InetAddresses.forString(address);
		for (NetMask proxy : trustedProxies) {
			if (proxy.matches(inetAddress)) {
				return true;
			}
		}
		return false;
	}

}
