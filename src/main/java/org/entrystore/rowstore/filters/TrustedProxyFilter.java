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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
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
import java.util.concurrent.TimeUnit;

/**
 * Replaces the remote address of requests from trusted reverse proxies with the client address from X-Forwarded-For,
 * so that {@link HttpServletRequest#getRemoteAddr()} returns the client address in all subsequent filters (e.g.
 * {@link RateLimitFilter}).
 * <p>
 * Without "trustedproxies" in rowstore.json, the connecting peer is trusted as the reverse proxy, since RowStore must
 * not be reachable other than through it (SEC-9.01), and the last X-Forwarded-For entry, which the proxy has
 * appended or set, is the client. Entries left of it may have been supplied by the client and are ignored.
 * <p>
 * With "trustedproxies", only the listed peers are trusted, with the algorithm of Tomcat's RemoteIpValve: the header
 * is walked from the right, skipping trusted proxies, and the first untrusted address is the client. RemoteIpValve
 * itself is not used because its proxy lists are regular expressions (Tomcat 10.1), not CIDR ranges.
 * <p>
 * Other X-Forwarded-* headers are not evaluated.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 5)
public class TrustedProxyFilter implements Filter {

	private static final Logger log = LoggerFactory.getLogger(TrustedProxyFilter.class);

	// null if "trustedproxies" is not configured, then the connecting peer is trusted
	private final List<NetMask> trustedProxies;

	// Peers that sent X-Forwarded-For without being trusted, to warn at most once per hour and address
	private final Cache<String, Boolean> warnedPeers =
			CacheBuilder.newBuilder().maximumSize(1000).expireAfterWrite(1, TimeUnit.HOURS).build();

	public TrustedProxyFilter(RowStoreConfig config) {
		List<String> configured = config.getTrustedProxies();
		trustedProxies = configured == null ? null : configured.stream().map(NetMask::new).toList();
		if (trustedProxies == null) {
			log.info("No trusted proxies configured, the client address is the last X-Forwarded-For entry of the " +
					"connecting peer, which must be the reverse proxy");
		} else if (trustedProxies.isEmpty()) {
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
		if (trustedProxies == null) {
			List<String> hops = forwardedHops(request);
			return hops.isEmpty() ? null : hops.getLast();
		}
		String peer = request.getRemoteAddr();
		// The socket peer of a link-local IPv6 connection carries the zone ID of the local interface (e.g.
		// "fe80::1%2"), which is not client-controlled and irrelevant for matching
		int zone = peer != null ? peer.indexOf('%') : -1;
		if (!isTrusted(zone >= 0 ? peer.substring(0, zone) : peer)) {
			// Helps operators whose proxy is not in "trustedproxies" (e.g. on a public address); 1.x trusted any value
			if (!trustedProxies.isEmpty() && request.getHeader("X-Forwarded-For") != null &&
					warnedPeers.asMap().putIfAbsent(peer, Boolean.TRUE) == null) {
				log.warn("Ignoring X-Forwarded-For from {}, which is not a trusted proxy; if it is a reverse proxy, " +
						"add it to \"trustedproxies\" in rowstore.json (logged at most once per hour per address)", peer);
			}
			return null;
		}
		List<String> hops = forwardedHops(request);
		String client = null;
		for (int i = hops.size() - 1; i >= 0; i--) {
			client = hops.get(i);
			if (!isTrusted(client)) {
				break;
			}
		}
		return client;
	}

	/**
	 * @return the entries of all X-Forwarded-For header lines in order, without blank entries
	 */
	private static List<String> forwardedHops(HttpServletRequest request) {
		List<String> hops = new ArrayList<>();
		for (String header : Collections.list(request.getHeaders("X-Forwarded-For"))) {
			for (String hop : header.split(",")) {
				if (!hop.isBlank()) {
					hops.add(hop.trim());
				}
			}
		}
		return hops;
	}

	private boolean isTrusted(String address) {
		// Only IP literals are parsed; host names are never resolved. IPv6 zone IDs ("%...") are rejected up front,
		// InetAddresses.forString() would look up the network interface and throw for unknown ones.
		if (trustedProxies.isEmpty() || address == null || address.indexOf('%') >= 0 ||
				!InetAddresses.isInetAddress(address)) {
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
