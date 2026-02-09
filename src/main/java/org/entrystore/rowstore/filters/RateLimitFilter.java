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
import com.google.common.util.concurrent.RateLimiter;
import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.entrystore.rowstore.store.RowStoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 10)
public class RateLimitFilter implements Filter {

	private static final Logger log = LoggerFactory.getLogger(RateLimitFilter.class);

	private Cache<String, RateLimiter> rateLimiters;

	private Cache<String, Cache<Date, Object>> slidingWindows;

	private boolean rateLimitFilterEnabled = false;

	private final RowStoreConfig config;

	private boolean rateLimitTypeSlidingWindow = true;

	private final Object dummy = new Object();

	private Callable<Cache<Date, Object>> loader;

	public RateLimitFilter(RowStoreConfig config) {
		if (config == null) {
			throw new IllegalArgumentException("Configuration must not be null");
		}

		this.config = config;
		if (config.getRateLimitTimeRange() > 0 &&
				(config.getRateLimitRequestsGlobal() > 0 || config.getRateLimitRequestsDataset() > 0)) {
			rateLimitFilterEnabled = true;
			rateLimitTypeSlidingWindow = !"average".equalsIgnoreCase(config.getRateLimitType());
			if (rateLimitTypeSlidingWindow) {
				log.info("Rate limiting using sliding windows");
				slidingWindows = CacheBuilder.newBuilder().maximumSize(32768).build();
				loader = () -> CacheBuilder.newBuilder().expireAfterWrite(config.getRateLimitTimeRange(), TimeUnit.SECONDS).build();
			} else {
				log.info("Rate limiting using averaging");
				rateLimiters = CacheBuilder.newBuilder().maximumSize(32768).build();
			}
		} else {
			log.info("Rate limiting is disabled");
			if (config.getRateLimitTimeRange() != -1 && config.getRateLimitTimeRange() <= 0) {
				log.warn("Rate limit time range configured but not positive: {}", config.getRateLimitTimeRange());
			}
			if (config.getRateLimitRequestsGlobal() != -1 && config.getRateLimitRequestsGlobal() <= 0) {
				log.warn("Rate limit global requests configured but not positive: {}", config.getRateLimitRequestsGlobal());
			}
			if (config.getRateLimitRequestsDataset() != -1 && config.getRateLimitRequestsDataset() <= 0) {
				log.warn("Rate limit dataset requests configured but not positive: {}", config.getRateLimitRequestsDataset());
			}
		}
	}

	@Override
	public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain chain)
			throws IOException, ServletException {
		HttpServletRequest httpReq = (HttpServletRequest) servletRequest;
		HttpServletResponse httpResp = (HttpServletResponse) servletResponse;

		String path = httpReq.getRequestURI();
		if (rateLimitFilterEnabled &&
				path != null &&
				isRateLimitedPath(path) &&
				isRateLimitedMethod(httpReq.getMethod())) {
			long checkResult = countAndCheckIfRequestPermitted(httpReq);
			if (checkResult != 0) {
				if (checkResult > 0) {
					httpResp.setDateHeader("Retry-After", checkResult);
				}
				httpResp.setStatus(429);
				return;
			}
		}
		chain.doFilter(servletRequest, servletResponse);
	}

	private long countAndCheckIfRequestPermitted(HttpServletRequest request) {
		if (request == null) {
			throw new IllegalArgumentException("Request parameter must not be null");
		}

		String dataset = request.getRequestURI();

		String clientIP = request.getHeader("X-Forwarded-For");
		if (clientIP == null || clientIP.isEmpty()) {
			clientIP = request.getRemoteAddr();
		}

		try {
			if (rateLimitTypeSlidingWindow) {
				long result;
				result = checkSlidingWindow("global", config.getRateLimitRequestsGlobal());
				if (result != 0) return result;
				result = checkSlidingWindow(dataset, config.getRateLimitRequestsDataset());
				if (result != 0) return result;
				result = checkSlidingWindow(clientIP, config.getRateLimitRequestsClientIP());
				if (result != 0) return result;
			} else {
				if (checkAverageLimiter("global", config.getRateLimitRequestsGlobal())) return -1;
				if (checkAverageLimiter(dataset, config.getRateLimitRequestsDataset())) return -1;
				if (checkAverageLimiter(clientIP, config.getRateLimitRequestsClientIP())) return -1;
			}
			return 0;
		} catch (ExecutionException e) {
			log.error("Rate limit check failed, permitting request", e);
		}
		return 0;
	}

	private long checkSlidingWindow(String key, int maxRequests) throws ExecutionException {
		if (maxRequests <= 0) {
			return 0;
		}
		Cache<Date, Object> window = slidingWindows.get(key, loader);
		window.cleanUp();
		if (window.size() >= maxRequests) {
			log.debug("Request rate limit reached for {}", key);
			return calculateRetryAfter(window);
		}
		window.put(new Date(), dummy);
		return 0;
	}

	private boolean checkAverageLimiter(String key, int maxRequests) throws ExecutionException {
		if (maxRequests <= 0) {
			return false;
		}
		boolean limited = !rateLimiters.get(key, () -> {
			double permits = (double) maxRequests / (double) config.getRateLimitTimeRange();
			return RateLimiter.create(permits);
		}).tryAcquire();
		if (limited) {
			log.debug("Request rate limit reached for {}", key);
		}
		return limited;
	}

	private boolean isRateLimitedMethod(String method) {
		return "GET".equalsIgnoreCase(method) || "HEAD".equalsIgnoreCase(method);
	}

	private boolean isRateLimitedPath(String path) {
		if (path == null) {
			throw new IllegalArgumentException("Path must not be null");
		}
		return !path.endsWith("/status");
	}

	private long calculateRetryAfter(Cache<Date, Object> cache) {
		try {
			return Collections.min(cache.asMap().keySet()).getTime() + (config.getRateLimitTimeRange() * 1000L) + 1L;
		} catch (NoSuchElementException e) {
			log.debug("Sliding window emptied during retry-after calculation", e);
		}
		return new Date().getTime();
	}

}
