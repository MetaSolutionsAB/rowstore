/*
 * Copyright (c) 2011-2016 MetaSolutions AB <info@metasolutions.se>
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
import org.entrystore.rowstore.store.RowStoreConfig;
import org.restlet.Request;
import org.restlet.Response;
import org.restlet.data.Method;
import org.restlet.data.Status;
import org.restlet.routing.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Provides request rate limitation.
 * 
 * @author Hannes Ebner
 */
public class RateLimitFilter extends Filter {

	static private Logger log = LoggerFactory.getLogger(RateLimitFilter.class);

	private Cache<String, RateLimiter> rateLimiters;

	private Cache<String, Cache<Date, Object>> slidingWindows;

	private boolean rateLimitFilterEnabled = false;

	private RowStoreConfig config;

	private boolean rateLimitTypeSlidingWindow = true;

	private Object dummy = new Object();

	public RateLimitFilter(RowStoreConfig config) {
		if (config == null) {
			throw new IllegalArgumentException("Configuration must not be null");
		}

		this.config = config;
		if (config.getRateLimitTimeRange() != -1 &&
				(config.getRateLimitRequestsGlobal() != -1 || config.getRateLimitRequestsDataset() != -1)) {
			rateLimitFilterEnabled = true;
			rateLimitTypeSlidingWindow = !"average".equalsIgnoreCase(config.getRateLimitType());
			if (rateLimitTypeSlidingWindow) {
				log.info("Rate limiting using sliding windows");
				slidingWindows = CacheBuilder.newBuilder().maximumSize(32768).build();
			} else {
				log.info("Rate limiting using averaging");
				rateLimiters = CacheBuilder.newBuilder().maximumSize(32768).build();
			}
		}
	}

	@Override
	protected int beforeHandle(Request request, Response response) {
		String path = request.getResourceRef().getPath();
		if (rateLimitFilterEnabled &&
				path != null &&
				isRateLimitedPath(path) &&
				isRateLimitedMethod(request.getMethod())) {
			if (!countAndCheckIfRequestPermitted(request)) {
				response.setStatus(Status.CLIENT_ERROR_TOO_MANY_REQUESTS);
				return STOP;
			}
		}
		return CONTINUE;
	}

	private boolean countAndCheckIfRequestPermitted(Request request) {
		if (request == null) {
			throw new IllegalArgumentException("Request parameter must not be null");
		}

		String dataset = request.getResourceRef().getPath();
		String clientIP = request.getClientInfo().getUpstreamAddress();

		try {
			if (rateLimitTypeSlidingWindow) {
				Callable<Cache<Date, Object>> loader = () -> CacheBuilder.newBuilder().expireAfterWrite(config.getRateLimitTimeRange(), TimeUnit.SECONDS).build();

				// Checking for global rate limit
				Cache<Date, Object> globalWindow = slidingWindows.get("global", loader);
				globalWindow.cleanUp();
				if (globalWindow.size() >= config.getRateLimitRequestsGlobal()) {
					log.debug("Request rate limit reached globally");
					return false;
				}

				// Checking for per-dataset rate limit
				Cache<Date, Object> datasetWindow = slidingWindows.get(dataset, loader);
				datasetWindow.cleanUp();
				if (datasetWindow.size() >= config.getRateLimitRequestsDataset()) {
					log.debug("Request rate limit reached for " + dataset);
					return false;
				}

				// Checking for per-client IP rate limit
				Cache<Date, Object> clientIPWindow = slidingWindows.get(clientIP, loader);
				clientIPWindow.cleanUp();
				if (clientIPWindow.size() >= config.getRateLimitRequestsClientIP()) {
					log.debug("Request rate limit reached for client IP " + clientIP);
					return false;
				}

				Date now = new Date();
				globalWindow.put(now, dummy);
				datasetWindow.put(now, dummy);
				clientIPWindow.put(now, dummy);
			} else {
				// Checking for global rate limit
				if (config.getRateLimitRequestsGlobal() > 0 &&
						!rateLimiters.get("global", () -> {
							double permits = (double) config.getRateLimitRequestsGlobal() / (double) config.getRateLimitTimeRange();
							return RateLimiter.create(permits);
						}).tryAcquire()) {
					log.debug("Request rate limit reached globally");
					return false;
				}

				// Checking for per-dataset rate limit
				if (config.getRateLimitRequestsDataset() > 0 &&
						!rateLimiters.get(dataset, () -> {
							double permits = (double) config.getRateLimitRequestsDataset() / (double) config.getRateLimitTimeRange();
							return RateLimiter.create(permits);
						}).tryAcquire()) {
					log.debug("Request rate limit reached for " + dataset);
					return false;
				}

				// Checking for per-client IP rate limit
				if (config.getRateLimitRequestsClientIP() > 0 &&
						!rateLimiters.get(clientIP, () -> {
							double permits = (double) config.getRateLimitRequestsClientIP() / (double) config.getRateLimitTimeRange();
							return RateLimiter.create(permits);
						}).tryAcquire()) {
					log.debug("Request rate limit reached for client IP " + clientIP);
					return false;
				}
			}

			// Defaulting to permitting the request
			return true;
		} catch (ExecutionException e) {
			log.error(e.getMessage());
		}
		return false;
	}

	private boolean isRateLimitedMethod(Method method) {
		return Method.GET.equals(method) || Method.HEAD.equals(method);
	}

	private boolean isRateLimitedPath(String path) {
		if (path == null) {
			throw new IllegalArgumentException("Path must not be null");
		}
		return !path.endsWith("/status");
	}

}