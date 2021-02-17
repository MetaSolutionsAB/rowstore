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

import java.util.Collections;
import java.util.Date;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Provides request rate limitation.
 * 
 * @author Hannes Ebner
 */
public class RateLimitFilter extends Filter {

	static private final Logger log = LoggerFactory.getLogger(RateLimitFilter.class);

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
		if (config.getRateLimitTimeRange() != -1 &&
				(config.getRateLimitRequestsGlobal() != -1 || config.getRateLimitRequestsDataset() != -1)) {
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
		}
	}

	@Override
	protected int beforeHandle(Request request, Response response) {
		String path = request.getResourceRef().getPath();
		if (rateLimitFilterEnabled &&
				path != null &&
				isRateLimitedPath(path) &&
				isRateLimitedMethod(request.getMethod())) {
			long checkResult = countAndCheckIfRequestPermitted(request);
			if (checkResult != 0) {
				if (checkResult > 0) {
					response.setRetryAfter(new Date(checkResult));
				}
				response.setStatus(Status.CLIENT_ERROR_TOO_MANY_REQUESTS);
				return STOP;
			}
		}
		return CONTINUE;
	}

	/**
	 *
	 * @param request Request to be checked against rate limiters
	 * @return 0 if request is permitted,
	 *        -1 if rate limit has been exceeded and the time is unknown when the next request may be carried out,
	 *        or a positive integer if the request is not permitted but the point in time for the next possible
	 *        request is known (in that case the return values corresponds to the next possible time for a request
	 *        in ms, e.g. to be used with new Date(time in ms))
	 */
	private long countAndCheckIfRequestPermitted(Request request) {
		if (request == null) {
			throw new IllegalArgumentException("Request parameter must not be null");
		}

		String dataset = request.getResourceRef().getPath();

		// NOTE: request.getClientInfo().getUpstreamAddress() does not seem to work (bug in Restlet),
		// so we try to read the header manually
		String clientIP = request.getHeaders().getFirstValue("x-forwarded-for", true, request.getClientInfo().getAddress());

		try {
			if (rateLimitTypeSlidingWindow) {
				// Checking for global rate limit
				Cache<Date, Object> globalWindow = slidingWindows.get("global", loader);
				globalWindow.cleanUp();
				if (globalWindow.size() >= config.getRateLimitRequestsGlobal()) {
					log.debug("Request rate limit reached globally");
					return calculateRetryAfter(globalWindow);
				}

				// Checking for per-dataset rate limit
				Cache<Date, Object> datasetWindow = slidingWindows.get(dataset, loader);
				datasetWindow.cleanUp();
				if (datasetWindow.size() >= config.getRateLimitRequestsDataset()) {
					log.debug("Request rate limit reached for " + dataset);
					return calculateRetryAfter(datasetWindow);
				}

				// Checking for per-client IP rate limit
				Cache<Date, Object> clientIPWindow = slidingWindows.get(clientIP, loader);
				clientIPWindow.cleanUp();
				if (clientIPWindow.size() >= config.getRateLimitRequestsClientIP()) {
					log.debug("Request rate limit reached for client IP " + clientIP);
					return calculateRetryAfter(clientIPWindow);
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
					return -1;
				}

				// Checking for per-dataset rate limit
				if (config.getRateLimitRequestsDataset() > 0 &&
						!rateLimiters.get(dataset, () -> {
							double permits = (double) config.getRateLimitRequestsDataset() / (double) config.getRateLimitTimeRange();
							return RateLimiter.create(permits);
						}).tryAcquire()) {
					log.debug("Request rate limit reached for " + dataset);
					return -1;
				}

				// Checking for per-client IP rate limit
				if (config.getRateLimitRequestsClientIP() > 0 &&
						!rateLimiters.get(clientIP, () -> {
							double permits = (double) config.getRateLimitRequestsClientIP() / (double) config.getRateLimitTimeRange();
							return RateLimiter.create(permits);
						}).tryAcquire()) {
					log.debug("Request rate limit reached for client IP " + clientIP);
					return -1;
				}
			}

			// Defaulting to permitting the request
			return 0;
		} catch (ExecutionException e) {
			log.error(e.getMessage());
		}
		return -1;
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

	private long calculateRetryAfter(Cache<Date, Object> cache) {
		try {
			// we fetch the oldest entry and add the time range in order to get the time for the next possible request
			// we also add 1 ms in order to avoid corner cases with inclusive vs exclusive boundaries
			return Collections.min(cache.asMap().keySet()).getTime() + (config.getRateLimitTimeRange() * 1000L) + 1L;
		} catch (NoSuchElementException ignored) {
			// it could be the case that the collection is emptied
			// while be are in this function (which leads to an exception)
		}
		return new Date().getTime();
	}

}