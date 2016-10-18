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

	private Cache<String, Cache> slidingWindows;

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
				slidingWindows = CacheBuilder.newBuilder().maximumSize(4096).build();
			} else {
				log.info("Rate limiting using averaging");
				rateLimiters = CacheBuilder.newBuilder().maximumSize(4096).build();
			}
		}
	}

	@Override
	protected int beforeHandle(Request request, Response response) {
		String dataset = request.getResourceRef().getPath();
		if (dataset != null &&
				rateLimitFilterEnabled &&
				request != null && isRateLimitedMethod(request.getMethod())) {
			if (!countAndCheckIfRequestPermitted(dataset)) {
				response.setStatus(Status.CLIENT_ERROR_TOO_MANY_REQUESTS);
				return STOP;
			}
		}
		return CONTINUE;
	}

	private boolean countAndCheckIfRequestPermitted(String dataset) {
		if (dataset == null) {
			throw new IllegalArgumentException("Dataset must not be null");
		}

		try {
			if (rateLimitTypeSlidingWindow) {
				// Checking for global rate limit
				Cache globalWindow = slidingWindows.get("global", new Callable<Cache>() {
					@Override
					public Cache call() throws Exception {
						return CacheBuilder.newBuilder().expireAfterWrite(config.getRateLimitTimeRange(), TimeUnit.SECONDS).build();
					}
				});
				globalWindow.cleanUp();
				if (globalWindow.size() >= config.getRateLimitRequestsGlobal()) {
					log.debug("Request rate limit reached globally");
					return false;
				}

				// Checking for per-dataset rate limit
				Cache datasetWindow = slidingWindows.get(dataset, new Callable<Cache>() {
					@Override
					public Cache call() throws Exception {
						return CacheBuilder.newBuilder().expireAfterWrite(config.getRateLimitTimeRange(), TimeUnit.SECONDS).build();
					}
				});
				datasetWindow.cleanUp();
				if (datasetWindow.size() >= config.getRateLimitRequestsDataset()) {
					log.debug("Request rate limit reached for " + dataset);
					return false;
				}

				globalWindow.put(new Date(), dummy);
				datasetWindow.put(new Date(), dummy);
			} else {
				// Checking for global rate limit
				if (config.getRateLimitRequestsGlobal() > 0 &&
						!rateLimiters.get("global", new Callable<RateLimiter>() {
							@Override
							public RateLimiter call() throws Exception {
								double permits = (double) config.getRateLimitRequestsGlobal() / (double) config.getRateLimitTimeRange();
								return RateLimiter.create(permits);
							}
						}).tryAcquire()) {
					log.debug("Request rate limit reached globally");
					return false;
				}

				// Checking for per-dataset rate limit
				if (config.getRateLimitRequestsDataset() > 0 &&
						!rateLimiters.get(dataset, new Callable<RateLimiter>() {
							@Override
							public RateLimiter call() throws Exception {
								double permits = (double) config.getRateLimitRequestsDataset() / (double) config.getRateLimitTimeRange();
								return RateLimiter.create(permits);
							}
						}).tryAcquire()) {
					log.debug("Request rate limit reached for " + dataset);
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

}