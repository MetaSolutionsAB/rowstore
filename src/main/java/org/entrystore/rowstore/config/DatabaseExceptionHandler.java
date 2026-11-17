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

package org.entrystore.rowstore.config;

import jakarta.servlet.http.HttpServletResponse;
import org.entrystore.rowstore.store.impl.LimitedDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Maps database connectivity failures (e.g., the database is unreachable or no connection became available within
 * the wait timeout) and temporary unavailability (e.g., a lock timeout of a short operation, ARCH-7.03) that
 * propagate from a controller to 503 Service Unavailable with an empty body and a Retry-After header (ARCH-7.09).
 * All other exceptions keep their default handling.
 * <p>
 * A connectivity failure is logged at ERROR level for every request. A request that got no connection within the
 * wait timeout is logged at DEBUG level only, since the connection budget logs aggregated warnings about it
 * (LimitedDataSource), and lock and statement timeouts are logged at WARN level at most once per
 * {@link #WARN_INTERVAL_MILLIS}, with the number of such responses since the last warning, so that an overloaded
 * instance does not flood its log.
 */
@RestControllerAdvice
public class DatabaseExceptionHandler {

	private static final Logger log = LoggerFactory.getLogger(DatabaseExceptionHandler.class);

	/**
	 * Seconds after which a client may retry a request that got 503 because the database was busy or unavailable.
	 */
	static final int RETRY_AFTER_SECONDS = 5;

	static final long WARN_INTERVAL_MILLIS = 60000;

	private final AtomicLong lastWarning = new AtomicLong();

	private final AtomicInteger unwarned = new AtomicInteger();

	@ExceptionHandler({DataAccessResourceFailureException.class, TransientDataAccessResourceException.class})
	public ResponseEntity<Void> handleDatabaseUnavailable(DataAccessException e, HttpServletResponse response) {
		if (response.isCommitted()) {
			// the status of a streamed response cannot be changed anymore
			throw e;
		}
		// the message of a DataAccessException does not include its cause
		String cause = NestedExceptionUtils.getMostSpecificCause(e).getMessage();
		if (LimitedDataSource.isWaitTimeout(e)) {
			log.debug("No database connection available: {} ({})", e.getMessage(), cause);
		} else if (e instanceof TransientDataAccessResourceException) {
			warnAggregated(e.getMessage(), cause);
		} else {
			log.error("Database unavailable: {} ({})", e.getMessage(), cause);
		}
		return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
				.header(HttpHeaders.RETRY_AFTER, String.valueOf(RETRY_AFTER_SECONDS))
				.build();
	}

	private void warnAggregated(String message, String cause) {
		int count = unwarned.incrementAndGet();
		long now = System.currentTimeMillis();
		long last = lastWarning.get();
		if (now - last >= WARN_INTERVAL_MILLIS && lastWarning.compareAndSet(last, now)) {
			log.warn("Database temporarily unavailable (lock or statement timeout), {} response(s) with 503 since the " +
					"last warning; latest: {} ({})", unwarned.getAndSet(0), message, cause);
		} else {
			log.debug("Database temporarily unavailable ({} not yet reported): {} ({})", count, message, cause);
		}
	}

}
