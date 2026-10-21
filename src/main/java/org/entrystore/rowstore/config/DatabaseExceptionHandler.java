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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Maps database connectivity failures (e.g., the database is unreachable) that propagate from a controller to
 * 503 Service Unavailable with an empty body. All other exceptions keep their default handling.
 */
@RestControllerAdvice
public class DatabaseExceptionHandler {

	private static final Logger log = LoggerFactory.getLogger(DatabaseExceptionHandler.class);

	@ExceptionHandler(DataAccessResourceFailureException.class)
	public ResponseEntity<Void> handleDatabaseUnavailable(DataAccessResourceFailureException e,
			HttpServletResponse response) {
		if (response.isCommitted()) {
			// the status of a streamed response cannot be changed anymore
			throw e;
		}
		log.error("Database unavailable: {}", e.getMessage());
		return new ResponseEntity<>(HttpStatus.SERVICE_UNAVAILABLE);
	}

}
