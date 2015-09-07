/*
 * Copyright (c) 2011-2015 MetaSolutions AB <info@metasolutions.se>
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

package org.entrystore.rowstore.store.impl;

import org.slf4j.Logger;

import java.sql.SQLException;

/**
 * Provides a utility method to log SQLExceptions, in particular the recursive getNextException().
 *
 * @author Hannes Ebner
 */
public class SqlExceptionLogUtil {

	private static void error(Logger log, SQLException exception, int loopCount) {
		if (log == null || exception == null) {
			throw new IllegalArgumentException("Arguments must not be null");
		}
		log.error(exception.getMessage());
		if (exception.getNextException() != null && ++loopCount < 10) {
			error(log, exception.getNextException(), loopCount);
		}
	}

	/**
	 * Writes all error messages (including SQLException's getNextException(), recursively) to the logger's error-method.
	 *
	 * @param log The logger's instance
	 * @param exception A SQLException
	 */
	public static void error(Logger log, SQLException exception) {
		error(log, exception, 0);
	}

}