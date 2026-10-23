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

/**
 * Thrown at startup if the configuration is missing, cannot be read or is invalid. The message names the problem
 * (e.g., the offending key) and is reported without a stack trace by {@link RowStoreConfigFailureAnalyzer}.
 */
public class RowStoreConfigException extends IllegalStateException {

	public RowStoreConfigException(String message) {
		super(message);
	}

	public RowStoreConfigException(String message, Throwable cause) {
		super(message, cause);
	}

}
