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

import org.springframework.boot.diagnostics.AbstractFailureAnalyzer;
import org.springframework.boot.diagnostics.FailureAnalysis;

/**
 * Reports a missing, unreadable or invalid configuration (CFG-1.02, CFG-1.03) as an error message instead of the stack
 * trace of the failed bean creation. Registered in {@code META-INF/spring.factories}.
 */
public class RowStoreConfigFailureAnalyzer extends AbstractFailureAnalyzer<RowStoreConfigException> {

	@Override
	protected FailureAnalysis analyze(Throwable rootFailure, RowStoreConfigException cause) {
		return new FailureAnalysis(cause.getMessage(),
				"Fix the configuration (see the Configuration section of the README) and start RowStore again.", cause);
	}

}
