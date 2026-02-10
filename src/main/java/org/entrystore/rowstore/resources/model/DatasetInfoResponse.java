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

package org.entrystore.rowstore.resources.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Set;

public record DatasetInfoResponse(
		int status,
		String created,
		Set<String> columnnames,
		long rowcount,
		String identifier,
		Set<String> aliases,
		@JsonProperty("@context") String context,
		@JsonProperty("@id") String id
) {}
