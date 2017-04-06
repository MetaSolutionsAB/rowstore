/*
 * Copyright (c) 2011-2017 MetaSolutions AB <info@metasolutions.se>
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

package org.entrystore.rowstore.store;

import org.json.JSONObject;

import java.util.List;

public class QueryResult {

	private int limit;

	private int offset;

	private int resultCount;

	private List<JSONObject> results;

	public QueryResult(List<JSONObject> results, int limit, int offset, int resultCount) {
		this.limit = limit;
		this.offset = offset;
		this.resultCount = resultCount;
		this.results = results;
	}

	public int getLimit() {
		return limit;
	}

	public int getOffset() {
		return offset;
	}

	public int getResultCount() {
		return resultCount;
	}

	public List<JSONObject> getResults() {
		return results;
	}

}