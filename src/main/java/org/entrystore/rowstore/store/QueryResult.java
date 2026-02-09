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

	private long resultCount;

	private List<JSONObject> results;

	private String status;

	private long queryTime;

	public QueryResult(List<JSONObject> results, int limit, int offset, long resultCount, long queryTime) {
		this(results, limit, offset, resultCount, queryTime, null);
	}

	public QueryResult(List<JSONObject> results, int limit, int offset, long resultCount, long queryTime, String status) {
		this.limit = limit;
		this.offset = offset;
		this.resultCount = resultCount;
		this.results = results;
		this.queryTime = queryTime;
		this.status = status;
	}

	public int getLimit() {
		return limit;
	}

	public int getOffset() {
		return offset;
	}

	public long getResultCount() {
		return resultCount;
	}

	public List<JSONObject> getResults() {
		return results;
	}

	public String getStatus() {
		return status;
	}

	public long getQueryTime() {
		return queryTime;
	}

	public static class Error extends QueryResult {

		public Error(String sqlStatus) {
			super(null, 0, 0, 0, 0, sqlStatus);
		}

	}

}