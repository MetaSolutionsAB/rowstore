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

package org.entrystore.rowstore.store;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads the configuration from a JSON file and provides convenience methods to access configuration properties.
 *
 * @author Hannes Ebner
 */
public class RowStoreConfig {

	private static Logger log = LoggerFactory.getLogger(RowStoreConfig.class);

	private String dbUser;

	private String dbPassword;

	private String dbHost;

	private int dbPort;

	private String dbType;

	private String dbName;

	private boolean dbSsl;

	private int dbMaxConnections;

	private String logLevel;

	private String baseURL;

	private int regExpSupport;

	private int maxEtlProcesses;

	private String rateLimitType;

	private int rateLimitTimeRange = -1;

	private int rateLimitRequestsGlobal = -1;

	private int rateLimitRequestsDataset = -1;

	private boolean rateLimitEnabled = false;

	public RowStoreConfig(JSONObject config) {
		try {
			// Base URL
			baseURL = config.getString("baseurl");
			// Support for RegExp pattern matching
			String regExpSupportStr = config.optString("regexpqueries", "false");
			if ("simple".equalsIgnoreCase(regExpSupportStr)) {
				regExpSupport = Dataset.REGEXP_QUERY_SIMPLE;
			} else if ("full".equalsIgnoreCase(regExpSupportStr) || "true".equalsIgnoreCase(regExpSupportStr)) {
				regExpSupport = Dataset.REGEXP_QUERY_FULL;
			} else {
				regExpSupport = Dataset.REGEXP_QUERY_DISABLED;
			}
			// ETL
			maxEtlProcesses = config.optInt("maxetlprocesses", 5);
			// Logging
			logLevel = config.optString("loglevel", "info");

			// Database
			JSONObject dbConfig = config.getJSONObject("database");
			dbType = dbConfig.optString("type", "postgresql");
			dbHost = dbConfig.getString("host");
			dbPort = dbConfig.optInt("port", 5432);
			dbName = dbConfig.getString("database");
			dbUser = dbConfig.getString("user");
			dbPassword = dbConfig.getString("password");
			dbMaxConnections = dbConfig.optInt("maxconnections", 30);
			dbSsl = dbConfig.optBoolean("ssl", false);

			// Rate limitation
			if (config.has("ratelimit")) {
				JSONObject rateLimitConfig = config.getJSONObject("ratelimit");
				rateLimitType = rateLimitConfig.optString("type", "slidingwindow");
				rateLimitTimeRange = rateLimitConfig.optInt("timerange", -1);
				rateLimitRequestsGlobal = rateLimitConfig.optInt("global", -1);
				rateLimitRequestsDataset = rateLimitConfig.optInt("dataset", -1);
				if (rateLimitTimeRange > 0 && (rateLimitRequestsGlobal > 0 || rateLimitRequestsDataset > 0)) {
					rateLimitEnabled = true;
				}
			}
		} catch (JSONException e) {
			log.error(e.getMessage());
		}
	}

	public String getDbUser() {
		return dbUser;
	}

	public String getDbPassword() {
		return dbPassword;
	}

	public String getDbHost() {
		return dbHost;
	}

	public int getDbPort() {
		return dbPort;
	}

	public String getDbType() {
		return dbType;
	}

	public String getDbName() {
		return dbName;
	}

	public int getDbMaxConnections() {
		return dbMaxConnections;
	}

	public boolean getDbSsl() {
		return dbSsl;
	}

	public String getLogLevel() {
		return logLevel;
	}

	public String getBaseURL() {
		return baseURL;
	}

	public int getMaxEtlProcesses() {
		return maxEtlProcesses;
	}

	public int getRegexpQuerySupport() {
		return regExpSupport;
	}

	public boolean isRateLimitEnabled() {
		return rateLimitEnabled;
	}

	public int getRateLimitTimeRange() {
		return rateLimitTimeRange;
	}

	public int getRateLimitRequestsGlobal() {
		return rateLimitRequestsGlobal;
	}

	public int getRateLimitRequestsDataset() {
		return rateLimitRequestsDataset;
	}

	public String getRateLimitType() {
		return rateLimitType;
	}

}