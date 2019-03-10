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

	private String logLevel;

	private String baseURL;

	private int regExpSupport;

	private int maxEtlProcesses;

	private String rateLimitType;

	private int rateLimitTimeRange = -1;

	private int rateLimitRequestsGlobal = -1;

	private int rateLimitRequestsDataset = -1;

	private int rateLimitRequestsClientIP = -1;

	private boolean rateLimitEnabled = false;

	private int queryTimeout = -1;

	private int queryMaxLimit = -1;

	private Database database;

	private Database queryDatabase;

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
			database = new Database(config.getJSONObject("database"));
			if (config.has("queryDatabase")) {
				queryDatabase = new Database(config.getJSONObject("queryDatabase"));
			} else {
				queryDatabase = database;
			}

			// Rate limitation
			if (config.has("ratelimit")) {
				JSONObject rateLimitConfig = config.getJSONObject("ratelimit");
				rateLimitType = rateLimitConfig.optString("type", "slidingwindow");
				rateLimitTimeRange = rateLimitConfig.optInt("timerange", -1);
				rateLimitRequestsGlobal = rateLimitConfig.optInt("global", -1);
				rateLimitRequestsDataset = rateLimitConfig.optInt("dataset", -1);
				rateLimitRequestsClientIP = rateLimitConfig.optInt("clientip", -1);
				if (rateLimitTimeRange > 0 && (rateLimitRequestsGlobal > 0 || rateLimitRequestsDataset > 0)) {
					rateLimitEnabled = true;
				}
			}

			// Query time out
			queryTimeout = config.optInt("querytimeout", -1);

			// Maximum size of reponse size limit (i.e. "_limit" in the URL parameters)
			queryMaxLimit = config.optInt("querymaxlimit", 100);
		} catch (JSONException e) {
			log.error(e.getMessage());
		}
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

	public int getRateLimitRequestsClientIP() {
		return rateLimitRequestsClientIP;
	}

	public String getRateLimitType() {
		return rateLimitType;
	}

	public int getQueryTimeout() {
		return queryTimeout;
	}

	public int getQueryMaxLimit() {
		return queryMaxLimit;
	}

	public Database getDatabase() {
		return database;
	}

	public Database getQueryDatabase() {
		return queryDatabase;
	}

	public class Database {

		private String user;

		private String password;

		private String host;

		private int port;

		private String type;

		private String name;

		private boolean ssl;

		private int maxConnections;

		Database() {
		}

		public Database(JSONObject dbConfig) {
			setType(dbConfig.optString("type", "postgresql"));
			setHost(dbConfig.getString("host"));
			setPort(dbConfig.optInt("port", 5432));
			setName(dbConfig.getString("database"));
			setUser(dbConfig.getString("user"));
			setPassword(dbConfig.getString("password"));
			setDbMaxConnections(dbConfig.optInt("maxconnections", 30));
			setSsl(dbConfig.optBoolean("ssl", false));
		}

		public Database setUser(String user) {
			this.user = user;
			return this;
		}

		public Database setPassword(String password) {
			this.password = password;
			return this;
		}

		public Database setHost(String host) {
			this.host = host;
			return this;
		}

		public Database setPort(int port) {
			this.port = port;
			return this;
		}

		public Database setType(String type) {
			this.type = type;
			return this;
		}

		public Database setName(String name) {
			this.name = name;
			return this;
		}

		public Database setDbMaxConnections(int maxConnections) {
			this.maxConnections = maxConnections;
			return this;
		}

		public Database setSsl(boolean ssl) {
			this.ssl = ssl;
			return this;
		}

		public String getUser() {
			return user;
		}

		public String getPassword() {
			return password;
		}

		public String getHost() {
			return host;
		}

		public int getPort() {
			return port;
		}

		public String getType() {
			return type;
		}

		public String getName() {
			return name;
		}

		public int getMaxConnections() {
			return maxConnections;
		}

		public boolean getSsl() {
			return ssl;
		}

	}

}