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

package org.entrystore.rowstore.store;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Loads the configuration from a JSON file and provides convenience methods to access configuration properties.
 *
 * @author Hannes Ebner
 */
public class RowStoreConfig {

	private final static Logger log = LoggerFactory.getLogger(RowStoreConfig.class);

	private String logLevel;

	private String baseURL;

	private int regExpSupport;

	private int maxEtlProcesses;

	private String rateLimitType;

	private boolean legacyParserEnabled;

	private int rateLimitTimeRange = -1;

	private int rateLimitRequestsGlobal = -1;

	private int rateLimitRequestsDataset = -1;

	private int rateLimitRequestsClientIP = -1;

	private boolean rateLimitEnabled = false;

	private int queryTimeout = -1;

	private int queryMaxLimit = -1;

	private int exportPageSize = -1;

	private long maxUploadSize = 104857600; // 100 MB default

	private List<String> corsAllowedOrigins = List.of("*");

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

			// Parser
			legacyParserEnabled = config.optBoolean("legacyparser", false);

			// Database
			database = new Database(config.getJSONObject("database"));
			requirePositive(config.getJSONObject("database"), "database", CONNECTION_LIMIT_KEYS);
			requireNonNegative(config.getJSONObject("database"), "database", "queryLockTimeout");
			if (config.has("queryDatabase")) {
				queryDatabase = new Database(config.getJSONObject("queryDatabase"));
				requirePositive(config.getJSONObject("queryDatabase"), "queryDatabase", CONNECTION_LIMIT_KEYS);
				requireNonNegative(config.getJSONObject("queryDatabase"), "queryDatabase", "queryLockTimeout");
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

			// Page size for queries when exporting
			exportPageSize = config.optInt("exportpagesize", 100000);

			// Max upload size in bytes (default 100 MB)
			maxUploadSize = config.optLong("maxuploadsize", 104857600);

			// CORS allowed origins
			if (config.has("cors")) {
				JSONObject corsConfig = config.getJSONObject("cors");
				if (corsConfig.has("allowedorigins")) {
					JSONArray origins = corsConfig.getJSONArray("allowedorigins");
					List<String> originList = new ArrayList<>();
					for (int i = 0; i < origins.length(); i++) {
						originList.add(origins.getString(i));
					}
					if (!originList.isEmpty()) {
						corsAllowedOrigins = originList;
					}
				}
			}
		} catch (JSONException e) {
			log.error(e.getMessage());
		}
	}

	/**
	 * Keys of the connection limits in a database block (CFG-4.05 to CFG-4.07), which must be greater than 0 if set.
	 */
	private static final String[] CONNECTION_LIMIT_KEYS = {"maxQueryConnections", "maxExportConnections", "connectionWaitTimeout"};

	/**
	 * @throws IllegalStateException If one of the keys is present in the block and not a number greater than 0.
	 */
	private static void requirePositive(JSONObject block, String blockName, String... keys) {
		for (String key : keys) {
			if (block.has(key) && block.optInt(key, 0) <= 0) {
				throw new IllegalStateException("rowstore.json: '" + blockName + "." + key + "' must be a number greater than 0");
			}
		}
	}

	/**
	 * @throws IllegalStateException If the key is present in the block and not a number greater than or equal to 0.
	 */
	private static void requireNonNegative(JSONObject block, String blockName, String key) {
		if (block.has(key) && block.optInt(key, -1) < 0) {
			throw new IllegalStateException("rowstore.json: '" + blockName + "." + key + "' must be a number greater than or equal to 0");
		}
	}

	public String getLogLevel() {
		return logLevel;
	}

	public boolean isLegacyParserEnabled() { return legacyParserEnabled; }

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

	public int getExportPageSize() {
		return exportPageSize;
	}

	public Database getDatabase() {
		return database;
	}

	public long getMaxUploadSize() {
		return maxUploadSize;
	}

	public List<String> getCorsAllowedOrigins() {
		return corsAllowedOrigins;
	}

	public Database getQueryDatabase() {
		return queryDatabase;
	}

	public class Database {

		public static final int DEFAULT_MAX_QUERY_CONNECTIONS = 10;

		public static final int DEFAULT_MAX_EXPORT_CONNECTIONS = 2;

		public static final int DEFAULT_CONNECTION_WAIT_TIMEOUT = 30;

		private String user;

		private String password;

		private String host;

		private int port;

		private String type;

		private String name;

		private boolean ssl;

		private int connectionPoolInit;

		private int connectionPoolMax;

		private int socketTimeout;

		private int connectTimeout;

		private int loginTimeout;

		private int maxQueryConnections = DEFAULT_MAX_QUERY_CONNECTIONS;

		private int maxExportConnections = DEFAULT_MAX_EXPORT_CONNECTIONS;

		private int connectionWaitTimeout = -1;

		private int queryLockTimeout = -1;

		Database() {
		}

		public Database(JSONObject dbConfig) {
			setType(dbConfig.optString("type", "postgresql"));
			setHost(dbConfig.getString("host"));
			setPort(dbConfig.optInt("port", 5432));
			setName(dbConfig.getString("database"));
			setUser(dbConfig.getString("user"));
			setPassword(dbConfig.getString("password"));
			setSsl(dbConfig.optBoolean("ssl", false));
			setConnectionPoolInit(dbConfig.optInt("connectionPoolInit", -1));
			setConnectionPoolMax(dbConfig.optInt("connectionPoolMax", -1));
			setSocketTimeout(dbConfig.optInt("socketTimeout", 0));		// seconds, PG default 0
			setConnectTimeout(dbConfig.optInt("connectTimeout", 5));	// seconds, PG default 10
			setLoginTimeout(dbConfig.optInt("loginTimeout", 5));		// seconds, PG default 0
			setMaxQueryConnections(dbConfig.optInt("maxQueryConnections", DEFAULT_MAX_QUERY_CONNECTIONS));
			setMaxExportConnections(dbConfig.optInt("maxExportConnections", DEFAULT_MAX_EXPORT_CONNECTIONS));
			setConnectionWaitTimeout(dbConfig.optInt("connectionWaitTimeout", -1));	// seconds, see getConnectionWaitTimeout()
			setQueryLockTimeout(dbConfig.optInt("queryLockTimeout", -1));	// seconds, see getQueryLockTimeoutMillis()
		}

		public Database setMaxQueryConnections(int maxQueryConnections) {
			this.maxQueryConnections = maxQueryConnections;
			return this;
		}

		public Database setMaxExportConnections(int maxExportConnections) {
			this.maxExportConnections = maxExportConnections;
			return this;
		}

		public Database setQueryLockTimeout(int queryLockTimeout) {
			this.queryLockTimeout = queryLockTimeout;
			return this;
		}

		public Database setConnectionWaitTimeout(int connectionWaitTimeout) {
			this.connectionWaitTimeout = connectionWaitTimeout;
			return this;
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

		public Database setSsl(boolean ssl) {
			this.ssl = ssl;
			return this;
		}

		public Database setConnectionPoolInit(int connectionPoolInit) {
			this.connectionPoolInit = connectionPoolInit;
			return this;
		}

		public Database setConnectionPoolMax(int connectionPoolMax) {
			this.connectionPoolMax = connectionPoolMax;
			return this;
		}

		public Database setSocketTimeout(int socketTimeout) {
			this.socketTimeout = socketTimeout;
			return this;
		}

		public Database setConnectTimeout(int connectTimeout) {
			this.connectTimeout = connectTimeout;
			return this;
		}

		public Database setLoginTimeout(int loginTimeout) {
			this.loginTimeout = loginTimeout;
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

		public boolean getSsl() {
			return ssl;
		}

		public int getConnectionPoolInit() {
			return connectionPoolInit;
		}

		public int getConnectionPoolMax() {
			return connectionPoolMax;
		}

		public int getSocketTimeout() {
			return socketTimeout;
		}

		public int getConnectTimeout() {
			return connectTimeout;
		}

		public int getLoginTimeout() {
			return loginTimeout;
		}

		/**
		 * @return Returns the maximum number of connections for short operations (CFG-4.05).
		 */
		public int getMaxQueryConnections() {
			return maxQueryConnections;
		}

		/**
		 * @return Returns the maximum number of connections for exports (CFG-4.06).
		 */
		public int getMaxExportConnections() {
			return maxExportConnections;
		}

		/**
		 * @return Returns the maximum time in seconds to wait for a connection (CFG-4.07): the configured value, or
		 * querytimeout if no value is configured and querytimeout is greater than 0, or 30.
		 */
		public int getConnectionWaitTimeout() {
			if (connectionWaitTimeout > 0) {
				return connectionWaitTimeout;
			}
			return queryTimeout > 0 ? queryTimeout : DEFAULT_CONNECTION_WAIT_TIMEOUT;
		}

		/**
		 * @return Returns the lock timeout in milliseconds of connections for short operations (CFG-4.08): the
		 * configured value, 0 if disabled (no lock timeout is set), or half of the connection wait timeout.
		 */
		public long getQueryLockTimeoutMillis() {
			if (queryLockTimeout >= 0) {
				return queryLockTimeout * 1000L;
			}
			return Math.max(1, getConnectionWaitTimeout() * 1000L / 2);
		}

	}

}