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

import com.google.common.net.InetAddresses;
import org.apache.catalina.util.NetMask;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * Loads the configuration from a JSON file and provides convenience methods to access configuration properties.
 *
 * @author Hannes Ebner
 */
public class RowStoreConfig {

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

	private int queryTimeout = -1;

	private int queryMaxLimit = -1;

	private long maxUploadSize = 104857600; // 100 MB default

	private List<String> corsAllowedOrigins = List.of("*");

	// null if not configured: the connecting peer is then trusted as the reverse proxy
	private List<String> trustedProxies = null;

	private Database database;

	private Database queryDatabase;

	/**
	 * @throws IllegalStateException if the configuration is invalid, with a message naming the offending key
	 */
	public RowStoreConfig(JSONObject config) {
		try {
			parse(config);
		} catch (JSONException e) {
			throw new IllegalStateException("rowstore.json invalid: " + e.getMessage(), e);
		}
	}

	private void parse(JSONObject config) {
		// Base URL
		baseURL = requiredString(config, "baseurl", "baseurl");

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
		database = new Database(requiredObject(config, "database"), "database");
		JSONObject queryDatabaseConfig = optionalObject(config, "queryDatabase");
		queryDatabase = queryDatabaseConfig != null ? new Database(queryDatabaseConfig, "queryDatabase") : database;

		// Rate limitation
		JSONObject rateLimitConfig = optionalObject(config, "ratelimit");
		if (rateLimitConfig != null) {
			rateLimitType = rateLimitConfig.optString("type", "slidingwindow");
			rateLimitTimeRange = rateLimitConfig.optInt("timerange", -1);
			rateLimitRequestsGlobal = rateLimitConfig.optInt("global", -1);
			rateLimitRequestsDataset = rateLimitConfig.optInt("dataset", -1);
			rateLimitRequestsClientIP = rateLimitConfig.optInt("clientip", -1);
		}

		// Query time out
		queryTimeout = config.optInt("querytimeout", -1);

		// Maximum size of reponse size limit (i.e. "_limit" in the URL parameters)
		queryMaxLimit = config.optInt("querymaxlimit", 100);
		if (queryMaxLimit <= 0) {
			throw new IllegalStateException("rowstore.json: 'querymaxlimit' must be greater than 0");
		}

		// Max upload size in bytes (default 100 MB)
		maxUploadSize = config.optLong("maxuploadsize", 104857600);
		if (maxUploadSize <= 0) {
			throw new IllegalStateException("rowstore.json: 'maxuploadsize' must be greater than 0");
		}

		// CORS allowed origins
		JSONObject corsConfig = optionalObject(config, "cors");
		if (corsConfig != null && corsConfig.has("allowedorigins")) {
			JSONArray origins = corsConfig.optJSONArray("allowedorigins");
			if (origins == null) {
				throw wrongType("cors.allowedorigins", "an array of strings");
			}
			List<String> originList = new ArrayList<>();
			for (int i = 0; i < origins.length(); i++) {
				if (!(origins.get(i) instanceof String origin)) {
					throw wrongType("cors.allowedorigins", "an array of strings");
				}
				originList.add(origin);
			}
			if (!originList.isEmpty()) {
				corsAllowedOrigins = originList;
			}
		}

		// Reverse proxies whose X-Forwarded-For header is trusted; an empty array trusts nobody, without the key the
		// connecting peer is trusted
		if (config.has("trustedproxies")) {
			JSONArray proxies = config.optJSONArray("trustedproxies");
			if (proxies == null) {
				throw wrongType("trustedproxies", "an array of strings");
			}
			List<String> proxyList = new ArrayList<>();
			for (int i = 0; i < proxies.length(); i++) {
				if (!(proxies.get(i) instanceof String proxy)) {
					throw wrongType("trustedproxies", "an array of strings");
				}
				if (!isIpAddressOrCidr(proxy)) {
					throw new IllegalStateException("rowstore.json: 'trustedproxies' contains '" + proxy +
							"', which is not an IP address or CIDR range");
				}
				proxyList.add(proxy);
			}
			trustedProxies = List.copyOf(proxyList);
		}
	}

	/**
	 * Accepts IP address literals only (no host names, which would require DNS lookups), with an optional prefix length.
	 * NetMask's ";port" suffix and IPv6 zone IDs ("%...") are not accepted.
	 */
	private static boolean isIpAddressOrCidr(String value) {
		int slash = value.indexOf('/');
		if (value.indexOf(';') >= 0 || value.indexOf('%') >= 0 ||
				!InetAddresses.isInetAddress(slash < 0 ? value : value.substring(0, slash))) {
			return false;
		}
		try {
			new NetMask(value);
			return true;
		} catch (IllegalArgumentException e) {
			return false;
		}
	}

	private static String requiredString(JSONObject obj, String key, String qualifiedKey) {
		if (!obj.has(key)) {
			throw missingKey(qualifiedKey);
		}
		if (!(obj.get(key) instanceof String value)) {
			throw wrongType(qualifiedKey, "a string");
		}
		return value;
	}

	private static JSONObject requiredObject(JSONObject obj, String key) {
		if (!obj.has(key)) {
			throw missingKey(key);
		}
		return optionalObject(obj, key);
	}

	/**
	 * @return the JSON object for the key, or null if the key is not present
	 * @throws IllegalStateException if the key is present but not a JSON object
	 */
	private static JSONObject optionalObject(JSONObject obj, String key) {
		if (!obj.has(key)) {
			return null;
		}
		JSONObject result = obj.optJSONObject(key);
		if (result == null) {
			throw wrongType(key, "a JSON object");
		}
		return result;
	}

	private static IllegalStateException missingKey(String qualifiedKey) {
		return new IllegalStateException("rowstore.json: missing required key '" + qualifiedKey + "'");
	}

	private static IllegalStateException wrongType(String qualifiedKey, String expected) {
		return new IllegalStateException("rowstore.json: '" + qualifiedKey + "' must be " + expected);
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

	public long getMaxUploadSize() {
		return maxUploadSize;
	}

	public List<String> getCorsAllowedOrigins() {
		return corsAllowedOrigins;
	}

	public Database getQueryDatabase() {
		return queryDatabase;
	}

	/**
	 * @return IP addresses and CIDR ranges of trusted reverse proxies, validated by {@link NetMask}, or null if
	 * "trustedproxies" is not configured, in which case the connecting peer is trusted as the reverse proxy
	 */
	public List<String> getTrustedProxies() {
		return trustedProxies;
	}

	public class Database {

		private String user;

		private String password;

		private String host;

		private int port;

		private String type;

		private String name;

		// null if not configured
		private Boolean ssl;

		private int connectionPoolInit;

		private int connectionPoolMax;

		private int socketTimeout;

		private int connectTimeout;

		private int loginTimeout;

		Database() {
		}

		/**
		 * @param name the key of the block in rowstore.json, used in error messages
		 */
		public Database(JSONObject dbConfig, String name) {
			setType(dbConfig.optString("type", "postgresql"));
			setHost(requiredString(dbConfig, "host", name + ".host"));
			setPort(dbConfig.optInt("port", 5432));
			setName(requiredString(dbConfig, "database", name + ".database"));
			setUser(requiredString(dbConfig, "user", name + ".user"));
			setPassword(requiredString(dbConfig, "password", name + ".password"));
			if (dbConfig.has("ssl")) {
				setSsl(dbConfig.optBoolean("ssl", false));
			}
			setConnectionPoolInit(dbConfig.optInt("connectionPoolInit", -1));
			setConnectionPoolMax(dbConfig.optInt("connectionPoolMax", -1));
			setSocketTimeout(dbConfig.optInt("socketTimeout", 0));		// seconds, PG default 0
			setConnectTimeout(dbConfig.optInt("connectTimeout", 5));	// seconds, PG default 10
			setLoginTimeout(dbConfig.optInt("loginTimeout", 5));		// seconds, PG default 0
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
			return Boolean.TRUE.equals(ssl);
		}

		/**
		 * @return the pgjdbc sslmode: require if ssl is true, disable if ssl is false, and the
		 * pgjdbc default prefer if ssl is not configured (as in 1.7)
		 */
		public String getSslMode() {
			if (ssl == null) {
				return "prefer";
			}
			return ssl ? "require" : "disable";
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

	}

}