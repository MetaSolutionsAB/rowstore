package org.entrystore.rowstore.store;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Hannes Ebner
 */
public class RowStoreConfig {

	private String dbUser;

	private String dbPassword;

	private String dbHost;

	private String dbType;

	private String dbName;

	private int dbMaxConnections;

	private String logLevel;

	private String baseURL;

	private boolean regExpSupport;

	private int maxEtlProcesses;

	private static Logger log = LoggerFactory.getLogger(RowStoreConfig.class);

	public RowStoreConfig(JSONObject config) {
		try {
			// Base URL
			baseURL = config.getString("baseurl");

			// Support for RegExp pattern matching
			regExpSupport = config.optBoolean("regexpqueries", false);

			// ETL
			maxEtlProcesses = config.optInt("maxetlprocesses", 5);

			// Database
			JSONObject dbConfig = config.getJSONObject("database");
			dbType = dbConfig.optString("type", "postgresql");
			dbHost = dbConfig.getString("host");
			dbName = dbConfig.getString("database");
			dbUser = dbConfig.getString("user");
			dbPassword = dbConfig.getString("password");
			dbMaxConnections = dbConfig.optInt("maxconnections", 30);

			// Logging
			logLevel = dbConfig.optString("loglevel", "info");
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

	public String getDbType() {
		return dbType;
	}

	public String getDbName() {
		return dbName;
	}

	public int getDbMaxConnections() {
		return dbMaxConnections;
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

	public boolean hasRegExpQuerySupport() {
		return regExpSupport;
	}

}