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
 * @author Hannes Ebner
 */
public class RowStoreConfig {

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

	public boolean hasRegExpQuerySupport() {
		return regExpSupport;
	}

}