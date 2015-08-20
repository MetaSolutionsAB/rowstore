package org.entrystore.rowstore.store;

import org.json.JSONObject;

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

	public RowStoreConfig(JSONObject config) {
		// TODO
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

}