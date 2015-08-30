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

package org.entrystore.rowstore.store.impl;

import org.entrystore.rowstore.etl.EtlStatus;
import org.entrystore.rowstore.store.Dataset;
import org.entrystore.rowstore.store.Datasets;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * @author Hannes Ebner
 */
public class PgDatasets implements Datasets {

	private static Logger log = LoggerFactory.getLogger(PgDatasets.class);

	public static String TABLE_NAME = "datasets";

	PgRowStore rowstore;

	protected PgDatasets(PgRowStore rowstore) {
		this.rowstore = rowstore;
		createTableIfNotExists();
	}

	@Override
	public Set<Dataset> getAll() {
		Set<Dataset> result = new HashSet<>();
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			conn = getRowStore().getConnection();
			stmt = conn.prepareStatement("SELECT * FROM " + TABLE_NAME);
			log.info("Executing: " + stmt);
			rs = stmt.executeQuery();
			while (rs.next()) {
				UUID id = (UUID) rs.getObject("id");
				int status = rs.getInt("status");
				Timestamp created = rs.getTimestamp("created");
				String dataTable = rs.getString("data_table");
				result.add(new PgDataset(rowstore, id.toString(), status, created, dataTable));
			}
			rs.close();
		} catch (SQLException e) {
			SqlExceptionLogUtil.error(log, e);
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					SqlExceptionLogUtil.error(log, e);
				}
			}
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					SqlExceptionLogUtil.error(log, e);
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					SqlExceptionLogUtil.error(log, e);
				}
			}
		}

		return result;
	}

	@Override
	public Dataset createDataset(String id) {
		if (id == null) {
			throw new IllegalArgumentException("Dataset ID must not be null");
		}
		Connection conn = null;
		String dataTable = constructDataTableName(id);
		try {
			conn = getRowStore().getConnection();
			conn.setAutoCommit(false);

			PreparedStatement ps = conn.prepareStatement("INSERT INTO " + TABLE_NAME + " (id, status, created, data_table) VALUES (?, ?, ?, ?)");
			PGobject uuid = new PGobject();
			uuid.setType("uuid");
			uuid.setValue(id);
			ps.setObject(1, uuid);
			ps.setInt(2, EtlStatus.UNKNOWN);
			java.util.Date created = new java.util.Date();
			ps.setTimestamp(3, new Timestamp(created.getTime()));
			ps.setString(4, dataTable);
			log.info("Executing: " + ps);
			ps.execute();
			ps.close();

			ps = conn.prepareStatement("CREATE TABLE IF NOT EXISTS " + dataTable + " (rownr INTEGER PRIMARY KEY, data JSONB NOT NULL)");
			log.info("Executing: " + ps);
			ps.execute();
			ps.close();

			conn.commit();

			return new PgDataset(getRowStore(), id, EtlStatus.UNKNOWN, created, dataTable);
		} catch (SQLException e) {
			try {
				conn.rollback();
			} catch (SQLException e1) {
				SqlExceptionLogUtil.error(log, e1);
			}
			log.error(e.getMessage());
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					SqlExceptionLogUtil.error(log, e);
				}
			}
		}

		return null;
	}

	@Override
	public boolean purgeDataset(String id) {
		if (id == null) {
			throw new IllegalArgumentException("Dataset ID must not be null");
		}
		Connection conn = null;
		try {
			conn = getRowStore().getConnection();
			conn.setAutoCommit(false);

			PreparedStatement ps = conn.prepareStatement("DROP TABLE " + TABLE_NAME);
			log.info("Executing: " + ps);
			ps.execute();
			ps.close();

			ps = conn.prepareStatement("DELETE FROM " + TABLE_NAME + " WHERE id = ?");
			ps.setString(1, id);
			log.info("Executing: " + ps);
			ps.execute();
			ps.close();

			conn.commit();
			return true;
		} catch (SQLException e) {
			try {
				conn.rollback();
			} catch (SQLException e1) {
				SqlExceptionLogUtil.error(log, e1);
			}
			log.error(e.getMessage());
			return false;
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					SqlExceptionLogUtil.error(log, e);
				}
			}
		}
	}

	@Override
	public Dataset getDataset(String id) {
		if (id == null) {
			throw new IllegalArgumentException("Dataset ID must not be null");
		}
		return new PgDataset(rowstore, id);
	}

	private void createTableIfNotExists() {
		Connection conn = null;
		try {
			conn = getRowStore().getConnection();
			PreparedStatement ps = conn.prepareStatement("CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " (id UUID PRIMARY KEY, status INT NOT NULL, created TIMESTAMP NOT NULL, data_table CHAR(" + getDataTableNameLength() + "))");
			log.info("Executing: " + ps);
			ps.execute();
			ps.close();
		} catch (SQLException e) {
			SqlExceptionLogUtil.error(log, e);
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					SqlExceptionLogUtil.error(log, e);
				}
			}
		}
	}

	public PgRowStore getRowStore() {
		return this.rowstore;
	}

	@Override
	public int amount() {
		int result = -1;
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			conn = getRowStore().getConnection();
			stmt = conn.prepareStatement("SELECT COUNT(*) AS amount FROM " + TABLE_NAME);
			log.info("Executing: " + stmt);
			rs = stmt.executeQuery();
			while (rs.next()) {
				result = rs.getInt("amount");
			}
			rs.close();
		} catch (SQLException e) {
			SqlExceptionLogUtil.error(log, e);
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					SqlExceptionLogUtil.error(log, e);
				}
			}
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					SqlExceptionLogUtil.error(log, e);
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					SqlExceptionLogUtil.error(log, e);
				}
			}
		}

		return result;
	}

	private String constructDataTableName(String id) {
		return "data_" + id.replaceAll("-", "");
	}

	private int getDataTableNameLength() {
		return constructDataTableName(UUID.randomUUID().toString()).length();
	}

}