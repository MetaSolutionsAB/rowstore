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
 * A PostgreSQL-specific implementation of the Datasets interface.
 *
 * @author Hannes Ebner
 * @see Datasets
 */
public class PgDatasets implements Datasets {

	private static Logger log = LoggerFactory.getLogger(PgDatasets.class);

	protected static String DATASETS_TABLE_NAME = "datasets";

	protected static String ALIAS_TABLE_NAME = "aliases";

	PgRowStore rowstore;

	protected PgDatasets(PgRowStore rowstore) {
		this.rowstore = rowstore;
		createDatasetTableIfNotExists();
		createAliasTableIfNotExists();
	}

	/**
	 * @see Datasets#getAll()
	 */
	@Override
	public Set<Dataset> getAll() {
		Set<Dataset> result = null;
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			conn = getRowStore().getConnection();
			stmt = conn.prepareStatement("SELECT * FROM " + DATASETS_TABLE_NAME);
			log.debug("Executing: " + stmt);
			rs = stmt.executeQuery();
			result = new HashSet<>();
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

	/**
	 * @see Datasets#createDataset()
	 */
	@Override
	public Dataset createDataset() {
		String id = createUniqueDatasetId();
		Connection conn = null;
		String dataTable = constructDataTableName(id);
		try {
			conn = getRowStore().getConnection();
			conn.setAutoCommit(false);

			PreparedStatement ps = conn.prepareStatement("INSERT INTO " + DATASETS_TABLE_NAME + " (id, status, created, data_table) VALUES (?, ?, ?, ?)");
			PGobject uuid = new PGobject();
			uuid.setType("uuid");
			uuid.setValue(id);
			ps.setObject(1, uuid);
			ps.setInt(2, EtlStatus.CREATED);
			java.util.Date created = new java.util.Date();
			ps.setTimestamp(3, new Timestamp(created.getTime()));
			ps.setString(4, dataTable);
			log.debug("Executing: " + ps);
			ps.execute();
			ps.close();

			ps = conn.prepareStatement("CREATE TABLE IF NOT EXISTS " + dataTable + " (rownr SERIAL, data JSONB NOT NULL)");
			log.debug("Executing: " + ps);
			ps.execute();
			ps.close();

			conn.commit();
			log.info("Created dataset " + id);
			return new PgDataset(getRowStore(), id, EtlStatus.CREATED, created, dataTable);
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

	/**
	 * @see Datasets#purgeDataset(String)
	 */
	@Override
	public boolean purgeDataset(String id) {
		if (id == null) {
			throw new IllegalArgumentException("Dataset ID must not be null");
		}
		Connection conn = null;
		try {
			conn = getRowStore().getConnection();
			conn.setAutoCommit(false);

			PreparedStatement ps = conn.prepareStatement("DROP TABLE " + constructDataTableName(id));
			log.debug("Executing: " + ps);
			ps.execute();
			ps.close();

			ps = conn.prepareStatement("DELETE FROM " + DATASETS_TABLE_NAME + " WHERE id = ?");
			PGobject uuid = new PGobject();
			uuid.setType("uuid");
			uuid.setValue(id);
			ps.setObject(1, uuid);
			log.debug("Executing: " + ps);
			ps.execute();
			ps.close();

			conn.commit();
			log.info("Purged dataset " + id);
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

	/**
	 * @see Datasets#getDataset(String)
	 */
	@Override
	public Dataset getDataset(String id) {
		if (id == null) {
			throw new IllegalArgumentException("Dataset ID must not be null");
		}
		try {
			return new PgDataset(rowstore, id);
		} catch (IllegalArgumentException iae) {
			return null;
		}
	}

	/**
	 * @see Datasets#hasDataset(String)
	 */
	@Override
	public boolean hasDataset(String id) {
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			conn = rowstore.getConnection();
			stmt = conn.prepareStatement("SELECT * FROM " + PgDatasets.DATASETS_TABLE_NAME + " WHERE id = ?");
			PGobject uuid = new PGobject();
			uuid.setType("uuid");
			uuid.setValue(id);
			stmt.setObject(1, uuid);
			log.debug("Executing: " + stmt);
			rs = stmt.executeQuery();
			if (rs.next()) {
				return true;
			}
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

		return false;
	}

	/**
	 * Makes sure the table for keeping track of datasets exists.
	 */
	private void createDatasetTableIfNotExists() {
		Connection conn = null;
		try {
			conn = getRowStore().getConnection();
			PreparedStatement ps = conn.prepareStatement("CREATE TABLE IF NOT EXISTS " + DATASETS_TABLE_NAME + " (id UUID PRIMARY KEY, status INT NOT NULL, created TIMESTAMP NOT NULL, data_table CHAR(" + getDataTableNameLength() + "))");
			log.debug("Executing: " + ps);
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

	/**
	 * Makes sure the table for keeping track of datasets exists.
	 */
	private void createAliasTableIfNotExists() {
		Connection conn = null;
		try {
			conn = getRowStore().getConnection();
			PreparedStatement ps = conn.prepareStatement("CREATE TABLE IF NOT EXISTS " + ALIAS_TABLE_NAME + " (id SERIAL, dataset_id UUID NOT NULL, alias TEXT NOT NULL)");
			log.debug("Executing: " + ps);
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

	/**
	 * @return Returns the RowStore instance.
	 */
	public PgRowStore getRowStore() {
		return this.rowstore;
	}

	/**
	 * @see Datasets#amount()
	 */
	@Override
	public int amount() {
		int result = -1;
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			conn = getRowStore().getConnection();
			stmt = conn.prepareStatement("SELECT COUNT(*) AS amount FROM " + DATASETS_TABLE_NAME);
			log.debug("Executing: " + stmt);
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

	/**
	 * @see Datasets#createUniqueDatasetId()
	 */
	@Override
	public String createUniqueDatasetId() {
		String uuid;
		do {
			uuid = UUID.randomUUID().toString();
		} while (getRowStore().getDatasets().hasDataset(uuid));
		return uuid;
	}

	/**
	 * Constructs a name for a DB data table based on a supplied ID.
	 * @param id The ID to be used for constructing the table name.
	 * @return Returns a table name for storing a dataset's data.
	 */
	private String constructDataTableName(String id) {
		return "data_" + id.replaceAll("-", "");
	}

	/**
	 * @return Returns the length of the data table names.
	 */
	private int getDataTableNameLength() {
		return constructDataTableName(UUID.randomUUID().toString()).length();
	}

}