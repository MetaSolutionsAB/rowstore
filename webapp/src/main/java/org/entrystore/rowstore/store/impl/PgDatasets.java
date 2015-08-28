package org.entrystore.rowstore.store.impl;

import org.entrystore.rowstore.etl.EtlStatus;
import org.entrystore.rowstore.store.Dataset;
import org.entrystore.rowstore.store.Datasets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Set;

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
			stmt = conn.prepareStatement("SELECT * FROM ?");
			stmt.setString(1, TABLE_NAME);
			rs = stmt.executeQuery();
			while (rs.next()) {
				String id = rs.getString("id");
				int status = rs.getInt("status");
				Timestamp created = rs.getTimestamp("created");
				result.add(new PgDataset(rowstore, id, status, created));
			}
			rs.close();
		} catch (SQLException e) {
			log.error(e.getMessage());
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					log.error(e.getMessage());
				}
			}
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					log.error(e.getMessage());
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					log.error(e.getMessage());
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
		String dataTable = "data_" + id.replaceAll("-", "");
		try {
			conn = getRowStore().getConnection();
			conn.setAutoCommit(false);

			PreparedStatement ps = conn.prepareStatement("INSERT INTO ? (id, status, created, data_table) VALUES (?, ?, ?, ?)");
			ps.setString(1, TABLE_NAME);
			ps.setString(2, id);
			ps.setInt(3, EtlStatus.UNKNOWN);
			java.util.Date created = new java.util.Date();
			ps.setTimestamp(4, new Timestamp(created.getTime()));
			ps.setString(5, dataTable);
			ps.execute();
			ps.close();

			ps = conn.prepareStatement("CREATE TABLE IF NOT EXISTS ? (row PRIMARY KEY, data JSONB NOT NULL)");
			ps.setString(1, dataTable);
			ps.execute();
			ps.close();

			conn.commit();

			return new PgDataset(getRowStore(), id, EtlStatus.UNKNOWN, created);
		} catch (SQLException e) {
			try {
				conn.rollback();
			} catch (SQLException e1) {
				log.error(e1.getMessage());
			}
			log.error(e.getMessage());
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					log.error(e.getMessage());
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

			PreparedStatement ps = conn.prepareStatement("DROP TABLE ?");
			ps.setString(1, id);
			ps.execute();
			ps.close();

			ps = conn.prepareStatement("DELETE FROM ? WHERE id = ?");
			ps.setString(1, TABLE_NAME);
			ps.setString(2, id);
			ps.execute();
			ps.close();

			conn.commit();
			return true;
		} catch (SQLException e) {
			try {
				conn.rollback();
			} catch (SQLException e1) {
				log.error(e1.getMessage());
			}
			log.error(e.getMessage());
			return false;
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					log.error(e.getMessage());
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
			PreparedStatement ps = conn.prepareStatement("CREATE TABLE IF NOT EXISTS ? (id UUID PRIMARY KEY, status INT NOT NULL, created TIMESTAMP NOT NULL, data_table VARCHAR(48))");
			ps.setString(1, TABLE_NAME);
			ps.execute();
			ps.close();
			conn.commit();
		} catch (SQLException e) {
			log.error(e.getMessage());
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					log.error(e.getMessage());
				}
			}
		}
	}

	public PgRowStore getRowStore() {
		return this.rowstore;
	}

}