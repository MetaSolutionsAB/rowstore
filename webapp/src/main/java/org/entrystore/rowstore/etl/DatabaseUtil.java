package org.entrystore.rowstore.etl;

import com.opencsv.CSVReader;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author Hannes Ebner
 */
public class DatabaseUtil {

	private static Logger log = LoggerFactory.getLogger(DatabaseUtil.class);

	public void insertJson(Connection conn, String table, JSONObject json) throws SQLException {
		if (table == null || table.length() == 0) {
			throw new IllegalArgumentException("Table name must neither be null nor empty.");
		}

		PreparedStatement stmt = null;
		try {
			String sql = "INSERT INTO ? (data) values (?)";
			stmt = conn.prepareStatement(sql);
			stmt.setString(1, table);
			stmt.setString(2, json.toString());
			stmt.executeUpdate();
		} finally {
			if (stmt != null) {
				stmt.close();
			}
		}
	}

	public boolean loadCsv(Connection conn, String table, File input) throws IOException, SQLException {
		if (conn == null || table == null || input == null) {
			throw new IllegalArgumentException("Arguments must not be null");
		}
		if (table.length() == 0) {
			throw new IllegalArgumentException("Table name must not be empty.");
		}

		CSVReader cr = null;
		try {
			cr = new CSVReader(new FileReader(input));
			long lineCount = 0;
			String[] labels = null;
			String[] line;
			conn.setAutoCommit(false);
			createTableIfNotExists(conn, table);
			while ((line = cr.readNext()) != null) {
				if (lineCount == 0) {
					labels = line;
				} else {
					try {
						JSONObject jsonLine = ConverterUtil.csvLineToJsonObject(line, labels);
						insertJson(conn, table, jsonLine);
					} catch (SQLException e) {
						log.error(e.getMessage());
						conn.rollback();
						return false;
					} catch (JSONException e) {
						log.error(e.getMessage());
						conn.rollback();
						return false;
					}
				}
				lineCount++;
			}
			conn.commit();
			return true;
		} finally {
			try {
				if (cr != null) {
					cr.close();
				}
			} catch (IOException e) {
				log.error(e.getMessage());
			}
		}
	}

	private void createTableIfNotExists(Connection conn, String table) throws SQLException {
		String sql = "CREATE TABLE IF NOT EXISTS ? (id SERIAL PRIMARY KEY, data JSONB NOT NULL);";
		PreparedStatement s = conn.prepareStatement(sql);
		s.setString(1, table);
		s.execute();
	}

}