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
public class CsvToPgLoader {

	private static Logger log = LoggerFactory.getLogger(CsvToPgLoader.class);

	public boolean loadCsv(Connection conn, String table, File input) throws IOException, SQLException {
		if (conn == null || table == null || input == null) {
			throw new IllegalArgumentException("Arguments must not be null");
		}
		if (table.length() == 0) {
			throw new IllegalArgumentException("Table name must not be empty");
		}

		CSVReader cr = null;
		try {
			cr = new CSVReader(new FileReader(input));
			long lineCount = 0;
			String[] labels = null;
			String[] line;
			conn.setAutoCommit(false);
			createTableIfNotExists(conn, table);
			PreparedStatement stmt = conn.prepareStatement("INSERT INTO ? (id, data) VALUES (?, ?)");
			while ((line = cr.readNext()) != null) {
				if (lineCount == 0) {
					labels = line;
				} else {
					try {
						JSONObject jsonLine = ConverterUtil.csvLineToJsonObject(line, labels);
						stmt.setString(1, table);
						stmt.setString(2, jsonLine.toString());
						stmt.addBatch();
						// we execute the batch every 100th line
						if ((lineCount % 100) == 0) {
							stmt.executeBatch();
						}
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
			// in case there are some inserts left to be sent (i.e.
			// batch size above was smaller than 100 when loop ended)
			stmt.executeBatch();

			// we commit the transaction and free the resources of the statement
			conn.commit();
			stmt.close();
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
		if (conn == null || table == null) {
			throw new IllegalArgumentException("Arguments must not be null");
		}
		if (table.length() == 0) {
			throw new IllegalArgumentException("Table name must not be empty");
		}
		PreparedStatement ps = conn.prepareStatement("CREATE TABLE IF NOT EXISTS ? (id PRIMARY KEY, data JSONB NOT NULL)");
		ps.setString(1, table);
		ps.execute();
		ps.close();
	}

}