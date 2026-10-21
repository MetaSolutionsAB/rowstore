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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Cursor over the rows of a dataset as returned by {@link Dataset#streamAll()}. The connection is in a transaction
 * (required for cursor-based fetching) until the stream is closed.
 *
 * @param conn Connection the cursor belongs to.
 * @param stmt Statement of the query, may be null if the stream is closed before the statement was created.
 * @param rs Result set with the column "data", may be null if the stream is closed before the query was executed.
 */
public record RowStream(Connection conn, PreparedStatement stmt, ResultSet rs) implements AutoCloseable {

	private static final Logger log = LoggerFactory.getLogger(RowStream.class);

	/**
	 * Ends the transaction and closes result set, statement and connection. Errors are logged and not thrown, as
	 * they must not mask an error that occurred while consuming the stream. Not thread-safe: the stream must be
	 * closed by its single owner, and not while another thread is still reading from it.
	 */
	@Override
	public void close() {
		closeQuietly(rs);
		closeQuietly(stmt);
		try {
			// a pooled connection must not be returned with an open transaction
			// (the driver closes the connection itself if it was terminated by the database, and auto-commit
			// is still on if the stream failed before the transaction was started)
			if (!conn.isClosed() && !conn.getAutoCommit()) {
				conn.rollback();
			}
		} catch (SQLException e) {
			log.warn("Unable to roll back transaction: {}", e.getMessage());
		}
		closeQuietly(conn);
	}

	private static void closeQuietly(AutoCloseable closeable) {
		if (closeable == null) {
			return;
		}
		try {
			closeable.close();
		} catch (Exception e) {
			log.warn("Unable to close {}: {}", closeable.getClass().getSimpleName(), e.getMessage());
		}
	}

}
