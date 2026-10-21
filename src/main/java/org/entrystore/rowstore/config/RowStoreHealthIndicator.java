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

package org.entrystore.rowstore.config;

import org.entrystore.rowstore.store.RowStore;
import org.entrystore.rowstore.store.impl.LimitedDataSource;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Checks the primary database and, if configured separately, the query database, one after another (ARCH-7.04).
 * The checks wait only briefly for a free connection; if all connections are in use, the database is reported as
 * "BUSY" (it is in use, hence reachable) and the health is UP. Only a connectivity failure results in DOWN.
 */
@Component
public class RowStoreHealthIndicator implements HealthIndicator {

	private final RowStore rowStore;

	public RowStoreHealthIndicator(RowStore rowStore) {
		this.rowStore = rowStore;
	}

	@Override
	public Health health() {
		Map<String, String> databases = new LinkedHashMap<>();
		Exception failure = null;
		boolean separateQueryDatabase = rowStore.getConfig().getQueryDatabase() != rowStore.getConfig().getDatabase();
		for (boolean queryDatabase : separateQueryDatabase ? new boolean[]{false, true} : new boolean[]{false}) {
			String state;
			try {
				state = check(queryDatabase);
			} catch (SQLException | DataAccessException e) {
				state = "DOWN";
				failure = e;
			}
			databases.put(queryDatabase ? "queryDatabase" : "database", state);
		}
		Health.Builder health = databases.containsValue("DOWN") ? Health.down() : Health.up();
		if (failure != null) {
			health.withException(failure);
		} else {
			// read after the checks, a request must not hold more than one connection at a time; not read if all
			// connections are in use, so that the check does not wait again
			health.withDetail("datasets", databases.containsValue("BUSY") ? -1 : getDatasetCount());
		}
		return health
				.withDetail("activeEtlProcesses", rowStore.getEtlProcessor().getActiveEtlProcesses())
				.withDetail("databases", databases)
				.withDetail("connections", rowStore.getConnectionUsage())
				.build();
	}

	/**
	 * @return Returns "UP" if the database is reachable, "BUSY" if all connections are in use, or "DOWN".
	 */
	private String check(boolean queryDatabase) throws SQLException {
		try (Connection conn = rowStore.getProbeConnection(queryDatabase)) {
			return conn.isValid(5) ? "UP" : "DOWN";
		} catch (DataAccessException e) {
			if (LimitedDataSource.isWaitTimeout(e)) {
				return "BUSY";
			}
			throw e;
		}
	}

	private int getDatasetCount() {
		try {
			return rowStore.getDatasets().amount();
		} catch (DataAccessException e) {
			return -1;
		}
	}

}
