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

package org.entrystore.rowstore.store.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Records one-time maintenance tasks that RowStore runs at startup, in the table {@code maintenance_tasks} (DOM-4.04),
 * so that a task whose run has completed is skipped at later startups (e.g., the cleanup of column names, ETL-11.07).
 * A task is identified by its name; deleting its row makes it run again.
 */
public class MaintenanceTasks {

	private static final Logger log = LoggerFactory.getLogger(MaintenanceTasks.class);

	private static final String TABLE_NAME = "maintenance_tasks";

	private final JdbcTemplate jdbcTemplate;

	public MaintenanceTasks(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	/**
	 * @return Returns whether a run of the task has completed. If this cannot be determined, e.g., because the database
	 * is not reachable, false is returned, so that the task runs (and is expected to stop by itself if it fails).
	 */
	public boolean isCompleted(String name) {
		try {
			Boolean completed = jdbcTemplate.queryForObject(
					"SELECT EXISTS (SELECT 1 FROM " + TABLE_NAME + " WHERE name = ?)", Boolean.class, name);
			return Boolean.TRUE.equals(completed);
		} catch (DataAccessException e) {
			log.warn("Unable to determine whether maintenance task {} has completed: {}", name, e.getMessage());
			return false;
		}
	}

	/**
	 * Records that a run of the task has completed. Failures are logged; the task then runs again at the next startup.
	 */
	public void markCompleted(String name) {
		try {
			jdbcTemplate.update("INSERT INTO " + TABLE_NAME + " (name, completed) VALUES (?, now()) ON CONFLICT (name) DO NOTHING", name);
			log.info("Maintenance task {} completed, it is skipped at later startups", name);
		} catch (DataAccessException e) {
			log.warn("Unable to record the completion of maintenance task {}: {}", name, e.getMessage());
		}
	}

}
