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
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.SQLException;

@Component
public class RowStoreHealthIndicator implements HealthIndicator {

	private final RowStore rowStore;

	public RowStoreHealthIndicator(RowStore rowStore) {
		this.rowStore = rowStore;
	}

	@Override
	public Health health() {
		try (Connection conn = rowStore.getConnection()) {
			if (conn.isValid(5)) {
				return Health.up()
						.withDetail("datasets", rowStore.getDatasets().amount())
						.withDetail("activeEtlProcesses", rowStore.getEtlProcessor().getActiveEtlProcesses())
						.build();
			}
		} catch (SQLException e) {
			return Health.down()
					.withException(e)
					.build();
		}
		return Health.down().build();
	}

}
