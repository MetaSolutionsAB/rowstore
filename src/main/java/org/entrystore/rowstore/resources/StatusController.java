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

package org.entrystore.rowstore.resources;

import org.entrystore.rowstore.resources.model.JvmStatusResponse;
import org.entrystore.rowstore.resources.model.StatusResponse;
import org.entrystore.rowstore.store.RowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.lang.management.ManagementFactory;

@RestController
public class StatusController {

	private static final Logger log = LoggerFactory.getLogger(StatusController.class);

	private final RowStore rowStore;
	private final VersionInfo versionInfo;

	public StatusController(RowStore rowStore, VersionInfo versionInfo) {
		this.rowStore = rowStore;
		this.versionInfo = versionInfo;
	}

	@GetMapping(value = "/status", params = "jvm", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<JvmStatusResponse> getJvmStatus() {
		JvmStatusResponse body = new JvmStatusResponse(
				Runtime.getRuntime().totalMemory(),
				Runtime.getRuntime().freeMemory(),
				Runtime.getRuntime().maxMemory(),
				Runtime.getRuntime().availableProcessors(),
				getTotalCommittedMemory(),
				ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getCommitted(),
				getTotalUsedMemory(),
				ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed()
		);
		return ResponseEntity.ok(body);
	}

	@GetMapping(value = "/status", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<StatusResponse> getSimpleStatus() {
		StatusResponse body = new StatusResponse(
				"RowStore",
				versionInfo.getVersion(),
				getDatasetCount(),
				rowStore.getEtlProcessor().getActiveEtlProcesses(),
				rowStore.getConnectionUsage()
		);
		return ResponseEntity.ok(body);
	}

	/**
	 * @return Returns the number of datasets, or -1 if it could not be read, e.g., because all connections are in use
	 * (the status is reported anyway, ARCH-7.05).
	 */
	private int getDatasetCount() {
		try {
			return rowStore.getDatasets().amount();
		} catch (DataAccessException e) {
			// waits for connections are logged aggregated by the connection budget
			log.debug("Unable to count datasets for the status: {}", e.getMessage());
			return -1;
		}
	}

	long getTotalCommittedMemory() {
		return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getCommitted() +
				ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage().getCommitted();
	}

	long getTotalUsedMemory() {
		return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed() +
				ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage().getUsed();
	}

}
