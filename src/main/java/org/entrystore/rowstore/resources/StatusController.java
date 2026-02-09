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

package org.entrystore.rowstore.resources;

import org.entrystore.rowstore.store.RowStore;
import org.json.JSONObject;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.lang.management.ManagementFactory;

@RestController
public class StatusController {

	private final RowStore rowStore;
	private final VersionInfo versionInfo;

	public StatusController(RowStore rowStore, VersionInfo versionInfo) {
		this.rowStore = rowStore;
		this.versionInfo = versionInfo;
	}

	@GetMapping(value = "/status", params = "jvm", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> getJvmStatus() {
		JSONObject jvm = new JSONObject();
		jvm.put("totalMemory", Runtime.getRuntime().totalMemory());
		jvm.put("freeMemory", Runtime.getRuntime().freeMemory());
		jvm.put("maxMemory", Runtime.getRuntime().maxMemory());
		jvm.put("availableProcessors", Runtime.getRuntime().availableProcessors());
		jvm.put("totalCommittedMemory", getTotalCommittedMemory());
		jvm.put("committedHeap", ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getCommitted());
		jvm.put("totalUsedMemory", getTotalUsedMemory());
		jvm.put("usedHeap", ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed());
		return ResponseEntity.ok(jvm.toString());
	}

	@GetMapping(value = "/status", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> getSimpleStatus() {
		JSONObject result = new JSONObject();
		result.put("service", "RowStore");
		result.put("version", versionInfo.getVersion());
		result.put("datasets", rowStore.getDatasets().amount());
		result.put("activeEtlProcesses", rowStore.getEtlProcessor().getActiveEtlProcesses());
		return ResponseEntity.ok(result.toString());
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
