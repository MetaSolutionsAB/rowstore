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

import org.entrystore.rowstore.RowStoreApplication;
import org.json.JSONException;
import org.json.JSONObject;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.Get;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;

/**
 * Returns status about the RowStore instance.
 *
 * @author Hannes Ebner
 */
public class StatusResource extends BaseResource {

	private final static Logger log = LoggerFactory.getLogger(StatusResource.class);

	@Get("json")
	public Representation getSimpleStatus() throws JSONException {
		JSONObject result = new JSONObject();
		result.put("service", "RowStore");
		result.put("version", RowStoreApplication.getVersion());
		result.put("datasets", getRowStore().getDatasets().amount());
		result.put("activeEtlProcesses", getRowStore().getEtlProcessor().getActiveEtlProcesses());
		return new JsonRepresentation(result);
	}

	@Get("json?jvm")
	public Representation getJvmStatus() throws JSONException {
		JSONObject jvm = new JSONObject();
		jvm.put("totalMemory", Runtime.getRuntime().totalMemory());
		jvm.put("freeMemory", Runtime.getRuntime().freeMemory());
		jvm.put("maxMemory", Runtime.getRuntime().maxMemory());
		jvm.put("availableProcessors", Runtime.getRuntime().availableProcessors());
		jvm.put("totalCommittedMemory", getTotalCommittedMemory());
		jvm.put("committedHeap", ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getCommitted());
		jvm.put("totalUsedMemory", getTotalUsedMemory());
		jvm.put("usedHeap", ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed());
		return new JsonRepresentation(jvm);
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