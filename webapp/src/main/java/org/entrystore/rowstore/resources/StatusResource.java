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

/**
 * Returns status about the RowStore instance.
 *
 * @author Hannes Ebner
 */
public class StatusResource extends BaseResource {

	private static Logger log = LoggerFactory.getLogger(StatusResource.class);

	@Get("json")
	public Representation getJSON() throws JSONException {
		JSONObject result = new JSONObject();
		result.put("service", "RowStore");
		result.put("version", RowStoreApplication.getVersion());
		result.put("datasets", getRowStore().getDatasets().amount());
		result.put("activeEtlProcesses", getRowStore().getEtlProcessor().getActiveEtlProcesses());
		return new JsonRepresentation(result);
	}

}