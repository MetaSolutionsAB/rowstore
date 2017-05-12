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

import org.apache.log4j.Logger;
import org.entrystore.rowstore.store.Dataset;
import org.json.JSONException;
import org.json.JSONObject;
import org.restlet.data.Status;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.Get;

import java.text.SimpleDateFormat;

/**
 * Returns information about a dataset.
 *
 * @author Hannes Ebner
 */
public class DatasetInfoResource extends BaseResource {

	static Logger log = Logger.getLogger(DatasetInfoResource.class);

	private Dataset dataset;

	@Override
	public void doInit() {
		String datasetId = (String) getRequest().getAttributes().get("id");
		if (datasetId != null) {
			dataset = getRowStore().getDatasets().getDataset(datasetId);
		}
	}

	@Get("json")
	public Representation represent() {
		if (dataset == null) {
			setStatus(Status.CLIENT_ERROR_NOT_FOUND);
			return null;
		}

		return new JsonRepresentation(constructInfo());
	}

	private JSONObject constructInfo() {
		JSONObject result = new JSONObject();
		try {
			result.put("status", dataset.getStatus());
			result.put("created", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").format(dataset.getCreationDate()));
			result.put("columnnames", dataset.getColumnNames());
			result.put("rowcount", dataset.getRowCount());
			result.put("identifier", dataset.getId());
			result.put("aliases", dataset.getAliases());

			// we add JSON-LD stuff
			String baseURL = getRowStore().getConfig().getBaseURL();
			baseURL += baseURL.endsWith("/") ? "" : "/";
			result.put("@context", "http://entrystore.org/rowstore/");
			result.put("@id", baseURL + "dataset/" + dataset.getId());
		} catch (JSONException e) {
			log.error(e.getMessage());
		}
		return result;
	}

}