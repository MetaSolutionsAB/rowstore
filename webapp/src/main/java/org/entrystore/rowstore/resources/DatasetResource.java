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
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.restlet.Context;
import org.restlet.Request;
import org.restlet.Response;
import org.restlet.data.Status;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.ext.json.JsonpRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.Delete;
import org.restlet.resource.Get;

import java.util.Date;
import java.util.List;

/**
 * @author Hannes Ebner
 */
public class DatasetResource extends BaseResource {

	static Logger log = Logger.getLogger(DatasetResource.class);

	private Dataset dataset;

	@Override
	public void doInit() {
		String datasetId = (String) getRequest().getAttributes().get("id");
		if (datasetId != null) {
			dataset = getRowStore().getDatasets().getDataset(datasetId);
		}
	}

	@Get("application/json")
	public Representation represent() {
		if (dataset == null) {
			getResponse().setStatus(Status.CLIENT_ERROR_NOT_FOUND);
			return null;
		}

		// query parameters: /dataset/{id}?[{column-name}={value},{column-name={value}]

		JSONArray result = new JSONArray();
		Date before = new Date();

		List<JSONObject> qResult = dataset.query(parameters);

		long elapsedTime = new Date().getTime() - before.getTime();
		log.info("Query took " + elapsedTime + " ms");

		for (JSONObject row : qResult) {
			result.put(row);
		}

		getResponse().setStatus(Status.SUCCESS_OK);
		return new JsonRepresentation(result);
	}

	@Override
	public Representation head() {
		// TODO to be tested

		JSONObject result = new JSONObject();
		if (dataset != null) {
			try {
				result.put("status", dataset.getStatus());
				result.put("created", dataset.getCreationDate());
				result.put("columnnames", dataset.getColumnNames());
			} catch (JSONException e) {
				log.error(e.getMessage());
			}
		} else {
			setStatus(Status.CLIENT_ERROR_NOT_FOUND);
		}

		return new JsonRepresentation(result);
	}

	@Delete
	public void purgeDataset() {
		// TODO to be tested

		JSONObject result = new JSONObject();
		if (dataset != null) {
			boolean successful = getRowStore().getDatasets().purgeDataset(dataset.getId());
			if (successful) {
				setStatus(Status.SUCCESS_OK);
			} else {
				setStatus(Status.SERVER_ERROR_INTERNAL);
			}
		} else {
			setStatus(Status.CLIENT_ERROR_NOT_FOUND);
		}
	}

}