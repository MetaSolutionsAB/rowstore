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
import org.entrystore.rowstore.etl.EtlStatus;
import org.entrystore.rowstore.store.Dataset;
import org.json.JSONArray;
import org.json.JSONObject;
import org.restlet.data.Status;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.Delete;
import org.restlet.resource.Get;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Supports to fetch, query and purge a dataset.
 *
 * @author Hannes Ebner
 */
public class DatasetResource extends BaseResource {

	static Logger log = Logger.getLogger(DatasetResource.class);

	private Dataset dataset;

	@Override
	public void doInit() {
		String datasetId = (String) getRequest().getAttributes().get("id");
		if (datasetId != null) {
			try {
				dataset = getRowStore().getDatasets().getDataset(datasetId);
			} catch (IllegalStateException e) {
				log.error(e.getMessage());
				dataset = null;
			}
		}
	}

	@Get("application/json")
	public Representation represent() {
		if (dataset == null) {
			getResponse().setStatus(Status.CLIENT_ERROR_NOT_FOUND);
			return null;
		}

		if (dataset.getStatus() == EtlStatus.ERROR) {
			getResponse().setStatus(Status.CLIENT_ERROR_FAILED_DEPENDENCY);
			return null;
		} else if (dataset.getStatus() != EtlStatus.AVAILABLE) {
			getResponse().setStatus(Status.CLIENT_ERROR_LOCKED);
			return null;
		}

		// query parameters: /dataset/{id}?[{column-name}={value},{column-name={value}]
		JSONArray result = new JSONArray();
		Date before = new Date();

		// we only pass on the parameters that match column names of the dataset's JSON
		Set<String> columns = dataset.getColumnNames();
		Map<String, String> tuples = new HashMap<>(parameters);
		tuples.keySet().retainAll(columns);

		// TODO support _limit=xx

		// TODO support _sort=First%20name,asc

		// TODO support _offset=xx

		List<JSONObject> qResult = dataset.query(tuples);

		long elapsedTime = new Date().getTime() - before.getTime();
		log.info("Query took " + elapsedTime + " ms");

		for (JSONObject row : qResult) {
			result.put(row);
		}

		getResponse().setStatus(Status.SUCCESS_OK);
		return new JsonRepresentation(result);
	}

	@Delete
	public void purgeDataset() {
		if (dataset == null) {
			setStatus(Status.CLIENT_ERROR_NOT_FOUND);
			return;
		}

		if (dataset.getStatus() != EtlStatus.AVAILABLE && dataset.getStatus() != EtlStatus.ERROR) {
			// the dataset is either waiting or currently being processed
			setStatus(Status.CLIENT_ERROR_LOCKED);
			return;
		}

		log.info("Purging dataset " + dataset.getId());
		boolean successful = getRowStore().getDatasets().purgeDataset(dataset.getId());
		if (successful) {
			log.info("Dataset " + dataset.getId() + " successfully purged");
			setStatus(Status.SUCCESS_OK);
		} else {
			log.error("An error occurred while purging dataset " + dataset.getId());
			setStatus(Status.SERVER_ERROR_INTERNAL);
		}
	}

}