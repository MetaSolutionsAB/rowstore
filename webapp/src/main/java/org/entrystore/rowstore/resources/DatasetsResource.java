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

import org.entrystore.rowstore.etl.EtlResource;
import org.entrystore.rowstore.etl.EtlStatus;
import org.entrystore.rowstore.store.Dataset;
import org.entrystore.rowstore.util.DatasetUtil;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.Get;
import org.restlet.resource.Post;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Set;

/**
 * Supports listing of all existing datasets (GET) and the creation of new datasets (POST).
 *
 * @author Hannes Ebner
 */
public class DatasetsResource extends BaseResource {

	static Logger log = LoggerFactory.getLogger(DatasetsResource.class);

	@Get("application/json")
	public Representation represent() {
		JSONArray result = new JSONArray();
		Set<Dataset> datasets = getRowStore().getDatasets().getAll();
		if (datasets != null) {
			for (Dataset ds : datasets) {
				result.put(ds.getId());
			}
			setStatus(Status.SUCCESS_OK);
			return new JsonRepresentation(result);
		}

		setStatus(Status.SERVER_ERROR_INTERNAL);
		return null;
	}

	@Post("csv")
	public void acceptCSV(Representation entity) {
		if (entity == null) {
			getResponse().setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
			return;
		}

		File tmpFile = null;
		try {
			try {
				tmpFile = DatasetUtil.writeTempFile(entity);
			} catch (IOException ioe) {
				log.error(ioe.getMessage());
				getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
				return;
			}

			Dataset newDataset = getRowStore().getDatasets().createDataset();
			if (newDataset == null) {
				getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
				return;
			}
			newDataset.setStatus(EtlStatus.ACCEPTED_DATA);
			EtlResource etlResource = new EtlResource(newDataset, tmpFile, MediaType.TEXT_CSV, true);
			getRowStore().getEtlProcessor().submit(etlResource);

			String datasetURL = DatasetUtil.buildDatasetURL(getRowStore().getConfig().getBaseURL(), newDataset.getId());

			JSONObject result = new JSONObject();
			try {
				result.put("id", newDataset.getId());
				result.put("url", datasetURL);
				result.put("status", EtlStatus.ACCEPTED_DATA);
				result.put("info", datasetURL + "/info");
			} catch (JSONException e) {
				log.error(e.getMessage());
			}

			getResponse().setLocationRef(datasetURL);
			getResponse().setEntity(new JsonRepresentation(result));
			getResponse().setStatus(Status.SUCCESS_ACCEPTED);
		} finally {
			// Delete the temporary file if something has gone wrong
			// and the conversion process does not continue
			if (tmpFile != null && !Status.SUCCESS_ACCEPTED.equals(getResponse().getStatus())) {
				log.info("Deleting temporary file " + tmpFile);
				tmpFile.delete();
			}
		}
	}

}