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
import org.entrystore.rowstore.RowStoreApplication;
import org.entrystore.rowstore.etl.EtlResource;
import org.entrystore.rowstore.etl.EtlStatus;
import org.entrystore.rowstore.store.Dataset;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.Get;
import org.restlet.resource.Post;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

/**
 * @author Hannes Ebner
 */
public class DatasetsResource extends BaseResource {

	static Logger log = Logger.getLogger(DatasetsResource.class);

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
		File tmpFile = null;
		try {
			try {
				tmpFile = File.createTempFile(RowStoreApplication.NAME, ".csv");
				tmpFile.deleteOnExit();
			} catch (IOException e) {
				log.error(e.getMessage());
				getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
				return;
			}

			if (tmpFile != null) {
				try {
					writeFile(entity.getStream(), tmpFile);
				} catch (IOException ioe) {
					log.error(ioe.getMessage());
					getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
					return;
				}
			}

			Dataset newDataset = getRowStore().getDatasets().createDataset();
			newDataset.setStatus(EtlStatus.ACCEPTED_DATA);
			EtlResource etlResource = new EtlResource(newDataset, tmpFile, MediaType.TEXT_CSV);
			getRowStore().getEtlProcessor().submit(etlResource);

			String datasetURL = buildDatasetURL(getRowStore().getConfig().getBaseURL(), newDataset.getId());

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
			// we delete the temporary file if something has gone wrong
			// and the conversion process does not continue
			if (tmpFile != null && !Status.SUCCESS_ACCEPTED.equals(getResponse().getStatus())) {
				tmpFile.delete();
			}
		}
	}

	private String buildDatasetURL(String baseURL, String datasetId) {
		if (baseURL == null || datasetId == null) {
			throw new IllegalArgumentException("Arguments must not be null");
		}
		StringBuilder result = new StringBuilder(baseURL);
		if (!baseURL.endsWith("/")) {
			result.append("/");
		}
		result.append("dataset/");
		result.append(datasetId);
		return result.toString();
	}

	private void writeFile(InputStream src, File dst) throws IOException {
		if (src == null || dst == null) {
			throw new IllegalArgumentException("Parameters must not be null");
		}

		byte[] buffer = new byte[4096];
		FileOutputStream fos = null;
		try {
			fos = new FileOutputStream(dst);
			for (int length = 0; (length = src.read(buffer)) > 0; ) {
				fos.write(buffer, 0, length);
			}
		} finally {
			if (src != null) {
				try {
					src.close();
				} catch (IOException e) {
					log.error(e.getMessage());
				}
			}
			if (fos != null) {
				try {
					fos.close();
				} catch (IOException e) {
					log.error(e.getMessage());
				}
			}
		}
	}

}