/*
 * Copyright (c) 2014 MetaSolutions AB <info@metasolutions.se>
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
import org.restlet.engine.io.IoUtils;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.Get;
import org.restlet.resource.Post;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Set;
import java.util.UUID;

/**
 * @author Hannes Ebner
 */
public class DatasetsResource extends BaseResource {

	static Logger log = Logger.getLogger(DatasetsResource.class);

	@Get("application/json")
	public Representation represent() {
		JSONArray result = new JSONArray();
		Set<Dataset> datasets = getRowStore().getDatasets().getAll();
		for (Dataset ds : datasets) {
			result.put(ds.getId());
		}

		setStatus(Status.SUCCESS_OK);
		return new JsonRepresentation(result);
	}

	@Post("csv")
	public void acceptCSV(Representation entity) {
		File tmpFile = null;
		try {
			try {
				tmpFile = File.createTempFile(RowStoreApplication.NAME, "csv");
				tmpFile.deleteOnExit();
			} catch (IOException e) {
				log.error(e.getMessage());
				getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
				return;
			}

			if (tmpFile != null) {
				InputStream src = null;
				OutputStream dst = null;

				try {
					try {
						src = entity.getStream();
					} catch (IOException ioe) {
						log.error(ioe.getMessage());
						getResponse().setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
						return;
					}

					try {
						dst = new FileOutputStream(tmpFile);
					} catch (IOException ioe) {
						log.error(ioe.getMessage());
						getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
						return;
					}

					try {
						IoUtils.copy(src, dst);
					} catch (IOException ioe) {
						log.error(ioe.getMessage());
						getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
						return;
					}
				} finally {
					if (src != null) {
						try {
							src.close();
						} catch (IOException e) {
							log.error(e.getMessage());
						}
					}
					if (dst != null) {
						try {
							dst.close();
						} catch (IOException e) {
							log.error(e.getMessage());
						}
					}
				}
			}

			String uuid = UUID.randomUUID().toString();

			Dataset newDataset = getRowStore().getDatasets().createDataset(uuid);
			newDataset.setStatus(EtlStatus.ACCEPTED);
			EtlResource etlResource = new EtlResource(newDataset, tmpFile, MediaType.TEXT_CSV);
			getRowStore().getEtlProcessor().submit(etlResource);

			String datasetURL = buildDatasetURL(getRowStore().getConfig().getBaseURL(), uuid);

			JSONObject result = new JSONObject();
			try {
				result.put("id", uuid);
				result.put("url", datasetURL);
				result.put("status", EtlStatus.ACCEPTED);
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
		result.append(datasetId);
		return result.toString();
	}

}