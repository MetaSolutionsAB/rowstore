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
import org.entrystore.rowstore.etl.EtlResource;
import org.entrystore.rowstore.etl.EtlStatus;
import org.entrystore.rowstore.store.Dataset;
import org.entrystore.rowstore.store.QueryResult;
import org.entrystore.rowstore.util.DatasetUtil;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.restlet.data.MediaType;
import org.restlet.data.Method;
import org.restlet.data.Status;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Delete;
import org.restlet.resource.Get;
import org.restlet.resource.Post;
import org.restlet.resource.Put;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
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

	private String datasetId;

	@Override
	public void doInit() {
		datasetId = (String) getRequest().getAttributes().get("id");
		if (datasetId != null) {
			try {
				dataset = getRowStore().getDatasets().getDataset(datasetId);
			} catch (IllegalStateException e) {
				log.error(e.getMessage());
				dataset = null;
			}
		}
	}

	@Get("html")
	public Representation representHtml() {
		if (dataset == null) {
			getResponse().setStatus(Status.CLIENT_ERROR_NOT_FOUND);
			return null;
		}

		String redir = getRowStore().getConfig().getBaseURL();
		redir += redir.endsWith("/") ? "" : "/";
		redir += "dataset/" + datasetId + "/html";
		getResponse().redirectSeeOther(redir);
		return null;
	}

	@Get("json")
	public Representation representJson() {
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

		// We only pass on the parameters that match column names of the dataset's JSON
		// We also skip parameters _limit, _offset and _sort as they are needed for advanced functionality
		Set<String> columns = dataset.getColumnNames();
		Map<String, String> tuples = new HashMap<>();
		int specialParamCount = 0;
		for (String k : parameters.keySet()) {
			if ("_limit".equals(k) || "_offset".equals(k) || "_sort".equals(k)) {
				specialParamCount++;
				continue;
			}
			tuples.put(k.toLowerCase(), parameters.get(k));
		}
		tuples.keySet().retainAll(columns);

		if (parameters.size() > specialParamCount && (parameters.size()-specialParamCount) != tuples.size()) {
			// One or more query parameters did not match
			// the column names, so we return an error
			getResponse().setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
			return null;
		}

		int limit = 100;
		if (parameters.containsKey("_limit")) {
			try {
				int paramLimit = Integer.valueOf(parameters.get("_limit"));
				if (paramLimit <= limit && paramLimit > 0) {
					limit = paramLimit;
				}
			} catch (NumberFormatException nfe) {
				getResponse().setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
				return null;
			}
		}

		int offset = 0;
		if (parameters.containsKey("_offset")) {
			try {
				int paramOffset = Integer.valueOf(parameters.get("_offset"));
				if (paramOffset > offset) {
					offset = paramOffset;
				}
			} catch (NumberFormatException nfe) {
				getResponse().setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
				return null;
			}
		}

		JSONArray rows = new JSONArray();
		Date before = new Date();

		// TODO support _sort=First%20name,asc

		QueryResult qResult = dataset.query(tuples, limit, offset);

		long elapsedTime = new Date().getTime() - before.getTime();
		log.info("Query took " + elapsedTime + " ms");

		if (qResult.getStatus() != null) {
			if ("57014".equals(qResult.getStatus())) {
				log.debug("Query timed out");
				getResponse().setStatus(Status.SERVER_ERROR_SERVICE_UNAVAILABLE);
				return new StringRepresentation("The submitted query exceeded the configured maximum time limit.");
			}

			getResponse().setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
			return null;
		}

		for (JSONObject row : qResult.getResults()) {
			rows.put(row);
		}

		JSONObject result = new JSONObject();
		result.put("results", rows);
		result.put("limit", qResult.getLimit());
		result.put("offset", qResult.getOffset());
		result.put("resultCount", qResult.getResultCount());

		getResponse().setStatus(Status.SUCCESS_OK);
		return new JsonRepresentation(result);
	}

	@Post("csv")
	@Put("csv")
	public void acceptCSV(Representation entity) {
		if (dataset == null) {
			getResponse().setStatus(Status.CLIENT_ERROR_NOT_FOUND);
			return;
		}

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

			boolean appendData = Method.POST.equals(getRequest().getMethod());

			if (dataset.getStatus() != EtlStatus.PROCESSING) {
				dataset.setStatus(EtlStatus.ACCEPTED_DATA);
			}
			EtlResource etlResource = new EtlResource(dataset, tmpFile, MediaType.TEXT_CSV, appendData);
			getRowStore().getEtlProcessor().submit(etlResource);

			String datasetURL = DatasetUtil.buildDatasetURL(getRowStore().getConfig().getBaseURL(), dataset.getId());

			JSONObject result = new JSONObject();
			try {
				result.put("id", dataset.getId());
				result.put("url", datasetURL);
				result.put("status", dataset.getStatus());
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
				log.info("Deleting temporary file " + tmpFile);
				tmpFile.delete();
			}
		}
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