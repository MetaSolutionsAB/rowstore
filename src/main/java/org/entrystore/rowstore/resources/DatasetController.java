/*
 * Copyright (c) 2011-2026 MetaSolutions AB <info@metasolutions.se>
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

import jakarta.servlet.http.HttpServletRequest;
import org.entrystore.rowstore.etl.EtlResource;
import org.entrystore.rowstore.etl.EtlStatus;
import org.entrystore.rowstore.store.Dataset;
import org.entrystore.rowstore.store.QueryResult;
import org.entrystore.rowstore.store.RowStore;
import org.entrystore.rowstore.util.DatasetUtil;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@RestController
public class DatasetController {

	private static final Logger log = LoggerFactory.getLogger(DatasetController.class);

	private final RowStore rowStore;

	public DatasetController(RowStore rowStore) {
		this.rowStore = rowStore;
	}

	@GetMapping(value = "/dataset/{id}", produces = MediaType.TEXT_HTML_VALUE)
	public ResponseEntity<Void> representHtml(@PathVariable("id") String id) {
		Dataset dataset = rowStore.getDatasets().getDataset(id);
		if (dataset == null) {
			return ResponseEntity.notFound().build();
		}

		String redir = rowStore.getConfig().getBaseURL();
		redir += redir.endsWith("/") ? "" : "/";
		redir += "dataset/" + id + "/html";
		return ResponseEntity.status(HttpStatus.SEE_OTHER)
				.header(HttpHeaders.LOCATION, redir)
				.build();
	}

	@GetMapping(value = "/dataset/{id}/json", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> representJsonExplicit(@PathVariable("id") String id,
			@RequestParam Map<String, String> allParams) {
		return doQuery(id, allParams);
	}

	@GetMapping(value = "/dataset/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> representJson(@PathVariable("id") String id,
			@RequestParam Map<String, String> allParams) {
		return doQuery(id, allParams);
	}

	private ResponseEntity<String> doQuery(String id, Map<String, String> parameters) {
		Dataset dataset = rowStore.getDatasets().getDataset(id);
		if (dataset == null) {
			return ResponseEntity.notFound().build();
		}

		if (dataset.getStatus() == EtlStatus.CREATED) {
			return ResponseEntity.status(HttpStatus.FAILED_DEPENDENCY).build();
		}

		Set<String> columns = dataset.getColumnNames();
		Map<String, String> tuples = new HashMap<>();
		int specialParamCount = 0;
		for (String k : parameters.keySet()) {
			if ("_limit".equals(k) || "_offset".equals(k) || "_sort".equals(k)) {
				specialParamCount++;
				continue;
			}
			String value = parameters.get(k);
			if (value != null && value.isEmpty()) {
				return ResponseEntity.badRequest().build();
			}
			tuples.put(k.toLowerCase(), value);
		}
		tuples.keySet().retainAll(columns);

		if (parameters.size() > specialParamCount && (parameters.size() - specialParamCount) != tuples.size()) {
			return ResponseEntity.badRequest().build();
		}

		int limit = parseLimit(parameters);
		if (limit < 0) {
			return ResponseEntity.badRequest().build();
		}

		int offset = parseOffset(parameters);
		if (offset < 0) {
			return ResponseEntity.badRequest().build();
		}

		long elapsedTime = System.currentTimeMillis();
		QueryResult qResult = dataset.query(tuples, limit, offset);
		elapsedTime = System.currentTimeMillis() - elapsedTime;
		log.debug("Request took {} ms to process", elapsedTime);

		if (qResult.getStatus() != null) {
			if ("57014".equals(qResult.getStatus())) {
				log.debug("Query timed out");
				return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
						.body("The submitted query exceeded the configured maximum time limit.");
			}
			return ResponseEntity.badRequest().build();
		}

		JSONArray rows = new JSONArray();
		for (JSONObject row : qResult.getResults()) {
			rows.put(row);
		}

		JSONObject result = new JSONObject();
		result.put("results", rows);
		result.put("limit", qResult.getLimit());
		result.put("offset", qResult.getOffset());
		result.put("resultCount", qResult.getResultCount());
		result.put("queryTime", qResult.getQueryTime());

		if ((qResult.getOffset() - qResult.getLimit()) >= 0) {
			result.put("prev", constructPageUrl(dataset, qResult.getOffset() - qResult.getLimit(), qResult, parameters));
		}

		if (qResult.getResultCount() >= (qResult.getLimit() + qResult.getOffset())) {
			result.put("next", constructPageUrl(dataset, qResult.getOffset() + qResult.getLimit(), qResult, parameters));
		}

		return ResponseEntity.ok(result.toString());
	}

	@PostMapping(value = "/dataset/{id}", consumes = "text/csv")
	public ResponseEntity<String> acceptCSVPost(@PathVariable("id") String id, HttpServletRequest request) {
		return acceptCSV(id, request, true);
	}

	@PutMapping(value = "/dataset/{id}", consumes = "text/csv")
	public ResponseEntity<String> acceptCSVPut(@PathVariable("id") String id, HttpServletRequest request) {
		return acceptCSV(id, request, false);
	}

	private ResponseEntity<String> acceptCSV(String id, HttpServletRequest request, boolean append) {
		Dataset dataset = rowStore.getDatasets().getDataset(id);
		if (dataset == null) {
			return ResponseEntity.notFound().build();
		}

		if (request.getContentLength() == 0) {
			return ResponseEntity.badRequest().build();
		}

		File tmpFile = null;
		boolean accepted = false;
		try {
			try {
				tmpFile = DatasetUtil.writeTempFile(request.getInputStream(), rowStore.getConfig().getMaxUploadSize());
			} catch (DatasetUtil.PayloadTooLargeException ptle) {
				log.warn(ptle.getMessage());
				return ResponseEntity.status(HttpStatus.PAYLOAD_TOO_LARGE).build();
			} catch (IOException ioe) {
				log.error(ioe.getMessage());
				return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
			}

			if (dataset.getStatus() != EtlStatus.PROCESSING) {
				dataset.setStatus(EtlStatus.ACCEPTED_DATA);
			}
			EtlResource etlResource = new EtlResource(dataset, tmpFile, "text/csv", append);
			rowStore.getEtlProcessor().submit(etlResource);

			String datasetURL = DatasetUtil.buildDatasetURL(rowStore.getConfig().getBaseURL(), dataset.getId());

			JSONObject result = new JSONObject();
			result.put("id", dataset.getId());
			result.put("url", datasetURL);
			result.put("status", dataset.getStatus());
			result.put("info", datasetURL + "/info");

			accepted = true;
			HttpHeaders headers = new HttpHeaders();
			headers.add(HttpHeaders.LOCATION, datasetURL);
			headers.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);
			return new ResponseEntity<>(result.toString(), headers, HttpStatus.ACCEPTED);
		} finally {
			if (tmpFile != null && !accepted) {
				log.info("Deleting temporary file " + tmpFile);
				tmpFile.delete();
			}
		}
	}

	@DeleteMapping(value = "/dataset/{id}")
	public ResponseEntity<Void> purgeDataset(@PathVariable("id") String id) {
		Dataset dataset = rowStore.getDatasets().getDataset(id);
		if (dataset == null) {
			return ResponseEntity.notFound().build();
		}

		if (dataset.getStatus() != EtlStatus.AVAILABLE && dataset.getStatus() != EtlStatus.ERROR) {
			return ResponseEntity.status(HttpStatus.LOCKED).build();
		}

		log.info("Purging dataset " + dataset.getId());
		boolean successful = rowStore.getDatasets().purgeDataset(dataset.getId());
		if (successful) {
			log.info("Dataset " + dataset.getId() + " successfully purged");
			return ResponseEntity.noContent().build();
		} else {
			log.error("An error occurred while purging dataset " + dataset.getId());
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
		}
	}

	private int parseLimit(Map<String, String> parameters) {
		if (!parameters.containsKey("_limit")) {
			return 100;
		}
		try {
			int paramLimit = Integer.parseInt(parameters.get("_limit"));
			int maxLimit = rowStore.getConfig().getQueryMaxLimit();
			if (paramLimit > 0 && paramLimit <= maxLimit) {
				return paramLimit;
			}
			return 100;
		} catch (NumberFormatException e) {
			return -1;
		}
	}

	private int parseOffset(Map<String, String> parameters) {
		if (!parameters.containsKey("_offset")) {
			return 0;
		}
		try {
			int paramOffset = Integer.parseInt(parameters.get("_offset"));
			return paramOffset > 0 ? paramOffset : 0;
		} catch (NumberFormatException e) {
			return -1;
		}
	}

	private String constructPageUrl(Dataset dataset, int newOffset, QueryResult qr, Map<String, String> parameters) {
		StringBuilder url = new StringBuilder(DatasetUtil.buildDatasetURL(rowStore.getConfig().getBaseURL(), dataset.getId()));
		url.append("/json?_offset=");
		url.append(newOffset);
		url.append("&_limit=");
		url.append(qr.getLimit());
		for (Map.Entry<String, String> entry : parameters.entrySet()) {
			String k = entry.getKey();
			if ("_offset".equals(k) || "_limit".equals(k)) {
				continue;
			}
			url.append("&");
			url.append(URLEncoder.encode(k, StandardCharsets.UTF_8));
			url.append("=");
			url.append(URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8));
		}
		return url.toString();
	}

}
