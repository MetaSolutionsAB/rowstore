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
import jakarta.servlet.http.HttpServletResponse;
import org.entrystore.rowstore.etl.EtlResource;
import org.entrystore.rowstore.etl.EtlStatus;
import org.entrystore.rowstore.filters.RequestDecodingFilter;
import org.entrystore.rowstore.filters.TunnelFilter;
import org.entrystore.rowstore.resources.model.DatasetAcceptedResponse;
import org.entrystore.rowstore.resources.model.QueryResponse;
import org.entrystore.rowstore.store.Dataset;
import org.entrystore.rowstore.store.QueryResult;
import org.entrystore.rowstore.store.RowStore;
import org.entrystore.rowstore.util.DatasetUtil;
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
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@RestController
public class DatasetController {

	private static final Logger log = LoggerFactory.getLogger(DatasetController.class);

	private static final MediaType TEXT_PLAIN_UTF8 = new MediaType(MediaType.TEXT_PLAIN, StandardCharsets.UTF_8);

	private final RowStore rowStore;

	public DatasetController(RowStore rowStore) {
		this.rowStore = rowStore;
	}

	/**
	 * Responds with the JSON query result or with a redirect to the HTML view, depending on the Accept header
	 * (API-4.03, API-4.04): JSON is the first representation, so it is selected without Accept header, for
	 * *&#47;* and if the Accept header matches neither (e.g., image/png). All responses vary by the Accept header,
	 * also errors.
	 */
	@GetMapping(value = "/dataset/{id}")
	public ResponseEntity<?> represent(@PathVariable("id") String id, HttpServletRequest request,
			HttpServletResponse response) {
		response.addHeader(HttpHeaders.VARY, HttpHeaders.ACCEPT);
		if (MediaType.TEXT_HTML.equals(ContentNegotiation.select(request, MediaType.APPLICATION_JSON, MediaType.TEXT_HTML))) {
			return representHtml(id);
		}
		return doQuery(id, queryParameters(request));
	}

	/**
	 * Always responds with JSON, regardless of the Accept header (API-4.04). Otherwise, /dataset/{id}/json is the same
	 * resource as /dataset/{id}, as in 1.x: PUT, POST and DELETE are mapped to both paths (API-3.16).
	 */
	@GetMapping(value = "/dataset/{id}/json")
	public ResponseEntity<?> representJson(@PathVariable("id") String id, HttpServletRequest request) {
		return doQuery(id, queryParameters(request));
	}

	private ResponseEntity<Void> representHtml(String id) {
		Dataset dataset = rowStore.getDatasets().getDataset(id);
		if (dataset == null) {
			return ResponseEntity.notFound().build();
		}

		String redir = DatasetUtil.buildDatasetURL(rowStore.getConfig().getBaseURL(), id) + "/html";
		return ResponseEntity.status(HttpStatus.SEE_OTHER)
				.header(HttpHeaders.LOCATION, redir)
				.build();
	}

	/**
	 * Returns the query parameters as in 1.x (QUERY-2.03): names and values are trimmed, parameters whose name is
	 * empty after trimming are skipped, and of a repeated parameter the last value is used. A value that is empty as
	 * sent ({@code ?col=} or {@code ?col}, which Tomcat does not distinguish) is returned as null, whereas a value that
	 * is only empty after trimming ({@code ?col=%20}) is returned as an empty string.
	 */
	static Map<String, String> queryParameters(HttpServletRequest request) {
		Map<String, String> parameters = new LinkedHashMap<>();
		for (Map.Entry<String, String[]> entry : request.getParameterMap().entrySet()) {
			String key = entry.getKey().trim();
			String[] values = entry.getValue();
			if (!key.isEmpty() && values != null && values.length > 0) {
				String value = values[values.length - 1];
				parameters.put(key, value.isEmpty() ? null : value.trim());
			}
		}
		return parameters;
	}

	private ResponseEntity<?> doQuery(String id, Map<String, String> parameters) {
		Dataset dataset = rowStore.getDatasets().getDataset(id);
		if (dataset == null) {
			return ResponseEntity.notFound().build();
		}

		if (dataset.getStatus() == EtlStatus.CREATED) {
			return ResponseEntity.status(HttpStatus.FAILED_DEPENDENCY).build();
		}

		Set<String> columns = dataset.getColumnNames();
		Map<String, String> tuples = new HashMap<>();
		// tunnel parameters of 1.x that are not column names, neither filters nor carried over to the page URLs
		Set<String> tunnelParams = new HashSet<>();
		int specialParamCount = 0;
		for (String k : parameters.keySet()) {
			// _callback is reserved since 1.x (JSONP); JSONP is no longer supported, so it is ignored (QUERY-3.05)
			if ("_limit".equals(k) || "_offset".equals(k) || "_sort".equals(k) || "_callback".equals(k)) {
				specialParamCount++;
				continue;
			}
			// consumed by the TunnelService in 1.x, unlike then they filter if the dataset has such a column (QUERY-3.06)
			if (TunnelFilter.PREFERENCE_PARAMETERS.contains(k) && !columns.contains(k)) {
				tunnelParams.add(k);
				specialParamCount++;
				continue;
			}
			// a value empty as sent is rejected as in 1.x, one that is empty after trimming matches empty cells
			String value = parameters.get(k);
			if (value == null) {
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
						.contentType(TEXT_PLAIN_UTF8)
						.body("The submitted query exceeded the configured maximum time limit.");
			}
			return ResponseEntity.badRequest().build();
		}

		List<Map<String, Object>> rows = qResult.getResults().stream()
				.map(JSONObject::toMap)
				.toList();

		Map<String, String> pageParameters = new LinkedHashMap<>(parameters);
		pageParameters.keySet().removeAll(tunnelParams);

		String prev = null;
		if ((qResult.getOffset() - qResult.getLimit()) >= 0) {
			prev = constructPageUrl(dataset, qResult.getOffset() - qResult.getLimit(), qResult, pageParameters);
		}

		String next = null;
		if (qResult.getResultCount() >= (qResult.getLimit() + qResult.getOffset())) {
			next = constructPageUrl(dataset, qResult.getOffset() + qResult.getLimit(), qResult, pageParameters);
		}

		return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(new QueryResponse(
				rows,
				qResult.getLimit(),
				qResult.getOffset(),
				qResult.getResultCount(),
				qResult.getQueryTime(),
				prev,
				next
		));
	}

	@PostMapping(value = {"/dataset/{id}", "/dataset/{id}/json"})
	public ResponseEntity<?> acceptCSVPost(@PathVariable("id") String id, HttpServletRequest request) {
		return acceptCSV(id, request, true);
	}

	@PutMapping(value = {"/dataset/{id}", "/dataset/{id}/json"})
	public ResponseEntity<?> acceptCSVPut(@PathVariable("id") String id, HttpServletRequest request) {
		return acceptCSV(id, request, false);
	}

	private ResponseEntity<?> acceptCSV(String id, HttpServletRequest request, boolean append) {
		if (!DatasetUtil.isCsvContentType(request.getContentType())) {
			return ResponseEntity.status(HttpStatus.UNSUPPORTED_MEDIA_TYPE).build();
		}

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
				return ResponseEntity.status(HttpStatus.CONTENT_TOO_LARGE).build();
			} catch (RequestDecodingFilter.MalformedContentException mce) {
				// e.g., truncated gzip data (API-3.17)
				log.info(mce.getMessage());
				return ResponseEntity.badRequest().build();
			} catch (IOException ioe) {
				log.error(ioe.getMessage());
				return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
			}

			// an empty body without Content-Length (e.g., chunked) or a body that is empty after decoding (API-3.17)
			if (tmpFile.length() == 0) {
				return ResponseEntity.badRequest().build();
			}

			if (dataset.getStatus() != EtlStatus.PROCESSING) {
				dataset.setStatus(EtlStatus.ACCEPTED_DATA);
			}
			EtlResource etlResource = new EtlResource(dataset, tmpFile, "text/csv", append);
			rowStore.getEtlProcessor().submit(etlResource);
			// the ETL job deletes the file from now on
			accepted = true;

			String datasetURL = DatasetUtil.buildDatasetURL(rowStore.getConfig().getBaseURL(), dataset.getId());

			DatasetAcceptedResponse body = new DatasetAcceptedResponse(
					dataset.getId(),
					datasetURL,
					dataset.getStatus(),
					datasetURL + "/info"
			);

			HttpHeaders headers = new HttpHeaders();
			headers.add(HttpHeaders.LOCATION, datasetURL);
			headers.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);
			return new ResponseEntity<>(body, headers, HttpStatus.ACCEPTED);
		} catch (IllegalStateException e) {
			// thrown by getStatus() if the dataset was deleted while the upload was received; a load that was
			// submitted already finds no dataset and is discarded (ETL-12.04)
			log.info("Dataset {} was deleted while its upload was received", dataset.getId());
			return ResponseEntity.notFound().build();
		} finally {
			if (tmpFile != null && !accepted) {
				log.info("Deleting temporary file " + tmpFile);
				tmpFile.delete();
			}
		}
	}

	/**
	 * Deletes a dataset (API-3.07): 204 without waiting for readers of its data table, which is dropped in the
	 * background (ETL-12.01), 423 unless its status is AVAILABLE or ERROR, 404 if it does not exist. Database failures
	 * propagate (503 for connectivity failures and lock or statement timeouts, see DatabaseExceptionHandler).
	 */
	@DeleteMapping(value = {"/dataset/{id}", "/dataset/{id}/json"})
	public ResponseEntity<Void> purgeDataset(@PathVariable("id") String id) {
		// resolves an alias to the dataset's ID
		Dataset dataset = rowStore.getDatasets().getDataset(id);
		if (dataset == null) {
			return ResponseEntity.notFound().build();
		}

		// the status is checked by purgeDataset() in the same transaction as the deletion
		return switch (rowStore.getDatasets().purgeDataset(dataset.getCanonicalId())) {
			case PURGED -> ResponseEntity.noContent().build();
			case LOCKED -> ResponseEntity.status(HttpStatus.LOCKED).build();
			case NOT_FOUND -> ResponseEntity.notFound().build();
		};
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
			// a value that is empty only after trimming is sent as a space, so that the link matches empty cells as
			// well (QUERY-2.03); a value that is empty as sent (null, e.g. _callback=) stays empty
			String value = entry.getValue();
			url.append(value == null ? "" : URLEncoder.encode(value.isEmpty() ? " " : value, StandardCharsets.UTF_8));
		}
		return url.toString();
	}

}
