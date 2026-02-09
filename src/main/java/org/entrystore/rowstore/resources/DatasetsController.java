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
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.IOException;
import java.util.Set;

@RestController
public class DatasetsController {

	private static final Logger log = LoggerFactory.getLogger(DatasetsController.class);

	private final RowStore rowStore;

	public DatasetsController(RowStore rowStore) {
		this.rowStore = rowStore;
	}

	@GetMapping(value = "/datasets", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> listDatasets() {
		JSONArray result = new JSONArray();
		Set<Dataset> datasets = rowStore.getDatasets().getAll();
		if (datasets != null) {
			for (Dataset ds : datasets) {
				result.put(ds.getId());
			}
			return ResponseEntity.ok(result.toString());
		}
		return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
	}

	@PostMapping(value = "/datasets", consumes = "text/csv")
	public ResponseEntity<String> acceptCSV(HttpServletRequest request) {
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

			// Check if the temp file is empty (contentLength was -1 but stream had no data)
			if (tmpFile.length() == 0) {
				return ResponseEntity.badRequest().build();
			}

			Dataset newDataset = rowStore.getDatasets().createDataset();
			if (newDataset == null) {
				return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
			}
			newDataset.setStatus(EtlStatus.ACCEPTED_DATA);
			EtlResource etlResource = new EtlResource(newDataset, tmpFile, "text/csv", true);
			rowStore.getEtlProcessor().submit(etlResource);

			String datasetURL = DatasetUtil.buildDatasetURL(rowStore.getConfig().getBaseURL(), newDataset.getId());

			JSONObject result = new JSONObject();
			result.put("id", newDataset.getId());
			result.put("url", datasetURL);
			result.put("status", EtlStatus.ACCEPTED_DATA);
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

}
