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

import org.entrystore.rowstore.store.Dataset;
import org.entrystore.rowstore.store.RowStore;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.text.SimpleDateFormat;

@RestController
public class DatasetInfoController {

	private static final Logger log = LoggerFactory.getLogger(DatasetInfoController.class);

	private final RowStore rowStore;

	public DatasetInfoController(RowStore rowStore) {
		this.rowStore = rowStore;
	}

	@GetMapping(value = "/dataset/{id}/info", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> getInfo(@PathVariable("id") String id) {
		Dataset dataset = rowStore.getDatasets().getDataset(id);
		if (dataset == null) {
			return ResponseEntity.notFound().build();
		}

		JSONObject result = new JSONObject();
		try {
			result.put("status", dataset.getStatus());
			result.put("created", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").format(dataset.getCreationDate()));
			result.put("columnnames", dataset.getColumnNames());
			result.put("rowcount", dataset.getRowCount());
			result.put("identifier", dataset.getId());
			result.put("aliases", dataset.getAliases());

			String baseURL = rowStore.getConfig().getBaseURL();
			baseURL += baseURL.endsWith("/") ? "" : "/";
			result.put("@context", "https://entrystore.org/rowstore/");
			result.put("@id", baseURL + "dataset/" + dataset.getId());
		} catch (JSONException e) {
			log.error(e.getMessage());
		}

		return ResponseEntity.ok(result.toString());
	}

}
