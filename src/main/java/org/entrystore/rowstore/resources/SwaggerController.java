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
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

@RestController
public class SwaggerController {

	private static final Logger log = LoggerFactory.getLogger(SwaggerController.class);

	private final RowStore rowStore;
	private final VersionInfo versionInfo;

	private static String swaggerTemplate;

	static {
		try (InputStream is = SwaggerController.class.getClassLoader().getResourceAsStream("swagger.json_template")) {
			if (is != null) {
				try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
					swaggerTemplate = reader.lines().collect(Collectors.joining("\n"));
				}
			}
		} catch (IOException e) {
			log.error(e.getMessage());
		}
	}

	public SwaggerController(RowStore rowStore, VersionInfo versionInfo) {
		this.rowStore = rowStore;
		this.versionInfo = versionInfo;
	}

	@GetMapping(value = "/dataset/{id}/swagger", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> represent(@PathVariable("id") String id) {
		Dataset dataset = rowStore.getDatasets().getDataset(id);
		if (dataset == null) {
			return ResponseEntity.notFound().build();
		}

		if (swaggerTemplate == null) {
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
		}

		JSONArray apiParams = new JSONArray();
		for (String p : dataset.getColumnNames()) {
			JSONObject apiParam = new JSONObject();
			apiParam.put("name", p);
			apiParam.put("in", "query");
			if (rowStore.getConfig().getRegexpQuerySupport() > 0) {
				apiParam.put("description", "Responses contain only rows with rows that match the provided tuple. Regular expressions may be used.");
			} else {
				apiParam.put("description", "Responses contain only rows that match the provided tuple(s). Exact matching is applied.");
			}
			apiParam.put("required", false);
			apiParam.put("type", "string");
			apiParams.put(apiParam);
		}

		// _limit
		JSONObject paramLimit = new JSONObject();
		paramLimit.put("name", "_limit");
		paramLimit.put("in", "query");
		paramLimit.put("required", false);
		paramLimit.put("type", "integer");
		paramLimit.put("default", 100);
		paramLimit.put("description", "Size of the result windows, expects a value from 1 to " + rowStore.getConfig().getQueryMaxLimit() + ". Default is 100.");
		apiParams.put(paramLimit);

		// _offset
		JSONObject paramOffset = new JSONObject();
		paramOffset.put("name", "_offset");
		paramOffset.put("in", "query");
		paramOffset.put("required", false);
		paramOffset.put("type", "integer");
		paramOffset.put("default", 0);
		paramOffset.put("description", "The offset (results, not pages) to be used when paginating through query results; example: page 3 of a multi page result can be requested with _limit=50 and _offset=100");
		apiParams.put(paramOffset);

		URI base = URI.create(rowStore.getConfig().getBaseURL());

		String result = swaggerTemplate.
				replaceAll("__ROWSTORE_VERSION__", versionInfo.getVersion()).
				replaceAll("__HOST__", base.getHost()).
				replaceAll("__BASEPATH__", base.getPath()).
				replaceAll("__DATASET_ID__", dataset.getId()).
				replaceAll("__DATASET_PARAMETERS__", apiParams.toString());

		return ResponseEntity.ok(result);
	}

}
