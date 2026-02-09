/*
 * Copyright (c) 2011-2017 MetaSolutions AB <info@metasolutions.se>
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
public class WebGuiController {

	private static final Logger log = LoggerFactory.getLogger(WebGuiController.class);

	private final RowStore rowStore;

	private static String htmlEmbed = null;
	private static String htmlFull = null;

	static {
		try {
			String header = readClasspathResource("webgui_header.html");
			htmlEmbed = header + readClasspathResource("webgui_body_embed.html");
			htmlFull = header + readClasspathResource("webgui_body_full.html");
		} catch (IOException e) {
			log.error(e.getMessage());
		}
	}

	public WebGuiController(RowStore rowStore) {
		this.rowStore = rowStore;
	}

	@GetMapping(value = "/dataset/{id}/html")
	public ResponseEntity<String> represent(@PathVariable("id") String id,
			@RequestParam Map<String, String> allParams) {
		Dataset dataset = rowStore.getDatasets().getDataset(id);
		if (dataset == null) {
			return ResponseEntity.notFound().build();
		}

		if (htmlEmbed == null || htmlFull == null) {
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
		}

		if (allParams.containsKey("embed")) {
			return ResponseEntity.ok(htmlEmbed);
		}

		return ResponseEntity.ok(htmlFull);
	}

	private static String readClasspathResource(String name) throws IOException {
		try (InputStream is = WebGuiController.class.getClassLoader().getResourceAsStream(name)) {
			if (is == null) {
				throw new IOException("Resource not found: " + name);
			}
			try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
				return reader.lines().collect(Collectors.joining("\n"));
			}
		}
	}

}
