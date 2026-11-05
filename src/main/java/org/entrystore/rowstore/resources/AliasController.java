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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletRequest;
import org.entrystore.rowstore.store.Dataset;
import org.entrystore.rowstore.store.RowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.Set;

@RestController
@RequestMapping("/dataset/{id}/aliases")
public class AliasController {

	private static final Logger log = LoggerFactory.getLogger(AliasController.class);

	private static final TypeReference<Set<String>> ALIASES = new TypeReference<>() {
	};

	private final RowStore rowStore;

	private final ObjectMapper objectMapper;

	public AliasController(RowStore rowStore, ObjectMapper objectMapper) {
		this.rowStore = rowStore;
		this.objectMapper = objectMapper;
	}

	@GetMapping
	public ResponseEntity<Set<String>> getAliases(@PathVariable("id") String id) {
		Dataset dataset = rowStore.getDatasets().getDataset(id);
		if (dataset == null) {
			return ResponseEntity.notFound().build();
		}

		return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(dataset.getAliases());
	}

	@PutMapping
	public ResponseEntity<Void> setAliases(@PathVariable("id") String id, HttpServletRequest request) {
		Set<String> aliases = readAliases(request);
		if (aliases == null) {
			return ResponseEntity.badRequest().build();
		}
		Dataset dataset = rowStore.getDatasets().getDataset(id);
		if (dataset == null) {
			return ResponseEntity.notFound().build();
		}

		try {
			if (!dataset.setAliases(aliases)) {
				return ResponseEntity.badRequest().build();
			}
		} catch (IllegalArgumentException e) {
			// database failures are not caught, e.g., no connection available within the wait timeout results in 503
			log.info(e.getMessage());
			return ResponseEntity.badRequest().build();
		}
		return ResponseEntity.noContent().build();
	}

	@PostMapping
	public ResponseEntity<Void> addAliases(@PathVariable("id") String id, HttpServletRequest request) {
		Set<String> newAliases = readAliases(request);
		if (newAliases == null) {
			return ResponseEntity.badRequest().build();
		}
		Dataset dataset = rowStore.getDatasets().getDataset(id);
		if (dataset == null) {
			return ResponseEntity.notFound().build();
		}

		try {
			Set<String> aliases = dataset.getAliases();
			aliases.addAll(newAliases);
			if (!dataset.setAliases(aliases)) {
				return ResponseEntity.badRequest().build();
			}
		} catch (IllegalArgumentException e) {
			// database failures are not caught, e.g., no connection available within the wait timeout results in 503
			log.info(e.getMessage());
			return ResponseEntity.badRequest().build();
		}
		return ResponseEntity.noContent().build();
	}

	@DeleteMapping
	public ResponseEntity<Void> purgeAliases(@PathVariable("id") String id) {
		Dataset dataset = rowStore.getDatasets().getDataset(id);
		if (dataset == null) {
			return ResponseEntity.notFound().build();
		}

		dataset.setAliases(Set.of());
		return ResponseEntity.noContent().build();
	}

	/**
	 * Reads the aliases from the request body, a JSON array, regardless of the Content-Type, as in 1.x (API-3.09):
	 * e.g., fetch() sends text/plain and jQuery application/x-www-form-urlencoded by default. The body is read from
	 * the input stream, since the parameters of a form POST would consume it.
	 *
	 * @return Returns the aliases, or null if the body is not a JSON array.
	 */
	private Set<String> readAliases(HttpServletRequest request) {
		try {
			return objectMapper.readValue(request.getInputStream().readAllBytes(), ALIASES);
		} catch (IOException e) {
			log.info("Unable to read aliases from request body: {}", e.getMessage());
			return null;
		}
	}

}
