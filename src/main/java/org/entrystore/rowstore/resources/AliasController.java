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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashSet;
import java.util.Set;

@RestController
@RequestMapping("/dataset/{id}/aliases")
public class AliasController {

	private static final Logger log = LoggerFactory.getLogger(AliasController.class);

	private final RowStore rowStore;

	public AliasController(RowStore rowStore) {
		this.rowStore = rowStore;
	}

	@GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> getAliases(@PathVariable("id") String id) {
		Dataset dataset = rowStore.getDatasets().getDataset(id);
		if (dataset == null) {
			return ResponseEntity.notFound().build();
		}

		JSONArray result = new JSONArray();
		for (String alias : dataset.getAliases()) {
			result.put(alias);
		}
		return ResponseEntity.ok(result.toString());
	}

	@PutMapping(consumes = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<Void> setAliases(@PathVariable("id") String id, @RequestBody String body) {
		Dataset dataset = rowStore.getDatasets().getDataset(id);
		if (dataset == null) {
			return ResponseEntity.notFound().build();
		}

		try {
			if (!dataset.setAliases(parseJSONArray(body))) {
				return ResponseEntity.badRequest().build();
			}
		} catch (Exception e) {
			log.info(e.getMessage());
			return ResponseEntity.badRequest().build();
		}
		return ResponseEntity.noContent().build();
	}

	@PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<Void> addAliases(@PathVariable("id") String id, @RequestBody String body) {
		Dataset dataset = rowStore.getDatasets().getDataset(id);
		if (dataset == null) {
			return ResponseEntity.notFound().build();
		}

		try {
			Set<String> aliases = dataset.getAliases();
			aliases.addAll(parseJSONArray(body));
			if (!dataset.setAliases(aliases)) {
				return ResponseEntity.badRequest().build();
			}
		} catch (Exception e) {
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

		dataset.setAliases(new HashSet<>());
		return ResponseEntity.noContent().build();
	}

	private Set<String> parseJSONArray(String jsonText) {
		JSONArray request = new JSONArray(jsonText);
		Set<String> aliases = new HashSet<>();
		for (int i = 0; i < request.length(); i++) {
			aliases.add(request.get(i).toString());
		}
		return aliases;
	}

}
