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

import org.entrystore.rowstore.resources.model.DatasetInfoResponse;
import org.entrystore.rowstore.store.Dataset;
import org.entrystore.rowstore.store.RowStore;
import org.entrystore.rowstore.util.DatasetUtil;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.text.SimpleDateFormat;

@RestController
public class DatasetInfoController {

	private final RowStore rowStore;

	public DatasetInfoController(RowStore rowStore) {
		this.rowStore = rowStore;
	}

	@GetMapping(value = "/dataset/{id}/info", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<DatasetInfoResponse> getInfo(@PathVariable("id") String id) {
		Dataset dataset = rowStore.getDatasets().getDataset(id);
		if (dataset == null) {
			return ResponseEntity.notFound().build();
		}

		DatasetInfoResponse body = new DatasetInfoResponse(
				dataset.getStatus(),
				new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").format(dataset.getCreationDate()),
				dataset.getColumnNames(),
				dataset.getRowCount(),
				dataset.getId(),
				dataset.getAliases(),
				"https://entrystore.org/rowstore/",
				DatasetUtil.buildDatasetURL(rowStore.getConfig().getBaseURL(), dataset.getId())
		);

		return ResponseEntity.ok(body);
	}

}
