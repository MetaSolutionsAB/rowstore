/*
 * Copyright (c) 2011-2016 MetaSolutions AB <info@metasolutions.se>
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
import org.entrystore.rowstore.store.Dataset;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Get;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.entrystore.rowstore.RowStoreApplication.getConfigurationURI;
import static org.entrystore.rowstore.RowStoreApplication.getVersion;

/**
 * Returns a Swagger description for a dataset.
 *
 * @author Hannes Ebner
 */
public class SwaggerResource extends BaseResource {

	static Logger log = Logger.getLogger(SwaggerResource.class);

	private Dataset dataset;

	@Override
	public void doInit() {
		String datasetId = (String) getRequest().getAttributes().get("id");
		if (datasetId != null) {
			dataset = getRowStore().getDatasets().getDataset(datasetId);
		}
	}

	@Get("application/json")
	public Representation represent() {
		if (dataset == null) {
			setStatus(Status.CLIENT_ERROR_NOT_FOUND);
			return null;
		}

		URI swaggerURI = getConfigurationURI("swagger.json_template");
		String swaggerTemplate = readFile(new File(swaggerURI).toPath(), Charset.defaultCharset());

		JSONArray apiParams = new JSONArray();
		for (String p : dataset.getColumnNames()) {
			JSONObject apiParam = new JSONObject();
			try {
				apiParam.put("name", p);
				apiParam.put("in", "query");
				//apiParam.put("description", "");
				apiParam.put("required", false);
				apiParam.put("type", "string");
			} catch (JSONException jsone) {
				log.error(jsone.getMessage());
			}
			apiParams.put(apiParam);
		}

		String result = swaggerTemplate.
				replaceAll("__ROWSTORE_VERSION__", getVersion()).
				replaceAll("__DATASET_ID__", dataset.getId()).
				replaceAll("__DATASET_PARAMETERS__", apiParams.toString());

		return new StringRepresentation(result, MediaType.APPLICATION_JSON);
	}

	private static String readFile(Path path, Charset encoding) {
		if (path == null || !path.toFile().exists()) {
			return null;
		}
		byte[] encoded = new byte[0];
		try {
			encoded = Files.readAllBytes(path);
		} catch (IOException e) {
			log.error(e.getMessage());
			return null;
		}
		return encoding.decode(ByteBuffer.wrap(encoded)).toString();
	}

}