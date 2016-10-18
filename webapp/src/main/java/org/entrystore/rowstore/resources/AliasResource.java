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
import org.restlet.data.Method;
import org.restlet.data.Status;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.Delete;
import org.restlet.resource.Get;
import org.restlet.resource.Post;
import org.restlet.resource.Put;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Returns information about a dataset.
 *
 * @author Hannes Ebner
 */
public class AliasResource extends BaseResource {

	static Logger log = Logger.getLogger(AliasResource.class);

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

		JSONArray result = new JSONArray();
		for (String alias : dataset.getAliases()) {
			result.put(alias);
		}

		return new JsonRepresentation(result);
	}

	@Put("application/json")
	@Post("application/json")
	public void setAliases(Representation r) {
		if (dataset == null) {
			setStatus(Status.CLIENT_ERROR_NOT_FOUND);
			return;
		}

		try {
			synchronized (dataset) {
				Set<String> aliases;
				if (Method.POST.equals(getRequest().getMethod())) {
					aliases = dataset.getAliases();
				} else {
					aliases = new HashSet<>();
				}
				aliases.addAll(parseJSONArrayInRepresentation(r));
				if (!dataset.setAliases(aliases)) {
					setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
				}
			}
		} catch (Exception e) {
			log.info(e.getMessage());
			setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
			return;
		}
	}

	@Delete
	public void purgeAliases() {
		if (dataset == null) {
			setStatus(Status.CLIENT_ERROR_NOT_FOUND);
			return;
		}

		dataset.setAliases(new HashSet<String>());
	}

	private Set<String> parseJSONArrayInRepresentation(Representation r) throws JSONException, IOException {
		JSONArray request = new JSONArray(r.getText());
		Set<String> aliases = new HashSet<>();
		for (int i=0; i < request.length(); i++) {
			aliases.add(request.get(i).toString());
		}
		return aliases;
	}

}