/*
 * Copyright (c) 2014 MetaSolutions AB <info@metasolutions.se>
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
import org.json.JSONException;
import org.json.JSONObject;
import org.postgresql.ds.PGSimpleDataSource;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.Get;

/**
 * @author Hannes Ebner
 */
public class StatusResource extends BaseResource {

	private static Logger log = Logger.getLogger(StatusResource.class);

	@Get("json")
	public Representation getJSON() throws JSONException {
		JSONObject result = new JSONObject();
		result.put("service", "RowStore");
		result.put("version", getRowStoreApplication().getVersion());
		result.put("datasets", getRowStore().getDatasets().amount());
		return new JsonRepresentation(result);
	}

}