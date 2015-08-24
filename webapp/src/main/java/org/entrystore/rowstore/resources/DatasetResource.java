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
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.resource.Delete;
import org.restlet.resource.Get;

/**
 * @author Hannes Ebner
 */
public class DatasetResource extends BaseResource {

	static Logger log = Logger.getLogger(DatasetResource.class);

	@Get
	public Representation represent() {
		if (!hasAllParameters()) {
			getResponse().setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
			return null;
		}

		/* TODO

		1. read id from /dataset/{id}
		2. return whole dataset in JSON

		// OR

		2. reply to query if parameters are present: /dataset/{id}?[{column-name}={value},{column-name={value}]

		 */

		getResponse().setStatus(Status.CLIENT_ERROR_NOT_FOUND);
		return null;
	}

	@Override
	public Representation head() {
		// TODO return conversion status or dataset metadata
		return null;
	}

	@Delete
	public void purgeDataset() {
		// TODO
	}

	private boolean hasAllParameters() {
		return true;
	}

}