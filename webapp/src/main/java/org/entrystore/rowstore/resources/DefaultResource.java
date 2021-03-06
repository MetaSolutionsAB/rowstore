/*
 * Copyright (c) 2011-2015 MetaSolutions AB <info@metasolutions.se>
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

import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Get;
import org.restlet.resource.ResourceException;


/**
 * Fallback if no other REST resource matches. Returns 404.
 * 
 * @author Hannes Ebner
 */
public class DefaultResource extends BaseResource {

	@Get
	public Representation represent() throws ResourceException {
		getResponse().setStatus(Status.CLIENT_ERROR_NOT_FOUND);
		String msg = "You made a request against the RowStore REST API. There is no resource at this URI.";
		return new StringRepresentation(msg);
	}

}