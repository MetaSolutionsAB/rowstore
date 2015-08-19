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

package org.entrystore.rowstore.filters;

import org.restlet.Request;
import org.restlet.Response;
import org.restlet.data.Method;
import org.restlet.data.Status;
import org.restlet.routing.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Filter to check for a valid API-key as basic form of authentication.
 * 
 * @author Hannes Ebner
 */
public class ApiKeyFilter extends Filter {
	
	static private Logger log = LoggerFactory.getLogger(ApiKeyFilter.class);

	@Override
	protected int beforeHandle(Request request, Response response) {
		if (request != null) {
			if ((Method.GET.equals(request.getMethod()) || Method.HEAD.equals(request.getMethod()))) {
				return CONTINUE;
			} else {
				/*
				TODO

				check for API-key and its validity,
				return CONTINUE if the key matches,
				otherwise return nothing
				 */
				return CONTINUE;
			}
		}
		response.setStatus(Status.CLIENT_ERROR_UNAUTHORIZED);
		return STOP;
	}
	
}