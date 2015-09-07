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

import org.apache.log4j.Logger;
import org.entrystore.rowstore.RowStoreApplication;
import org.entrystore.rowstore.store.RowStore;
import org.restlet.Context;
import org.restlet.Request;
import org.restlet.Response;
import org.restlet.resource.ServerResource;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

/**
 * Base resource from which all other REST resources are subclassed.
 *
 * Handles basic functionality such as parsing of parameters.
 *
 * @author Hannes Ebner
 */
public class BaseResource extends ServerResource {

	protected HashMap<String,String> parameters;

	private static Logger log = Logger.getLogger(BaseResource.class);

	@Override
	public void init(Context c, Request request, Response response) {
		parameters = parseRequest(request.getResourceRef().getRemainingPart());
		super.init(c, request, response);
	}

	@Override
	protected void doRelease() {

	}

	static public HashMap<String, String> parseRequest(String request) {
		HashMap<String, String> argsAndVal = new HashMap<String, String>();

		int r = request.lastIndexOf("?");
		String req = request.substring(r + 1);
		String[] arguments = req.split("&");

		try {
			for (int i = 0; i < arguments.length; i++) {
				if (arguments[i].contains("=")) {
					String[] elements = arguments[i].split("=");
					String key = urlDecode(elements[0]).trim();
					String value = urlDecode(elements[1]).trim();
					if (key.length() > 0) {
						argsAndVal.put(key, value);
					}
				} else {
					String key = urlDecode(arguments[i]).trim();
					if (key.length() > 0) {
						argsAndVal.put(key, "");
					}
				}
			}
		} catch (IndexOutOfBoundsException e) {
			// special case!
			argsAndVal.put(req, "");
		}
		return argsAndVal;
	}

	private static String urlDecode(String input) {
		if (input != null) {
			try {
				return URLDecoder.decode(input, "UTF-8");
			} catch (UnsupportedEncodingException uee) {
				log.error(uee.getMessage());
			}
		}
		return null;
	}

	public RowStoreApplication getRowStoreApplication() {
		Context c = getContext();
		return (RowStoreApplication) c.getAttributes().get(RowStoreApplication.KEY);
	}

	public RowStore getRowStore() {
		return getRowStoreApplication().getRowStore();
	}

}