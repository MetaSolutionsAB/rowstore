/*
 * Copyright (c) 2011-2017 MetaSolutions AB <info@metasolutions.se>
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

import org.entrystore.rowstore.RowStoreApplication;
import org.entrystore.rowstore.store.Dataset;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Get;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Returns a basic search GUI.
 *
 * @author Hannes Ebner
 */
public class WebGuiResource extends BaseResource {

	static Logger log = LoggerFactory.getLogger(WebGuiResource.class);

	private Dataset dataset;

	static String htmlEmbed = null;

	static String htmlFull = null;

	static {
		try {
			htmlEmbed = RowStoreApplication.readStringFromUrl(RowStoreApplication.getConfigurationURI("webgui_header.html").toURL());
			htmlFull = htmlEmbed;
			htmlEmbed += RowStoreApplication.readStringFromUrl(RowStoreApplication.getConfigurationURI("webgui_body_embed.html").toURL());
			htmlFull += RowStoreApplication.readStringFromUrl(RowStoreApplication.getConfigurationURI("webgui_body_full.html").toURL());
		} catch (IOException e) {
			log.error(e.getMessage());
		}
	}

	@Override
	public void doInit() {
		String datasetId = (String) getRequest().getAttributes().get("id");
		if (datasetId != null) {
			dataset = getRowStore().getDatasets().getDataset(datasetId);
		}
	}

	@Get("text/html")
	public Representation represent() {
		if (dataset == null) {
			setStatus(Status.CLIENT_ERROR_NOT_FOUND);
			return null;
		}

		if (htmlEmbed == null || htmlFull == null) {
			setStatus(Status.SERVER_ERROR_INTERNAL);
			return null;
		}

		if (parameters.containsKey("embed")) {
			return new StringRepresentation(htmlEmbed, MediaType.TEXT_HTML);
		}

		return new StringRepresentation(htmlFull, MediaType.TEXT_HTML);
	}

}