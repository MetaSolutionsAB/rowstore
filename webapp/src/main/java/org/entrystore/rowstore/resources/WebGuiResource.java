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

import org.apache.log4j.Logger;
import org.entrystore.rowstore.store.Dataset;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Get;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Returns a basic search GUI.
 *
 * @author Hannes Ebner
 */
public class WebGuiResource extends BaseResource {

	static Logger log = Logger.getLogger(WebGuiResource.class);

	private Dataset dataset;

	static String htmlEmbed = null;

	static String htmlFull = null;

	static {
		try {
			htmlEmbed = new String(Files.readAllBytes(getHtmlPath("webgui_header.html")));
			htmlFull = htmlEmbed;
			htmlEmbed += new String(Files.readAllBytes(getHtmlPath("webgui_body_embed.html")));
			htmlFull += new String(Files.readAllBytes(getHtmlPath("webgui_body_full.html")));
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

	static Path getHtmlPath(String fileName) {
		URL resURL = Thread.currentThread().getContextClassLoader().getResource(fileName);
		if (resURL != null) {
			try {
				return Paths.get(resURL.toURI());
			} catch (URISyntaxException e) {
				log.error(e.getMessage());
			}
		}
		log.error("Unable to find " + fileName + " in classpath");
		return null;
	}

}