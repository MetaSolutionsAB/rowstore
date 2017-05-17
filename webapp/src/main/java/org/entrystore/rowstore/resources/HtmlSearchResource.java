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
import org.restlet.representation.FileRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.Get;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * Returns a basic search GUI.
 *
 * @author Hannes Ebner
 */
public class HtmlSearchResource extends BaseResource {

	static Logger log = Logger.getLogger(HtmlSearchResource.class);

	private Dataset dataset;

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

		File htmlFile = getHtml("searchgui.html");
		if (htmlFile != null) {
			log.debug("Loading HTML file from " + htmlFile.getAbsolutePath());
			return new FileRepresentation(htmlFile, MediaType.TEXT_HTML);
		}

		getResponse().setStatus(Status.CLIENT_ERROR_NOT_FOUND);
		return null;
	}

	private File getHtml(String fileName) {
		URL resURL = Thread.currentThread().getContextClassLoader().getResource(fileName);
		if (resURL != null) {
			try {
				return new File(resURL.toURI());
			} catch (URISyntaxException e) {
				log.error(e.getMessage());
			}
		}
		log.error("Unable to find " + fileName + " in classpath");
		return null;
	}

}