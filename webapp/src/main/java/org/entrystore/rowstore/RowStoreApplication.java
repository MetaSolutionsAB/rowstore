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

package org.entrystore.rowstore;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.entrystore.rowstore.filters.JSCallbackFilter;
import org.entrystore.rowstore.filters.RateLimitFilter;
import org.entrystore.rowstore.resources.AliasResource;
import org.entrystore.rowstore.resources.DatasetInfoResource;
import org.entrystore.rowstore.resources.DatasetResource;
import org.entrystore.rowstore.resources.DatasetsResource;
import org.entrystore.rowstore.resources.DefaultResource;
import org.entrystore.rowstore.resources.ExportResource;
import org.entrystore.rowstore.resources.StatusResource;
import org.entrystore.rowstore.resources.SwaggerResource;
import org.entrystore.rowstore.resources.WebGuiResource;
import org.entrystore.rowstore.store.RowStore;
import org.entrystore.rowstore.store.RowStoreConfig;
import org.entrystore.rowstore.store.impl.PgRowStore;
import org.json.JSONException;
import org.json.JSONObject;
import org.restlet.Application;
import org.restlet.Context;
import org.restlet.Restlet;
import org.restlet.engine.io.IoUtils;
import org.restlet.routing.Router;
import org.restlet.routing.Template;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * RowStore converts and pushes CSV to a JSON-aware backend.
 * Provides a REST API for management and querying.
 *
 * @author Hannes Ebner
 */
public class RowStoreApplication extends Application {

	static Logger log = LoggerFactory.getLogger(RowStoreApplication.class);

	public static String KEY = RowStoreApplication.class.getCanonicalName();

	public static String NAME = "RowStore";

	protected final static String ENV_CONFIG_URI = "ROWSTORE_CONFIG_URI";

	private static String VERSION = null;

	RowStore rowstore;

	RowStoreConfig config;

	public RowStoreApplication(Context parentContext) throws IOException, JSONException {
		this(parentContext, null);
	}

	public RowStoreApplication(Context parentContext, URI configURI) throws IOException, JSONException {
		super(parentContext);
		getContext().getAttributes().put(KEY, this);
		if (configURI == null) {
			String envConfigURI = System.getenv(ENV_CONFIG_URI);
			if (envConfigURI != null) {
				configURI = URI.create(envConfigURI);
			}
			if (configURI == null) {
				configURI = getConfigurationURI("rowstore.json");
			}
		}

		if (configURI != null && (
				"file".equals(configURI.getScheme()) ||
				"http".equals(configURI.getScheme()) ||
				"https".equals(configURI.getScheme()))) {
			log.info("Loading configuration from " + configURI);

			try (InputStream is = configURI.toURL().openStream()) {
				config = new RowStoreConfig(new JSONObject(IoUtils.toString(is)));
			} catch (IOException e) {
				log.error("Error when loading configuration: " + e.getMessage());
				System.exit(1);
			}

			setLogLevel(config.getLogLevel());

			if (config.getQueryTimeout() > -1) {
				log.info("Query timeout set to " + config.getQueryTimeout() + " second" + (config.getQueryTimeout() > 1 ? "s" : ""));
			} else {
				log.info("No query timeout configured");
			}

			rowstore = new PgRowStore(config);
			log.info("Started RowStore " + getVersion());
		} else {
			log.error("No configuration found or URL scheme not supported");
			System.exit(1);
		}
	}

	@Override
	public synchronized Restlet createInboundRoot() {
		getContext().getParameters().add("useForwardedForHeader", "true");

		Router router = new Router(getContext());
		router.setDefaultMatchingMode(Template.MODE_EQUALS);

		// global scope
		router.attach("/status", StatusResource.class);
		router.attach("/dataset/{id}", DatasetResource.class);
		router.attach("/dataset/{id}/aliases", AliasResource.class);
		router.attach("/dataset/{id}/export", ExportResource.class);
		router.attach("/dataset/{id}/html", WebGuiResource.class);
		router.attach("/dataset/{id}/info", DatasetInfoResource.class);
		router.attach("/dataset/{id}/json", DatasetResource.class);
		router.attach("/dataset/{id}/swagger", SwaggerResource.class);
		router.attach("/datasets", DatasetsResource.class);
		router.attach("/", DefaultResource.class);

		JSCallbackFilter jsCallback = new JSCallbackFilter();
		jsCallback.setNext(router);

		if (config.isRateLimitEnabled()) {
			log.info("Request limit enabled. Time range: " + config.getRateLimitTimeRange() + " seconds. Limit globally: " + config.getRateLimitRequestsGlobal() + ", limit per dataset: " + config.getRateLimitRequestsDataset() + ", limit per client IP: " + config.getRateLimitRequestsClientIP());
			RateLimitFilter rateLimitFilter = new RateLimitFilter(config);
			rateLimitFilter.setNext(jsCallback);
			return rateLimitFilter;
		}

		return jsCallback;
	}

	public RowStore getRowStore() {
		return this.rowstore;
	}

	public static URI getConfigurationURI(String fileName) {
		URL resURL = Thread.currentThread().getContextClassLoader().getResource(fileName);
		try {
			if (resURL != null) {
				return resURL.toURI();
			}
		} catch (URISyntaxException e) {
			log.error(e.getMessage());
		}

		String classPath = System.getProperty("java.class.path");
		String[] pathElements = classPath.split(System.getProperty("path.separator"));
		for (String element : pathElements)	{
			File newFile = new File(element, fileName);
			if (newFile.exists()) {
				return newFile.toURI();
			}
		}
		log.error("Unable to find " + fileName + " in classpath");
		return null;
	}

	public static String getVersion() {
		if (VERSION == null) {
			URI versionFile = getConfigurationURI("VERSION.txt");
			try {
				log.debug("Reading version number from " + versionFile);
				VERSION = readFirstLine(versionFile.toURL());
			} catch (IOException e) {
				log.error(e.getMessage());
			}
			if (VERSION == null) {
				VERSION = new SimpleDateFormat("yyyyMMdd").format(new Date());
			}
		}
		return VERSION;
	}

	private void setLogLevel(String logLevel) {
		Level l = Level.toLevel(logLevel, Level.INFO);
		Configurator.setRootLevel(l);
		log.info("Log level set to " + l);
	}

	private static String readFirstLine(URL url) {
		BufferedReader in = null;
		try {
			in = new BufferedReader(new InputStreamReader(url.openStream()));
			return in.readLine();
		} catch (IOException ioe) {
			log.error(ioe.getMessage());
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
					log.error(e.getMessage());
				}
			}
		}
		return null;
	}

	public static String readStringFromUrl(URL inputUrl) {
		if (inputUrl == null) {
			throw new IllegalArgumentException("Parameter must not be null");
		}
		try (InputStream in = inputUrl.openStream()) {
			return new String(in.readAllBytes(), StandardCharsets.UTF_8);
		} catch (IOException ioe) {
			log.error(ioe.getMessage());
			return null;
		}
	}

	@Override
	public synchronized void stop() throws Exception {
		log.info("Shutting down");
		if (rowstore != null) {
			rowstore.shutdown();
		}
		super.stop();
	}

}