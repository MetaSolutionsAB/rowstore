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

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.entrystore.rowstore.filters.JSCallbackFilter;
import org.entrystore.rowstore.resources.DatasetResource;
import org.entrystore.rowstore.resources.DatasetsResource;
import org.entrystore.rowstore.resources.DefaultResource;
import org.entrystore.rowstore.resources.StatusResource;
import org.entrystore.rowstore.store.RowStore;
import org.entrystore.rowstore.store.RowStoreConfig;
import org.entrystore.rowstore.store.impl.PgRowStore;
import org.json.JSONException;
import org.json.JSONObject;
import org.restlet.Application;
import org.restlet.Component;
import org.restlet.Context;
import org.restlet.Restlet;
import org.restlet.data.Protocol;
import org.restlet.routing.Router;
import org.restlet.routing.Template;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * RowStore converts and pushes CSV to a JSON-aware backend. Provides a REST API for management and querying.
 *
 * @author Hannes Ebner
 */
public class RowStoreApplication extends Application {

	static Logger log = Logger.getLogger(RowStoreApplication.class);

	public static String KEY = RowStoreApplication.class.getCanonicalName();

	public static String NAME = "RowStore";

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
			configURI = getConfigurationURI("rowstore.json");
		}

		if (configURI != null && "file".equals(configURI.getScheme())) {
			config = new RowStoreConfig(new JSONObject(new String(Files.readAllBytes(Paths.get(configURI)))));
			setLogLevel(config.getLogLevel());
			rowstore = new PgRowStore(config);
		} else {
			log.error("No configuration found");
			System.exit(1);
		}
	}

	@Override
	public synchronized Restlet createInboundRoot() {
		Router router = new Router(getContext());
		router.setDefaultMatchingMode(Template.MODE_EQUALS);

		// global scope
		router.attach("/status", StatusResource.class);
		router.attach("/dataset/{id}", DatasetResource.class);
		router.attach("/datasets", DatasetsResource.class);
		router.attach("/", DefaultResource.class);

		JSCallbackFilter jsCallback = new JSCallbackFilter();
		jsCallback.setNext(router);

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

	public static void main(String[] args) {
		int port = 8282;
		URI configURI = null;

		if (args.length > 0) {
			configURI = new File(args[0]).toURI();
		}

		if (args.length > 1) {
			try {
				port = Integer.valueOf(args[0]);
			} catch (NumberFormatException nfe) {
				System.err.println(nfe.getMessage());
			}
		}

		if (configURI == null) {
			System.out.println("RowStore - http://entrystore.org/rowstore/");
			System.out.println("");
			System.out.println("Usage: rowstore <path to configuration file> [listening port]");
			System.out.println("");
			System.exit(1);
		}

		Component component = new Component();
		component.getServers().add(Protocol.HTTP, port);
		component.getClients().add(Protocol.HTTP);
		component.getClients().add(Protocol.HTTPS);
		Context c = component.getContext().createChildContext();

		try {
			component.getDefaultHost().attach(new RowStoreApplication(c, configURI));
			component.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void setLogLevel(String logLevel) {
		BasicConfigurator.configure();
		Level l = Level.INFO;
		if (logLevel != null) {
			l = Level.toLevel(logLevel, Level.INFO);
		}
		Logger.getRootLogger().setLevel(l);
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

	@Override
	public synchronized void stop() throws Exception {
		log.info("Shutting down");
		if (rowstore != null) {
			rowstore.shutdown();
		}
		super.stop();
	}

}