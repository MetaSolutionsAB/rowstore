/*
 * Copyright (c) 2011-2026 MetaSolutions AB <info@metasolutions.se>
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

package org.entrystore.rowstore.config;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.entrystore.rowstore.store.RowStoreConfig;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;

@Configuration
public class RowStoreConfigLoader {

	private static final Logger log = LoggerFactory.getLogger(RowStoreConfigLoader.class);

	private static final String ENV_CONFIG_URI = "ROWSTORE_CONFIG_URI";

	/**
	 * Jetty connector parameters of 1.x, no longer supported (CFG-8.02).
	 */
	private static final String ENV_CONNECTOR_PARAMS = "ROWSTORE_CONNECTOR_PARAMS";

	/**
	 * @throws RowStoreConfigException If no configuration is found, or if it cannot be read or is invalid; reported
	 * by {@link RowStoreConfigFailureAnalyzer} as a single error message without a stack trace.
	 */
	@Bean
	public RowStoreConfig rowStoreConfig(
			@Value("${rowstore.config.uri:#{null}}") String configUriProperty) {

		URI configURI = resolveConfigUri(configUriProperty);

		if (configURI == null) {
			throw new RowStoreConfigException("No RowStore configuration found. Set --rowstore.config.uri=<URI> or " +
					"the environment variable " + ENV_CONFIG_URI + ", or place rowstore.json on the classpath");
		}

		log.info("Loading configuration from {}", configURI);

		String jsonContent;
		try (InputStream is = configURI.toURL().openStream()) {
			jsonContent = new String(is.readAllBytes(), StandardCharsets.UTF_8);
		} catch (IOException | IllegalArgumentException e) {
			throw new RowStoreConfigException("Unable to read the configuration from " + configURI + ": " + e, e);
		}

		RowStoreConfig config;
		try {
			config = new RowStoreConfig(new JSONObject(jsonContent));
		} catch (JSONException e) {
			// Not parseable as JSON at all
			throw new RowStoreConfigException("rowstore.json invalid: " + e.getMessage(), e);
		} catch (IllegalStateException e) {
			throw new RowStoreConfigException(e.getMessage(), e);
		}

		setLogLevel(config.getLogLevel());

		if (System.getenv(ENV_CONNECTOR_PARAMS) != null) {
			log.warn("The environment variable {} of RowStore 1.x is not supported anymore and is ignored; the number " +
					"of concurrent database connections is limited with \"maxQueryConnections\" in rowstore.json", ENV_CONNECTOR_PARAMS);
		}

		if (config.getQueryTimeout() > -1) {
			log.info("Query timeout set to {} second{}", config.getQueryTimeout(),
					config.getQueryTimeout() > 1 ? "s" : "");
		} else {
			log.info("No query timeout configured");
		}

		return config;
	}

	private URI resolveConfigUri(String configUriProperty) {
		// 1. Spring property (command line --rowstore.config.uri=...)
		if (configUriProperty != null && !configUriProperty.isEmpty()) {
			return toUri(configUriProperty);
		}

		// 2. Environment variable
		String envConfigURI = System.getenv(ENV_CONFIG_URI);
		if (envConfigURI != null && !envConfigURI.isEmpty()) {
			return toUri(envConfigURI);
		}

		// 3. Classpath fallback
		var classLoader = Thread.currentThread().getContextClassLoader();
		var resource = classLoader.getResource("rowstore.json");
		if (resource != null) {
			try {
				return resource.toURI();
			} catch (Exception e) {
				log.error("Failed to resolve classpath resource URI: {}", e.getMessage());
			}
		}

		return null;
	}

	/**
	 * Converts the configured location to a URI (CFG-2.02): an absolute URI (e.g., {@code file:///etc/rowstore.json} or
	 * {@code https://...}) is used as it is, anything else is a file path, which is resolved against the working
	 * directory if it is relative.
	 *
	 * @throws RowStoreConfigException If the location is neither a URI nor a file path.
	 */
	static URI toUri(String location) {
		try {
			URI uri = new URI(location);
			// a single letter is a Windows drive (C:\...), not a scheme
			if (uri.isAbsolute() && uri.getScheme().length() > 1) {
				return uri;
			}
		} catch (URISyntaxException e) {
			// not a URI, e.g., a path with spaces
		}
		try {
			return Path.of(location).toAbsolutePath().normalize().toUri();
		} catch (InvalidPathException e) {
			throw new RowStoreConfigException("Invalid configuration location '" + location + "': " + e.getMessage(), e);
		}
	}

	private void setLogLevel(String logLevel) {
		Level l = Level.toLevel(logLevel, Level.INFO);
		Configurator.setRootLevel(l);
		log.info("Log4j log level set to {}", l);
	}

}
