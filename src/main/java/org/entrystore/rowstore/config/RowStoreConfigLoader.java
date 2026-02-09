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
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;

@Configuration
public class RowStoreConfigLoader {

	private static final Logger log = LoggerFactory.getLogger(RowStoreConfigLoader.class);

	private static final String ENV_CONFIG_URI = "ROWSTORE_CONFIG_URI";

	@Bean
	public RowStoreConfig rowStoreConfig(
			@Value("${rowstore.config.uri:#{null}}") String configUriProperty) throws IOException {

		URI configURI = resolveConfigUri(configUriProperty);

		if (configURI == null) {
			throw new IllegalStateException("No RowStore configuration found. " +
					"Set --rowstore.config.uri=..., ROWSTORE_CONFIG_URI env, or place rowstore.json on classpath.");
		}

		log.info("Loading configuration from {}", configURI);

		String jsonContent;
		try (InputStream is = configURI.toURL().openStream()) {
			jsonContent = new String(is.readAllBytes(), StandardCharsets.UTF_8);
		}

		RowStoreConfig config = new RowStoreConfig(new JSONObject(jsonContent));

		setLogLevel(config.getLogLevel());

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
			return URI.create(configUriProperty);
		}

		// 2. Environment variable
		String envConfigURI = System.getenv(ENV_CONFIG_URI);
		if (envConfigURI != null && !envConfigURI.isEmpty()) {
			return URI.create(envConfigURI);
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

	private void setLogLevel(String logLevel) {
		Level l = Level.toLevel(logLevel, Level.INFO);
		Configurator.setRootLevel(l);
		log.info("Log4j log level set to {}", l);
	}

}
