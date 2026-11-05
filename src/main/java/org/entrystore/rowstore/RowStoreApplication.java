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

package org.entrystore.rowstore;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Map;

@SpringBootApplication
public class RowStoreApplication {

	public static final String NAME = "RowStore";

	/**
	 * Options of the 1.x standalone (RowStoreApplicationStandalone) and their replacements in 2.0 (CFG-7.05).
	 */
	private static final Map<String, String> REMOVED_OPTIONS = Map.of(
			"-c", "--rowstore.config.uri=<URI>",
			"--config", "--rowstore.config.uri=<URI>",
			"-p", "--server.port=<PORT>",
			"--port", "--server.port=<PORT>",
			"-l", "\"loglevel\" in rowstore.json",
			"--log-level", "\"loglevel\" in rowstore.json",
			"--connector-params", "\"maxQueryConnections\" in rowstore.json (connections instead of threads are limited)",
			"-h", "the README",
			"--help", "the README");

	public static void main(String[] args) {
		String error = checkArguments(args);
		if (error != null) {
			// logging is not configured yet
			System.err.println(error);
			System.exit(1);
		}
		SpringApplication.run(RowStoreApplication.class, args);
	}

	/**
	 * Checks the command line arguments. Spring Boot ignores arguments that are not of the form {@code --name=value},
	 * so options of 1.x (e.g., {@code -p 8080}) would be ignored silently and RowStore would start with defaults
	 * (CFG-7.05).
	 *
	 * @return Returns an error message naming the first unsupported argument, or null if all arguments are supported.
	 */
	static String checkArguments(String[] args) {
		for (String arg : args) {
			String name = arg.contains("=") ? arg.substring(0, arg.indexOf('=')) : arg;
			String replacement = REMOVED_OPTIONS.get(name);
			if (replacement != null) {
				return "Unsupported argument '" + arg + "': the options of RowStore 1.x were removed, use " + replacement +
						" instead";
			}
			if (!arg.startsWith("--") || arg.length() == 2) {
				return "Unsupported argument '" + arg + "': RowStore only takes options of the form --name=value, " +
						"e.g. --rowstore.config.uri=file:///path/to/rowstore.json and --server.port=8282";
			}
		}
		return null;
	}

}
