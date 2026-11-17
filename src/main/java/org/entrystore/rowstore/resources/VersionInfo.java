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

package org.entrystore.rowstore.resources;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;

@Component
public class VersionInfo {

	private static final Logger log = LoggerFactory.getLogger(VersionInfo.class);

	private static String VERSION = null;

	public String getVersion() {
		if (VERSION == null) {
			try (InputStream is = getClass().getClassLoader().getResourceAsStream("VERSION.txt")) {
				if (is != null) {
					try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
						VERSION = reader.readLine();
					}
				}
			} catch (IOException e) {
				log.error(e.getMessage());
			}
			if (VERSION == null) {
				VERSION = new SimpleDateFormat("yyyyMMdd").format(new Date());
			}
		}
		return VERSION;
	}

}
