/*
 * Copyright (c) 2011-2016 MetaSolutions AB <info@metasolutions.se>
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

package org.entrystore.rowstore.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.entrystore.rowstore.RowStoreApplication;
import org.restlet.representation.Representation;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

/**
 * @author Hannes Ebner
 */
public class DatasetUtil {

	static Logger log = Logger.getLogger(DatasetUtil.class);

	public static String buildDatasetURL(String baseURL, String datasetId) {
		if (baseURL == null || datasetId == null) {
			throw new IllegalArgumentException("Arguments must not be null");
		}
		StringBuilder result = new StringBuilder(baseURL);
		if (!baseURL.endsWith("/")) {
			result.append("/");
		}
		result.append("dataset/");
		result.append(datasetId);
		return result.toString();
	}

	public static File writeTempFile(Representation entity) throws IOException {
		File tmpFile = File.createTempFile(RowStoreApplication.NAME, ".csv");
		tmpFile.deleteOnExit();
		log.info("Created temporary file " + tmpFile);

		if (tmpFile != null) {
			log.info("Writing request body to " + tmpFile);
			DatasetUtil.writeFile(entity.getStream(), tmpFile);
		}
		return tmpFile;
	}

	/**
	 * Writes an InputStream to a File.
	 *
	 * @param src Data source.
	 * @param dst Destination.
	 * @throws IOException
	 */
	private static void writeFile(InputStream src, File dst) throws IOException {
		if (src == null || dst == null) {
			throw new IllegalArgumentException("Parameters must not be null");
		}

		byte[] buffer = new byte[4096];
		FileOutputStream fos = null;
		try {
			fos = new FileOutputStream(dst);
			for (int length = 0; (length = src.read(buffer)) > 0; ) {
				fos.write(buffer, 0, length);
			}
		} finally {
			if (src != null) {
				try {
					src.close();
				} catch (IOException e) {
					log.error(e.getMessage());
				}
			}
			if (fos != null) {
				try {
					fos.close();
				} catch (IOException e) {
					log.error(e.getMessage());
				}
			}
		}
	}

	/**
	 * Tries to detect whether a given string contains a regular expression. Very
	 * simple heuristics as only the occurrence of the most typical characters in
	 * a regular expression is checked for.
	 *
	 * @param s The string to check.
	 * @return True if there is a chance that this string contains a regular expression.
	 */
	public static boolean isRegExpString(String s) {
		char[] indicators = {'^', '$', '(', '|', '[', '*', '+', '{', '?', '/'};
		return StringUtils.indexOfAny(s, indicators) > -1;
	}

	public static boolean isUUID(String string) {
		try {
			UUID.fromString(string);
			return true;
		} catch (Exception ex) {
			return false;
		}
	}

}