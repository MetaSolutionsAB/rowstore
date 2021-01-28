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

import com.ibm.icu.text.CharsetDetector;
import com.ibm.icu.text.CharsetMatch;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.entrystore.rowstore.RowStoreApplication;
import org.mozilla.universalchardet.UniversalDetector;
import org.restlet.representation.Representation;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
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
		Path tmpPath = Files.createTempFile(RowStoreApplication.NAME, ".csv");
		tmpPath.toFile().deleteOnExit();
		log.info("Writing request body to temporary file at " + tmpPath);
		Files.copy(entity.getStream(), tmpPath, StandardCopyOption.REPLACE_EXISTING);
		return tmpPath.toFile();
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

	public static Charset detectCharset(File f) throws IOException {
		byte[] data;
		try (InputStream is = Files.newInputStream(f.toPath())) {
			byte[] tmpData = new byte[16384]; // we try to read up to 16 kB
			int byteCount = is.read(tmpData);
			data = Arrays.copyOf(tmpData, byteCount);
			log.debug("Read " + byteCount + " bytes from " + f.getAbsolutePath() + " to detect charset");
		}

		UniversalDetector detector = new UniversalDetector(null);
		detector.handleData(data, 0, data.length);
		detector.dataEnd();
		String name = detector.getDetectedCharset();
		detector.reset();

		if (name != null) {
			log.debug("Detected charset " + name + " for file " + f.getAbsolutePath() + " using juniversalchardet");
		} else {
			CharsetDetector icuDetector = new CharsetDetector();
			icuDetector.setText(data);
			CharsetMatch match = icuDetector.detect();
			if (match != null) {
				name = match.getName();
				log.debug("Detected charset " + name + " for file " + f.getAbsolutePath() + " using ICU");
			}
		}

		if (name == null) {
			log.debug("Unable to detect charset for " + f.getAbsolutePath() + ", falling back to UTF-8");
			name = "UTF-8";
		}

		return Charset.forName(name);
	}

}