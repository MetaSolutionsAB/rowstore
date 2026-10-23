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

package org.entrystore.rowstore.util;

import com.ibm.icu.text.CharsetDetector;
import com.ibm.icu.text.CharsetMatch;
import org.apache.commons.lang3.StringUtils;
import org.entrystore.rowstore.RowStoreApplication;
import org.entrystore.rowstore.store.impl.SqlExceptionLogUtil;
import org.mozilla.universalchardet.UniversalDetector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.UUID;

/**
 * @author Hannes Ebner
 */
public class DatasetUtil {

	static Logger log = LoggerFactory.getLogger(DatasetUtil.class);

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

	public static File writeTempFile(InputStream inputStream) throws IOException {
		return writeTempFile(inputStream, -1);
	}

	public static File writeTempFile(InputStream inputStream, long maxSize) throws IOException {
		Path tmpPath = Files.createTempFile(RowStoreApplication.NAME, ".csv");
		tmpPath.toFile().deleteOnExit();
		log.info("Writing request body to temporary file at " + tmpPath);
		if (maxSize > 0) {
			Files.copy(new BoundedInputStream(inputStream, maxSize), tmpPath, StandardCopyOption.REPLACE_EXISTING);
		} else {
			Files.copy(inputStream, tmpPath, StandardCopyOption.REPLACE_EXISTING);
		}
		return tmpPath.toFile();
	}

	public static class PayloadTooLargeException extends IOException {
		public PayloadTooLargeException(long maxSize) {
			super("Upload exceeds maximum allowed size of " + maxSize + " bytes");
		}
	}

	private static class BoundedInputStream extends InputStream {
		private final InputStream delegate;
		private final long maxSize;
		private long bytesRead = 0;

		BoundedInputStream(InputStream delegate, long maxSize) {
			this.delegate = delegate;
			this.maxSize = maxSize;
		}

		@Override
		public int read() throws IOException {
			int b = delegate.read();
			if (b != -1 && ++bytesRead > maxSize) {
				throw new PayloadTooLargeException(maxSize);
			}
			return b;
		}

		@Override
		public int read(byte[] buf, int off, int len) throws IOException {
			int n = delegate.read(buf, off, len);
			if (n > 0) {
				bytesRead += n;
				if (bytesRead > maxSize) {
					throw new PayloadTooLargeException(maxSize);
				}
			}
			return n;
		}

		@Override
		public void close() throws IOException {
			delegate.close();
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

	public static Charset detectCharset(File f) throws IOException {
		byte[] data;
		try (InputStream is = Files.newInputStream(f.toPath())) {
			byte[] tmpData = new byte[524288]; // we try to read up to 512 kB
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

	/**
	 * Opens a reader that decodes a file with a charset and skips a byte order mark (U+FEFF) at the beginning of the
	 * file. Java's decoders keep a byte order mark, except for the charsets that are defined to consume it (e.g.,
	 * "UTF-16", but not "UTF-8", "UTF-16BE" or "UTF-16LE", which charset detection returns). Malformed input is
	 * reported as an exception, like by Files.newBufferedReader(). The reader is not buffered.
	 */
	public static Reader newReaderSkippingBom(Path path, Charset charset) throws IOException {
		PushbackReader reader = new PushbackReader(new InputStreamReader(Files.newInputStream(path), charset.newDecoder()));
		try {
			int first = reader.read();
			if (first != -1 && first != ColumnNames.BYTE_ORDER_MARK) {
				reader.unread(first);
			}
			return reader;
		} catch (IOException | RuntimeException e) {
			reader.close();
			throw e;
		}
	}

	public static void closeStatement(Statement stmt) {
		if (stmt == null) {
			return;
		}
		try {
			/*
			if (stmt instanceof PreparedStatement) {
				PreparedStatement ps = (PreparedStatement) stmt;
				ps.clearParameters();
			}
			stmt.clearBatch();
			stmt.clearWarnings();
			 */
			stmt.close();
		} catch (SQLException e) {
			SqlExceptionLogUtil.error(log, e);
		}
	}

}