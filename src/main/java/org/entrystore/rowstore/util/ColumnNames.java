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

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Normalization of column names, see ETL-4.04, ETL-4.06 and ETL-11.
 *
 * @author Hannes Ebner
 */
public final class ColumnNames {

	public static final char BYTE_ORDER_MARK = '\uFEFF';

	public static final char SOFT_HYPHEN = '\u00AD';

	private ColumnNames() {
	}

	/**
	 * Normalizes a column label of a CSV header: removes byte order marks and soft hyphens anywhere in the label,
	 * strips leading and trailing whitespace and converts it to lower case.
	 *
	 * @return Returns the normalized label, which is empty if the column is to be skipped.
	 */
	public static String normalize(String label) {
		return removeInvisibleCharacters(label).strip().toLowerCase();
	}

	/**
	 * Returns the name that a column name stored before 2.0 has if its CSV header is loaded now. Before 2.0, a byte
	 * order mark at the beginning of the file and soft hyphens were kept in the column names. With the default parser,
	 * a byte order mark in front of a quoted first label also kept the quotes, since the label did not start with a
	 * quote: {@code <BOM>"a""b"} was stored as {@code <BOM>"a""b"}, whereas the label is now parsed as {@code a"b}.
	 * Therefore a name starting with a byte order mark is unquoted, other names that merely look quoted are not (the
	 * header {@code """x"""} yields the name {@code "x"}). The result of a clean name is the name itself.
	 */
	public static String cleanStoredName(String name) {
		String result = removeInvisibleCharacters(name).strip();
		if (!name.isEmpty() && name.charAt(0) == BYTE_ORDER_MARK && result.length() >= 2 && result.startsWith("\"") && result.endsWith("\"")) {
			result = result.substring(1, result.length() - 1).replace("\"\"", "\"");
		}
		return normalize(result);
	}

	/**
	 * Determines the renames needed to clean stored column names, see {@link #cleanStoredName(String)}.
	 *
	 * @param names The stored column names of a dataset.
	 * @return Returns the renames (stored name to clean name) in iteration order of the names, an empty map if all
	 * names are clean, or null if the names cannot be cleaned because a clean name would be empty or would occur
	 * more than once.
	 */
	public static Map<String, String> renamesForStoredNames(Collection<String> names) {
		Map<String, String> renames = new LinkedHashMap<>();
		Set<String> cleanNames = new HashSet<>();
		for (String name : names) {
			String clean = cleanStoredName(name);
			if (clean.isEmpty() || !cleanNames.add(clean)) {
				return null;
			}
			if (!clean.equals(name)) {
				renames.put(name, clean);
			}
		}
		return renames;
	}

	/**
	 * @return Returns the name with byte order marks and soft hyphens written as Unicode escapes, for logging.
	 */
	public static String escapeForLog(String name) {
		return name.replace(String.valueOf(BYTE_ORDER_MARK), "\\uFEFF").replace(String.valueOf(SOFT_HYPHEN), "\\u00AD");
	}

	private static String removeInvisibleCharacters(String s) {
		if (s.indexOf(BYTE_ORDER_MARK) < 0 && s.indexOf(SOFT_HYPHEN) < 0) {
			return s;
		}
		StringBuilder sb = new StringBuilder(s.length());
		for (int i = 0; i < s.length(); i++) {
			char c = s.charAt(i);
			if (c != BYTE_ORDER_MARK && c != SOFT_HYPHEN) {
				sb.append(c);
			}
		}
		return sb.toString();
	}

}
