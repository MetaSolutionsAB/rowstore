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
	 * removes leading and trailing whitespace and control characters, and converts it to lower case. Removed are the
	 * characters up to U+0020 (as by {@code String.trim()}, like in 1.x) and Unicode whitespace (as by
	 * {@code String.strip()}); no-break spaces (e.g., U+00A0) are kept.
	 *
	 * @return Returns the normalized label, which is empty if the column is to be skipped.
	 */
	public static String normalize(String label) {
		return trim(removeInvisibleCharacters(label)).toLowerCase();
	}

	/**
	 * Returns the name that a column name stored before 2.0 gets if the byte order mark and the soft hyphens of the CSV
	 * header it was stored from had not been there (ETL-11.02). Before 2.0, a byte order mark at the beginning of the
	 * file and soft hyphens were kept in the column names, and the byte order mark in front of the first label changed
	 * how the label was parsed, since the label did not start with a quote:
	 * <ul>
	 * <li>The default parser (RFC 4180) kept the enclosing quotes but decoded doubled quotes: the header
	 * {@code <BOM>"kopare_id"} was stored as {@code <BOM>"kopare_id"}, {@code <BOM>"a""""b"} as {@code <BOM>"a""b"}
	 * (now {@code a""b}), {@code <BOM>"""x"""} as {@code <BOM>""x""} (now {@code "x"}). The name is the stored name
	 * without the byte order mark and the enclosing quotes; a single quote (from {@code <BOM>""}) becomes empty.</li>
	 * <li>The legacy parser removed the quotes, but kept one quote too many if the label started with a doubled quote
	 * inside the quotes: {@code <BOM>"""x"""} was stored as {@code <BOM>""x"} (now {@code "x"}), {@code <BOM>""} as
	 * {@code <BOM>"} (now empty). The name is the stored name without the byte order mark and its first quote.</li>
	 * </ul>
	 * A name of the form {@code <BOM>"x..."} whose second character after the byte order mark is not a quote is
	 * unquoted like by the default parser regardless of the configured parser: it is the common case of a plain quoted
	 * label stored by the default parser (e.g., if the legacy parser was configured after loading the dataset), whereas
	 * the legacy parser stored it only for unlikely headers like {@code ""x""}.
	 * This only applies if the stored name starts with a byte order mark directly followed by a quote. Otherwise, and
	 * afterwards, byte order marks and soft hyphens are removed anywhere in the name, and the name is trimmed like 1.x
	 * trimmed a label ({@code String.trim()}), since the byte order mark or soft hyphen prevented that. Names that
	 * merely look quoted are not unquoted (the header {@code """x"""} yields the name {@code "x"}), and neither are
	 * names with whitespace between the byte order mark and the quote (the header {@code <BOM> "x"} yields {@code "x"}
	 * with the default parser). Some stored names are ambiguous between the parsers or between raw headers; the result
	 * is the name of the most likely header for the given parser.
	 * <p>
	 * A name without a byte order mark and without soft hyphens is returned as it is, even if a fresh import would
	 * store it differently (ETL-11.08), e.g., with surrounding whitespace (stored before 1.2), upper case (stored before
	 * 1.0) or surrounding Unicode whitespace (1.x only trimmed whitespace up to U+0020). The cleaned name of a clean
	 * name is the name itself. A cleaned name may be empty (e.g., for the stored name {@code <BOM>}).
	 *
	 * @param legacyParser Whether the legacy parser is configured.
	 */
	public static String cleanStoredName(String name, boolean legacyParser) {
		if (name.indexOf(BYTE_ORDER_MARK) < 0 && name.indexOf(SOFT_HYPHEN) < 0) {
			return name;
		}
		String result = name;
		if (name.length() >= 2 && name.charAt(0) == BYTE_ORDER_MARK && name.charAt(1) == '"') {
			String quoted = name.substring(1);
			if (quoted.length() >= 3 && quoted.charAt(1) != '"' && quoted.endsWith("\"")) {
				// the common case of a plain quoted label stored by the default parser, also if the legacy parser is
				// configured now; the legacy parser stored such a name only for unlikely headers like ""x""
				result = quoted.substring(1, quoted.length() - 1);
			} else if (legacyParser) {
				result = quoted.substring(1);
			} else if (quoted.length() >= 2 && quoted.endsWith("\"")) {
				result = quoted.substring(1, quoted.length() - 1);
			} else if (quoted.length() == 1) {
				result = "";
			}
		}
		return removeInvisibleCharacters(result).trim();
	}

	/**
	 * Determines the renames needed to clean stored column names, see {@link #cleanStoredName(String, boolean)}. A
	 * name whose clean name would be empty, or would be empty as a label (see {@link #normalize(String)}), is not
	 * renamed and is kept as it is (ETL-11.09), since a fresh import would skip the column.
	 *
	 * @param names The stored column names of a dataset.
	 * @param legacyParser Whether the legacy parser is configured.
	 * @return Returns the renames (stored name to clean name) in iteration order of the names, an empty map if all
	 * names are clean, or null if the names cannot be cleaned because a clean name would occur more than once.
	 */
	public static Map<String, String> renamesForStoredNames(Collection<String> names, boolean legacyParser) {
		Map<String, String> renames = new LinkedHashMap<>();
		Set<String> cleanNames = new HashSet<>();
		for (String name : names) {
			String clean = cleanStoredName(name, legacyParser);
			if (normalize(clean).isEmpty()) {
				clean = name;
			}
			if (!cleanNames.add(clean)) {
				return null;
			}
			if (!clean.equals(name)) {
				renames.put(name, clean);
			}
		}
		return renames;
	}

	/**
	 * The column names that an append to an existing dataset uses for the labels of its CSV header, see
	 * {@link #forAppend(Collection, Collection)}.
	 *
	 * @param renames The stored names to be renamed before the append (stored name to clean name), see
	 *                {@link #cleanStoredName(String, boolean)}.
	 * @param namesByLabel The column name to store the values of each label under: the clean stored name.
	 */
	public record AppendNames(Map<String, String> renames, Map<String, String> namesByLabel) {
	}

	/**
	 * Determines the column names that an append uses for the labels of its CSV header (ETL-11.04). Each label
	 * matches the stored name whose clean name (see {@link #cleanStoredName(String, boolean)}, with one of the parsers)
	 * is the label when normalized as a label (see {@link #normalize(String)}); the values are stored under the clean
	 * name. So stored names with a byte order mark or soft hyphens are renamed by the append, and stored names that
	 * only differ from the labels in what the cleanup does not change (e.g., surrounding Unicode whitespace) are kept.
	 * Stored names that are empty as a label (e.g., {@code <BOM>}) match no label and are ignored: the rows of the
	 * append do not have them, like the rows of a fresh import of the same CSV file.
	 *
	 * @param names The stored column names of a dataset.
	 * @param labels The normalized labels of the CSV header, which must not be empty.
	 * @return Returns the names for the labels, or null if the stored names do not match the labels one to one.
	 */
	public static AppendNames forAppend(Collection<String> names, Collection<String> labels) {
		Set<String> labelSet = new HashSet<>(labels);
		Map<String, String> renames = new LinkedHashMap<>();
		Map<String, String> namesByLabel = new LinkedHashMap<>();
		for (String name : names) {
			// label to clean name; the default parser's clean name is preferred if both parsers yield the same label
			Map<String, String> candidates = new LinkedHashMap<>();
			boolean emptyLabel = true;
			for (boolean legacyParser : new boolean[]{false, true}) {
				String clean = cleanStoredName(name, legacyParser);
				String label = normalize(clean);
				emptyLabel &= label.isEmpty();
				if (labelSet.contains(label)) {
					candidates.putIfAbsent(label, clean);
				}
			}
			if (emptyLabel) {
				continue;
			}
			if (candidates.size() != 1) {
				return null;
			}
			Map.Entry<String, String> match = candidates.entrySet().iterator().next();
			if (namesByLabel.putIfAbsent(match.getKey(), match.getValue()) != null) {
				return null;
			}
			if (!match.getValue().equals(name)) {
				renames.put(name, match.getValue());
			}
		}
		return namesByLabel.keySet().equals(labelSet) ? new AppendNames(renames, namesByLabel) : null;
	}

	/**
	 * @return Returns the name with byte order marks and soft hyphens written as Unicode escapes, for logging.
	 */
	public static String escapeForLog(String name) {
		return name.replace(String.valueOf(BYTE_ORDER_MARK), "\\uFEFF").replace(String.valueOf(SOFT_HYPHEN), "\\u00AD");
	}

	private static String trim(String s) {
		int begin = 0;
		int end = s.length();
		while (begin < end && isTrimmed(s.charAt(begin))) {
			begin++;
		}
		while (end > begin && isTrimmed(s.charAt(end - 1))) {
			end--;
		}
		return s.substring(begin, end);
	}

	private static boolean isTrimmed(char c) {
		return c <= ' ' || Character.isWhitespace(c);
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
