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

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.InvalidMediaTypeException;
import org.springframework.http.MediaType;
import org.springframework.util.MimeTypeUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Selects the representation of a resource that has more than one (API-4.04), as the flexible content negotiation of
 * Restlet did in 1.x ({@code FlexibleConneg}). A representation scores the highest quality of the media ranges that
 * include it, after the Accept header has been enriched with ranges of low quality: the parent of each range (e.g.,
 * text/* for text/html, with 0.005 + 0.001 &times; its quality), application/* with 0.002 (the parent of Restlet's
 * default media type application/octet-stream) and *&#47;* with 0.001, which replaces a *&#47;* sent by the client.
 * Consequently, a representation named by its media type or by type/* wins over one only matched by a range of the
 * same top-level type (e.g., text/html for text/csv), the quality decides between ranges of the same kind, and
 * *&#47;* never decides. Requests are never answered with 406. Resources with only one representation always
 * respond with it, regardless of the Accept header; their controllers set the Content-Type explicitly instead of
 * using {@code produces}.
 */
final class ContentNegotiation {

	private static final MediaType APPLICATION_ALL = new MediaType("application", "*");

	private ContentNegotiation() {
	}

	/**
	 * @param representations The media types of the representations, in the order in which they win ties.
	 * @return Returns the representation with the highest score; of several with the same score, the first one.
	 */
	static MediaType select(HttpServletRequest request, MediaType... representations) {
		List<Preference> preferences = preferences(request);
		MediaType best = representations[0];
		float bestScore = -1;
		for (MediaType representation : representations) {
			float score = -1;
			for (Preference preference : preferences) {
				if (includes(preference.range(), representation)) {
					score = Math.max(score, preference.quality());
				}
			}
			if (score > bestScore) {
				best = representation;
				bestScore = score;
			}
		}
		return best;
	}

	private record Preference(MediaType range, float quality) {
	}

	/**
	 * Returns the media ranges of the Accept header, enriched as by {@code FlexibleConneg.getEnrichedPreferences()}.
	 * Ranges with quality 0 still match (with quality 0), they are only not added as the parent of another range.
	 */
	private static List<Preference> preferences(HttpServletRequest request) {
		List<Preference> preferences = readAcceptHeaders(request);

		Set<MediaType> undesired = preferences.stream()
				.filter(p -> p.quality() == 0)
				.map(Preference::range)
				.collect(Collectors.toSet());
		// the list grows while it is iterated, so that the parents of added parents are added as well
		for (int i = 0; i < preferences.size(); i++) {
			Preference preference = preferences.get(i);
			MediaType parent = parent(preference.range());
			if (parent != null && !undesired.contains(parent)) {
				preferences.add(new Preference(parent, 0.005f + 0.001f * preference.quality()));
			}
		}
		if (!undesired.contains(MediaType.APPLICATION_OCTET_STREAM)) {
			preferences.add(new Preference(MediaType.APPLICATION_OCTET_STREAM, 0.003f));
			if (!undesired.contains(APPLICATION_ALL)) {
				preferences.add(new Preference(APPLICATION_ALL, 0.002f));
			}
		}
		preferences.removeIf(p -> p.range().isWildcardType());
		preferences.add(new Preference(MediaType.ALL, 0.001f));
		return preferences;
	}

	/**
	 * Reads the media ranges of the Accept headers as Restlet's {@code PreferenceReader}: the media range consists of
	 * the parameters before the first {@code q}, the parameters after it are accept extensions and ignored. A type
	 * without subtype (e.g., text or text/) is read as type/*. Empty ranges between commas are skipped. An invalid
	 * quality (not a number from 0 to 1, also if quoted, or {@code q} without value) or parameters without a media
	 * type (e.g., {@code ;q=0.5}) end the reading: the ranges read before are kept, the following ones and further
	 * Accept headers are ignored. Ranges that Spring cannot parse are skipped; Restlet read most of them into ranges
	 * that include none of the representations (an exception is a range with an unknown charset such as
	 * text/html;charset=bogus, which Restlet still matched with the low quality of its parent text/html).
	 */
	private static List<Preference> readAcceptHeaders(HttpServletRequest request) {
		List<Preference> preferences = new ArrayList<>();
		for (String header : Collections.list(request.getHeaders(HttpHeaders.ACCEPT))) {
			for (String token : MimeTypeUtils.tokenize(header)) {
				List<String> parts = splitParameters(token);
				String type = parts.getFirst().trim();
				if (type.isEmpty()) {
					if (parts.size() > 1) {
						return preferences;
					}
					continue;
				}
				StringBuilder range = new StringBuilder(withSubtype(type));
				float quality = 1;
				for (String parameter : parts.subList(1, parts.size())) {
					int equals = parameter.indexOf('=');
					if ((equals < 0 ? parameter : parameter.substring(0, equals)).trim().equals("q")) {
						quality = quality(equals < 0 ? null : parameter.substring(equals + 1).trim());
						if (quality < 0) {
							return preferences;
						}
						break;
					}
					range.append(';').append(parameter);
				}
				try {
					preferences.add(new Preference(MediaType.parseMediaType(range.toString()), quality));
				} catch (InvalidMediaTypeException e) {
					// skipped
				}
			}
		}
		return preferences;
	}

	/**
	 * Splits a range at the semicolons outside quoted strings: the media type, then its parameters.
	 */
	private static List<String> splitParameters(String token) {
		List<String> parts = new ArrayList<>();
		boolean quoted = false;
		int start = 0;
		for (int i = 0; i < token.length(); i++) {
			char c = token.charAt(i);
			if (quoted && c == '\\') {
				i++;
			} else if (c == '"') {
				quoted = !quoted;
			} else if (c == ';' && !quoted) {
				parts.add(token.substring(start, i));
				start = i + 1;
			}
		}
		parts.add(token.substring(start));
		return parts;
	}

	private static String withSubtype(String type) {
		int slash = type.indexOf('/');
		if (slash < 0) {
			return type + "/*";
		}
		return slash == type.length() - 1 ? type + "*" : type;
	}

	/**
	 * @return Returns the quality, or -1 if it is invalid (as by {@code PreferenceReader.readQuality()}).
	 */
	private static float quality(String value) {
		if (value == null) {
			return -1;
		}
		if (value.length() >= 2 && value.startsWith("\"") && value.endsWith("\"")) {
			value = value.substring(1, value.length() - 1);
		}
		try {
			float quality = Float.parseFloat(value);
			return quality >= 0 && quality <= 1 ? quality : -1;
		} catch (NumberFormatException e) {
			return -1;
		}
	}

	/**
	 * As {@code MediaType.getParent()} of Restlet: the range without parameters, otherwise type/* or *&#47;*.
	 */
	private static MediaType parent(MediaType range) {
		if (!range.getParameters().isEmpty()) {
			return new MediaType(range.getType(), range.getSubtype());
		}
		if (range.isWildcardType()) {
			return null;
		}
		return "*".equals(range.getSubtype()) ? MediaType.ALL : new MediaType(range.getType(), "*");
	}

	/**
	 * As {@code MediaType.includes(Metadata, false)} of Restlet: if the subtypes are equal, the parameters of the range
	 * must be those of the representation; type/*+suffix also includes type/suffix (e.g., application/*+json includes
	 * application/json).
	 */
	private static boolean includes(MediaType range, MediaType representation) {
		if (range.isWildcardType()) {
			return true;
		}
		if (!range.getType().equals(representation.getType())) {
			return false;
		}
		String subtype = range.getSubtype();
		if (subtype.equals(representation.getSubtype())) {
			for (Map.Entry<String, String> parameter : range.getParameters().entrySet()) {
				if (!parameter.getValue().equals(representation.getParameter(parameter.getKey()))) {
					return false;
				}
			}
			return true;
		}
		return "*".equals(subtype) || (subtype.startsWith("*+") && representation.getSubtype().endsWith(subtype.substring(2)));
	}

}
