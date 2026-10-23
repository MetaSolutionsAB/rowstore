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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Selects the representation of a resource that has more than one (API-4.04). Like Restlet in 1.x, a request is never
 * answered with 406: if the Accept header accepts none of the representations, the first representation with the same
 * top-level type as an accepted media type is selected (e.g., text/plain selects text/html or text/csv), otherwise the
 * first representation. Resources with only one representation always respond with it, regardless of the Accept
 * header; their controllers set the Content-Type explicitly instead of using {@code produces}.
 */
final class ContentNegotiation {

	private ContentNegotiation() {
	}

	/**
	 * @param representations The media types of the representations, the default first.
	 * @return Returns the selected representation: the one with the highest quality in the Accept header; if several
	 * have the same quality, the one matched by the more specific media range (e.g., application/json over *&#47;*),
	 * then the earlier one.
	 */
	static MediaType select(HttpServletRequest request, MediaType... representations) {
		List<MediaType> accepted = acceptedMediaTypes(request);
		if (accepted.isEmpty()) {
			return representations[0];
		}

		MediaType best = null;
		double bestQuality = 0;
		int bestSpecificity = -1;
		for (MediaType representation : representations) {
			// the quality of a representation is that of the most specific media range that includes it
			double quality = 0;
			int specificity = -1;
			for (MediaType range : accepted) {
				if (range.includes(representation)) {
					int s = specificity(range);
					if (s > specificity || (s == specificity && range.getQualityValue() > quality)) {
						quality = range.getQualityValue();
						specificity = s;
					}
				}
			}
			if (quality > bestQuality || (quality > 0 && quality == bestQuality && specificity > bestSpecificity)) {
				best = representation;
				bestQuality = quality;
				bestSpecificity = specificity;
			}
		}
		if (best != null) {
			return best;
		}

		// none is acceptable: like Restlet, prefer a representation with the same top-level type
		List<MediaType> byQuality = new ArrayList<>(accepted);
		byQuality.sort(Comparator.comparingDouble(MediaType::getQualityValue).reversed());
		for (MediaType range : byQuality) {
			for (MediaType representation : representations) {
				if (range.getQualityValue() > 0 && !range.isWildcardType() &&
						range.getType().equalsIgnoreCase(representation.getType())) {
					return representation;
				}
			}
		}
		return representations[0];
	}

	private static int specificity(MediaType range) {
		if (range.isWildcardType()) {
			return 0;
		}
		return range.isWildcardSubtype() ? 1 : 2;
	}

	private static List<MediaType> acceptedMediaTypes(HttpServletRequest request) {
		List<String> headers = Collections.list(request.getHeaders(HttpHeaders.ACCEPT));
		if (headers.isEmpty()) {
			return List.of();
		}
		try {
			return MediaType.parseMediaTypes(headers);
		} catch (InvalidMediaTypeException e) {
			// an unparseable Accept header is treated like none, as by Restlet
			return List.of();
		}
	}

}
