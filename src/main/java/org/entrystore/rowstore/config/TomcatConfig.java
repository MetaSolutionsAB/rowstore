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

import org.apache.tomcat.util.buf.EncodedSolidusHandling;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TomcatConfig {

	/**
	 * Passes encoded slashes and backslashes ("%2F", "%5C") in request paths through undecoded, instead of rejecting
	 * them with Tomcat's HTML error page (400). They stay part of their path segment, so that e.g. "/dataset/a%2Fb" is
	 * an unknown dataset (404), as in 1.x, and "/dataset/x%2F..%2Fstatus" cannot reach another resource (QUERY-9.03).
	 */
	@Bean
	public WebServerFactoryCustomizer<TomcatServletWebServerFactory> encodedSolidusCustomizer() {
		return factory -> factory.addConnectorCustomizers(connector -> {
			connector.setEncodedSolidusHandling(EncodedSolidusHandling.PASS_THROUGH.getValue());
			connector.setEncodedReverseSolidusHandling(EncodedSolidusHandling.PASS_THROUGH.getValue());
		});
	}

}
