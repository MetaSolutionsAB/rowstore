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

package org.entrystore.rowstore.filters;

import org.apache.catalina.Globals;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ParameterParsingFilter}. Tomcat's parsing itself is covered by QueryParsingIT; here the
 * attribute that Tomcat sets on a parse failure is set directly.
 * Specification: QUERY-9.04
 */
@DisplayName("ParameterParsingFilter Tests")
class ParameterParsingFilterTest {

    @ParameterizedTest(name = "[{index}] {0} {1}, parse failed: {2} -> {3}")
    @CsvSource(nullValues = "null", value = {
            "GET,    null,                                  true,  400",
            "GET,    null,                                  false, 200",
            "HEAD,   null,                                  true,  400",
            "DELETE, null,                                  true,  400",
            "PUT,    application/x-www-form-urlencoded,     true,  400",
            "POST,   text/csv,                              true,  400",
            "POST,   null,                                  true,  400",
            // the parameters of form POSTs are not parsed, since that would consume the body
            "POST,   application/x-www-form-urlencoded,     true,  200",
            "POST,   multipart/form-data; boundary=x,       true,  200"
    })
    void rejectsRequestsWhoseParametersCouldNotBeParsed(String method, String contentType, boolean parseFailed,
                                                        int expectedStatus) throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest(method, "/dataset/x");
        request.setContentType(contentType);
        if (parseFailed) {
            request.setAttribute(Globals.PARAMETER_PARSE_FAILED_ATTR, Boolean.TRUE);
        }
        MockHttpServletResponse response = new MockHttpServletResponse();
        MockFilterChain chain = new MockFilterChain();
        new ParameterParsingFilter().doFilter(request, response, chain);

        assertThat(response.getStatus()).isEqualTo(expectedStatus);
        assertThat(chain.getRequest() != null).isEqualTo(expectedStatus == 200);
    }

}
