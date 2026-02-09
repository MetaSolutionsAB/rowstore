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

import jakarta.servlet.RequestDispatcher;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.boot.web.servlet.error.ErrorController;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class CustomErrorController implements ErrorController {

	@RequestMapping("/error")
	public ResponseEntity<String> handleError(HttpServletRequest request) {
		Object statusObj = request.getAttribute(RequestDispatcher.ERROR_STATUS_CODE);
		int statusCode = 404;
		if (statusObj != null) {
			statusCode = Integer.parseInt(statusObj.toString());
		}

		HttpStatus status = HttpStatus.resolve(statusCode);
		if (status == null) {
			status = HttpStatus.NOT_FOUND;
		}

		if (statusCode == 404) {
			return ResponseEntity.status(status)
					.body("You made a request against the RowStore REST API. There is no resource at this URI.");
		}

		return ResponseEntity.status(status).build();
	}

}
