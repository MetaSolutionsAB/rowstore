/*
 * Copyright (c) 2011-2015 MetaSolutions AB <info@metasolutions.se>
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

package org.entrystore.rowstore.etl;

/**
 * Status values for datasets to describe where in the ETL-pipeline they are.
 *
 * @author Hannes Ebner
 */
public class EtlStatus {

	public static final int CREATED = 0;

	public static final int ACCEPTED_DATA = 1;

	public static final int PROCESSING = 2;

	public static final int AVAILABLE = 3;

	public static final int ERROR = 4;

	public static String toString(int status) {
		switch (status) {
			case CREATED: return "Created";
			case ACCEPTED_DATA: return "Accepted data";
			case PROCESSING: return "Processing";
			case AVAILABLE: return "Available";
			case ERROR: return "Error";
			default: return "Unknown";
		}
	}

}