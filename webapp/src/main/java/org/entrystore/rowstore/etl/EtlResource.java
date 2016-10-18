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

import org.entrystore.rowstore.store.Dataset;
import org.restlet.data.MediaType;

import java.io.File;

/**
 * Information container for resources to be submitted to the ETL-pipeline.
 *
 * @author Hannes Ebner
 */
public class EtlResource {

	private Dataset dataset;

	private File dataSource;

	private MediaType format;

	private boolean append;

	/**
	 * Creates a new EtlResource object.
	 *
	 * @param dataset The dataset to load data into.
	 * @param dataSource Where to load data from.
	 * @param format The format of the data at the dataSource.
	 * @param append If true, append to already existing data.
	 *                  If false, purge already existing data before
	 *                  loading new data into the dataset.
	 */
	public EtlResource(Dataset dataset, File dataSource, MediaType format, boolean append) {
		this.dataset = dataset;
		this.dataSource = dataSource;
		this.format = format;
		this.append = append;
	}

	Dataset getDataset() {
		return dataset;
	}

	File getDataSource() {
		return dataSource;
	}

	MediaType getFormat() {
		return format;
	}

	boolean isAppending() {
		return append;
	}

}