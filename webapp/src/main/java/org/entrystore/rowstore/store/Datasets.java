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

package org.entrystore.rowstore.store;

import java.util.Set;

/**
 * @author Hannes Ebner
 */
public interface Datasets {

	/**
	 * @return Returns dataset instances of all available datasets. The datasets are lazy-loaded for performance reasons.
	 */
	Set<Dataset> getAll();

	/**
	 * Creates an empty dataset.
	 *
	 * @return Returns a dataset instance.
	 */
	Dataset createDataset();

	/**
	 * Deletes a dataset.
	 *
	 * @param id The ID of the dataset to be deleted.
	 * @return Returns true of successful.
	 */
	boolean purgeDataset(String id);

	/**
	 * Loads a dataset.
	 *
	 * @param id The ID of the dataset to be returned.
	 * @return Returns a dataset instance.
	 */
	Dataset getDataset(String id);

	/**
	 * Checks whether a dataset exists.
	 *
	 * @param id The ID of the dataset to be checked.
	 * @return Returns true if dataset exists.
	 */
	boolean hasDataset(String id);

	/**
	 * @return Returns the total amount of datasets.
	 */
	int amount();

	/**
	 * @return Returns a unique (per RowStore instance) dataset ID.
	 */
	String createUniqueDatasetId();

}