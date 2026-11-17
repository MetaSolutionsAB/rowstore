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
	 * Outcome of {@link #purgeDataset(String)}.
	 */
	enum PurgeResult {

		/**
		 * The dataset and its aliases were deleted; its data table is dropped in the background.
		 */
		PURGED,

		/**
		 * There is no dataset with the ID.
		 */
		NOT_FOUND,

		/**
		 * The dataset was not deleted because its status is neither AVAILABLE nor ERROR, e.g., because an upload is
		 * queued or being loaded.
		 */
		LOCKED

	}

	/**
	 * Deletes a dataset if its status is AVAILABLE or ERROR (API-3.07). The status check, the deletion of the
	 * dataset's row in the datasets table and the deletion of its aliases are one short transaction, which only takes
	 * row locks and therefore does not wait for readers of the data table, e.g., an export. From then on, the dataset
	 * does not exist for requests. Its data table (and shadow table, if any) is dropped afterwards by a job of the
	 * dataset in the ETL processor (ETL-12.01), or at the next startup (ETL-12.03).
	 *
	 * @param id The ID (UUID) of the dataset to be deleted, not an alias.
	 * @return Returns the outcome; the dataset was deleted only if it is {@link PurgeResult#PURGED}.
	 * @throws org.springframework.dao.DataAccessException If the database could not be accessed, e.g.,
	 * {@link org.springframework.dao.DataAccessResourceFailureException} if it is unreachable or no connection became
	 * available in time, or {@link org.springframework.dao.TransientDataAccessResourceException} for a lock or
	 * statement timeout (both 503); nothing is deleted then.
	 */
	PurgeResult purgeDataset(String id);

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