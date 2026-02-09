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

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.util.Date;
import java.util.Map;
import java.util.Set;

/**
 * @author Hannes Ebner
 */
public interface Dataset {

	/**
	 * @return Returns the ID of the dataset.
	 */
	String getId();

	/**
	 * @return Returns the status of the dataset.
	 * @see org.entrystore.rowstore.etl.EtlStatus
	 */
	int getStatus();

	/**
	 * Sets the status of the dataset.
	 * @param status
	 * @see org.entrystore.rowstore.etl.EtlStatus
	 */
	void setStatus(int status);

	/**
	 * @return Returns the Date and Time of when the dataset was created.
	 */
	Date getCreationDate();

	/**
	 * Populates the dataset with data from a CSV file, i.e., reads a CSV file, converts the rows into JSON and loads in the DB backend.
	 *
	 * @param csvFile CSV file that fulfills RowStore's requirements, see official documentation.
	 * @param append If true, appends data to already existing dataset. If false, the data of an eventually existing dataset is replaced.
	 * @return Returns true if successful.
	 * @throws IOException
	 */
	boolean populate(File csvFile, boolean append) throws IOException;

	/**
	 * Returns matching rows of the dataset.
	 *
	 * @param tuples Key/value pairs where the keys must match the row names. Returns all data if the map is emtpy. Keys are treated case-insensitively.
	 * @return Returns a list of matching JSON objects.
	 */
	QueryResult query(Map<String, String> tuples, int limit, int offset);

	/**
	 * @return Returns a ResultSet containing all data of the dataset's table. ResultSet and the underlying Statement and Connection need to be closed manually after the ResultSet is consumed.
	 */
	ResultSet streamAll();

	/**
	 * @return Returns the dataset's column names.
	 */
	Set<String> getColumnNames();

	/**
	 * @return Returns the size (amount of rows) of the dataset.
	 */
	long getRowCount();

	/**
	 * Returns all aliases of the dataset.
	 *
	 * @return A set of strings corresponding an alias.
	 */
	Set<String> getAliases();

	/**
	 * Sets the aliases under which a dataset should be reachable.
	 *
	 * @param aliases A set of strings with aliases.
	 */
	boolean setAliases(Set<String> aliases);

	int REGEXP_QUERY_DISABLED = 0;

	int REGEXP_QUERY_SIMPLE = 1;

	int REGEXP_QUERY_FULL = 2;

}