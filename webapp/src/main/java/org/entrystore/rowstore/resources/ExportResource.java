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

package org.entrystore.rowstore.resources;

import com.opencsv.CSVWriter;
import org.entrystore.rowstore.etl.EtlStatus;
import org.entrystore.rowstore.store.Dataset;
import org.entrystore.rowstore.store.QueryResult;
import org.json.JSONObject;
import org.restlet.data.Disposition;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StreamRepresentation;
import org.restlet.resource.Get;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Resource for dataset export/download.
 *
 * @author Hannes Ebner
 */
public class ExportResource extends BaseResource {

	static Logger log = LoggerFactory.getLogger(ExportResource.class);

	private Dataset dataset;

	private String datasetId;

	@Override
	public void doInit() {
		datasetId = (String) getRequest().getAttributes().get("id");
		if (datasetId != null) {
			try {
				dataset = getRowStore().getDatasets().getDataset(datasetId);
			} catch (IllegalStateException e) {
				log.error(e.getMessage());
				dataset = null;
			}
		}
	}

	@Get("csv")
	public Representation representCSV() {
		if (dataset == null) {
			getResponse().setStatus(Status.CLIENT_ERROR_NOT_FOUND);
			return null;
		}

		if (dataset.getStatus() == EtlStatus.CREATED) {
			getResponse().setStatus(Status.CLIENT_ERROR_FAILED_DEPENDENCY);
			return null;
		}

		StreamRepresentation result = new StreamRepresentation(MediaType.TEXT_CSV) {
			@Override
			public InputStream getStream() {
				// not needed
				return null;
			}

			@Override
			public void write(OutputStream outputStream) throws IOException {
                Set<String> columnNames = new LinkedHashSet<>(dataset.getColumnNames());
                int pageSize = getRowStore().getConfig().getExportPageSize();
                CSVWriter csvWriter = new CSVWriter(new OutputStreamWriter(outputStream));
                csvWriter.writeNext(columnNames.toArray(new String[0]), false);

                for (int page = 0; ; page++) {
					log.debug("Fetching dataset {}: offset {}, page size {}", datasetId, page * pageSize, pageSize);
                    QueryResult queryResult = dataset.query(Map.of(), pageSize, page * pageSize);
                    if (queryResult.getResultCount() > 0) {
                        for (JSONObject result : queryResult.getResults()) {
                            csvWriter.writeNext(jsonObjectToStringArray(result, columnNames), false);
							// We flush manually because we want to detect aborted connections.
							// Without flush() it seems we are forced to continue indefinitely.
							csvWriter.flush();
                        }
                    } else {
						log.debug("No more results for dataset {}, breaking", datasetId);
                        break;
                    }
                }

                csvWriter.close();
            }

		};

		Disposition disp = new Disposition();
		disp.setFilename(datasetId + ".csv");
		disp.setType(Disposition.TYPE_ATTACHMENT);
		result.setDisposition(disp);

		return result;
	}

	private String[] jsonObjectToStringArray(JSONObject json, Set<String> keys) {
		Set<String> result = new LinkedHashSet<>(); // we need to preserve order
		for (String key : keys) {
			result.add(json.getString(key));
		}
		return result.toArray(new String[keys.size()]);
	}

}