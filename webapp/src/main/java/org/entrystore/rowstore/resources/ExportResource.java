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
import org.json.JSONObject;
import org.restlet.data.Disposition;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StreamRepresentation;
import org.restlet.resource.Get;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Resource for dataset export/download.
 *
 * <p>The JSON (from the DB) and CSV handling (for serialization) required for the CSV export takes some time, that's why we use
 * multiple threads to saturate the output stream as much as possible. This is not needed for the JSON export since we can
 * write the data we get from the DB pretty much without changes.</p>
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

	@Get("json")
	public Representation representJSON() {
		if (dataset == null) {
			getResponse().setStatus(Status.CLIENT_ERROR_NOT_FOUND);
			return null;
		}

		if (dataset.getStatus() == EtlStatus.CREATED) {
			getResponse().setStatus(Status.CLIENT_ERROR_FAILED_DEPENDENCY);
			return null;
		}

		StreamRepresentation result = new StreamRepresentation(MediaType.APPLICATION_JSON) {

			@Override
			public InputStream getStream() {
				// not needed
				return null;
			}

			@Override
			public void write(OutputStream outputStream) {
				try (Writer writer = new BufferedWriter(new OutputStreamWriter(outputStream), 131072)) {
					writer.write("[");
					ResultSet rs = dataset.streamAll();
					try {
						while (rs.next()) {
							writer.write(rs.getString("data"));
							if (!rs.isLast()) {
								writer.write(",\n");
							}
							// we flush manually because we want to detect aborted connections early
							if ((rs.getRow() % 10000) == 0) {
								writer.flush();
							}
						}
					} catch (SQLException e) {
						log.error(e.getMessage());
					} finally {
						cleanup(rs);
					}

					writer.write("]");
				} catch (IOException ioe) {
					log.error(ioe.getMessage());
				}
			}

		};

		Disposition disp = new Disposition();
		disp.setFilename(datasetId + ".json");
		result.setDisposition(disp);

		return result;
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
			public void write(OutputStream outputStream) {
				Set<String> columnNames = new LinkedHashSet<>(dataset.getColumnNames());
				try (CSVWriter csvWriter = new CSVWriter(new BufferedWriter(new OutputStreamWriter(outputStream), 131072))) {
					csvWriter.writeNext(columnNames.toArray(new String[0]), false);
					ResultSet rs = dataset.streamAll();

					ExecutorService es = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

					Runnable csvWritingRunnable = () -> {
						try {
							while (true) {
								String data = null;
								synchronized (rs) {
									if (rs.next()) {
										data = rs.getString("data");
									} else {
										break;
									}
								}
								csvWriter.writeNext(jsonObjectToStringArray(new JSONObject(data), columnNames), false);
								// we flush manually because we want to detect aborted connections early
								if ((rs.getRow() % 10000) == 0) {
									csvWriter.flush();
								}
							}
						} catch (SQLException | IOException e) {
							log.error(e.getMessage());
						}
                    };

					List<Callable<Object>> writingThreads = new ArrayList<>();
					for (int i = 0; i < Runtime.getRuntime().availableProcessors(); i++) {
						writingThreads.add(Executors.callable(csvWritingRunnable));
					}

					es.invokeAll(writingThreads);
					es.shutdown();
					cleanup(rs);
				} catch (IOException | InterruptedException e) {
					log.error(e.getMessage());
				}
            }

		};

		Disposition disp = new Disposition();
		disp.setFilename(datasetId + ".csv");
		result.setDisposition(disp);

		return result;
	}

	private String[] jsonObjectToStringArray(JSONObject json, Set<String> keys) {
		Set<String> result = new LinkedHashSet<>(); // we need to preserve order
		for (String key : keys) {
			result.add(json.getString(key));
		}
		return result.toArray(new String[0]);
	}

	private void cleanup(ResultSet rs) {
		if (rs == null) {
			throw new IllegalArgumentException("ResultSet must not be null");
		}

		Statement statement = null;
		try {
			statement = rs.getStatement();
		} catch (SQLException e) {
			log.error(e.getMessage());
		}

		Connection connection = null;
		try {
			if (statement != null) {
				connection = statement.getConnection();
			}
		} catch (SQLException e) {
			log.error(e.getMessage());
		}

		try {
			rs.close();
		} catch (SQLException e) {
			log.error(e.getMessage());
		}

		try {
			if (statement != null) {
				statement.close();
			}
		} catch (SQLException e) {
			log.error(e.getMessage());
		}

		try {
			if (connection != null) {
				connection.close();
			}
		} catch (SQLException e) {
			log.error(e.getMessage());
		}
	}

}