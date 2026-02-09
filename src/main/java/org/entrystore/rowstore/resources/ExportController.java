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
import org.entrystore.rowstore.store.RowStore;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashSet;
import java.util.Set;

@RestController
public class ExportController {

	private static final Logger log = LoggerFactory.getLogger(ExportController.class);

	private final RowStore rowStore;

	public ExportController(RowStore rowStore) {
		this.rowStore = rowStore;
	}

	@GetMapping(value = "/dataset/{id}/export", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<StreamingResponseBody> exportJSON(@PathVariable("id") String id) {
		Dataset dataset = rowStore.getDatasets().getDataset(id);
		if (dataset == null) {
			return new ResponseEntity<>(HttpStatus.NOT_FOUND);
		}

		if (dataset.getStatus() == EtlStatus.CREATED) {
			return new ResponseEntity<>(HttpStatus.FAILED_DEPENDENCY);
		}

		StreamingResponseBody body = outputStream -> {
			ResultSet rs = null;
			try (Writer writer = new BufferedWriter(new OutputStreamWriter(outputStream), 131072)) {
				writer.write("[");
				rs = dataset.streamAll();
				boolean first = true;
				while (rs.next()) {
					if (!first) {
						writer.write(",\n");
					}
					first = false;
					writer.write(rs.getString("data"));
					if ((rs.getRow() % 10000) == 0) {
						writer.flush();
					}
				}
				writer.write("]");
			} catch (SQLException e) {
				log.error(e.getMessage());
			} finally {
				cleanup(rs);
			}
		};

		HttpHeaders headers = new HttpHeaders();
		headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + id + ".json\"");
		headers.setContentType(MediaType.APPLICATION_JSON);
		return ResponseEntity.ok().headers(headers).body(body);
	}

	@GetMapping(value = "/dataset/{id}/export", produces = "text/csv")
	public ResponseEntity<StreamingResponseBody> exportCSV(@PathVariable("id") String id) {
		Dataset dataset = rowStore.getDatasets().getDataset(id);
		if (dataset == null) {
			return new ResponseEntity<>(HttpStatus.NOT_FOUND);
		}

		if (dataset.getStatus() == EtlStatus.CREATED) {
			return new ResponseEntity<>(HttpStatus.FAILED_DEPENDENCY);
		}

		StreamingResponseBody body = outputStream -> {
			Set<String> columnNames = new LinkedHashSet<>(dataset.getColumnNames());
			ResultSet rs = null;
			try (CSVWriter csvWriter = new CSVWriter(new BufferedWriter(new OutputStreamWriter(outputStream), 131072))) {
				csvWriter.writeNext(columnNames.toArray(new String[0]), false);
				rs = dataset.streamAll();
				while (rs.next()) {
					String data = rs.getString("data");
					csvWriter.writeNext(jsonObjectToStringArray(new JSONObject(data), columnNames), false);
					if ((rs.getRow() % 10000) == 0) {
						csvWriter.flush();
					}
				}
			} catch (SQLException e) {
				log.error(e.getMessage());
			} finally {
				cleanup(rs);
			}
		};

		HttpHeaders headers = new HttpHeaders();
		headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + id + ".csv\"");
		headers.setContentType(MediaType.parseMediaType("text/csv"));
		return ResponseEntity.ok().headers(headers).body(body);
	}

	private String[] jsonObjectToStringArray(JSONObject json, Set<String> keys) {
		Set<String> result = new LinkedHashSet<>();
		for (String key : keys) {
			result.add(json.getString(key));
		}
		return result.toArray(new String[0]);
	}

	private void cleanup(ResultSet rs) {
		if (rs == null) {
			return;
		}

		Statement statement = getQuietly(rs::getStatement);
		Connection connection = (statement != null) ? getQuietly(statement::getConnection) : null;

		closeQuietly(rs);
		closeQuietly(statement);
		closeQuietly(connection);
	}

	private <T> T getQuietly(SqlSupplier<T> supplier) {
		try {
			return supplier.get();
		} catch (SQLException e) {
			log.error(e.getMessage());
			return null;
		}
	}

	private void closeQuietly(AutoCloseable closeable) {
		if (closeable == null) {
			return;
		}
		try {
			closeable.close();
		} catch (Exception e) {
			log.error(e.getMessage());
		}
	}

	@FunctionalInterface
	private interface SqlSupplier<T> {
		T get() throws SQLException;
	}

}
