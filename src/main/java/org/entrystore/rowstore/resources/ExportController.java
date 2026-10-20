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

package org.entrystore.rowstore.resources;

import com.opencsv.CSVWriter;
import jakarta.servlet.http.HttpServletRequest;
import org.entrystore.rowstore.etl.EtlStatus;
import org.entrystore.rowstore.store.Dataset;
import org.entrystore.rowstore.store.RowStore;
import org.entrystore.rowstore.store.RowStream;
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
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.context.request.async.CallableProcessingInterceptor;
import org.springframework.web.context.request.async.WebAsyncUtils;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Streams a dataset as JSON or CSV.
 * <p>
 * The row stream is opened before the response is committed, so that a database failure at that point results
 * in HTTP 500. Once the first bytes have been sent, status and headers cannot be changed anymore: a failure while
 * streaming aborts the connection instead (the writers are deliberately not closed in that case, because closing
 * them would terminate the chunked response cleanly and make a truncated export look complete).
 */
@RestController
public class ExportController {

	private static final Logger log = LoggerFactory.getLogger(ExportController.class);

	private final RowStore rowStore;

	public ExportController(RowStore rowStore) {
		this.rowStore = rowStore;
	}

	@GetMapping(value = "/dataset/{id}/export", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<StreamingResponseBody> exportJSON(@PathVariable("id") String id, HttpServletRequest request) {
		Dataset dataset = rowStore.getDatasets().getDataset(id);
		if (dataset == null) {
			return new ResponseEntity<>(HttpStatus.NOT_FOUND);
		}

		if (dataset.getStatus() == EtlStatus.CREATED) {
			return new ResponseEntity<>(HttpStatus.FAILED_DEPENDENCY);
		}

		RowStream stream = openRowStream(dataset, request);
		if (stream == null) {
			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
		}

		StreamingResponseBody body = outputStream -> {
			try (stream) {
				Writer writer = new BufferedWriter(new OutputStreamWriter(outputStream), 131072);
				writer.write("[");
				ResultSet rs = stream.rs();
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
				writer.flush();
			} catch (SQLException | RuntimeException e) {
				throw abortExport(id, e);
			}
		};

		HttpHeaders headers = new HttpHeaders();
		headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + id + ".json\"");
		headers.setContentType(MediaType.APPLICATION_JSON);
		return ResponseEntity.ok().headers(headers).body(body);
	}

	@GetMapping(value = "/dataset/{id}/export", produces = "text/csv")
	public ResponseEntity<StreamingResponseBody> exportCSV(@PathVariable("id") String id, HttpServletRequest request) {
		Dataset dataset = rowStore.getDatasets().getDataset(id);
		if (dataset == null) {
			return new ResponseEntity<>(HttpStatus.NOT_FOUND);
		}

		if (dataset.getStatus() == EtlStatus.CREATED) {
			return new ResponseEntity<>(HttpStatus.FAILED_DEPENDENCY);
		}

		// one list for header and cells guarantees the same column order in both
		List<String> columnNames = new ArrayList<>(dataset.getColumnNames());

		RowStream stream = openRowStream(dataset, request);
		if (stream == null) {
			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
		}

		StreamingResponseBody body = outputStream -> {
			try (stream) {
				CSVWriter csvWriter = new CSVWriter(new BufferedWriter(new OutputStreamWriter(outputStream), 131072));
				csvWriter.writeNext(columnNames.toArray(new String[0]), false);
				ResultSet rs = stream.rs();
				while (rs.next()) {
					String data = rs.getString("data");
					csvWriter.writeNext(toCsvRow(new JSONObject(data), columnNames), false);
					if ((rs.getRow() % 10000) == 0) {
						csvWriter.flush();
					}
				}
				csvWriter.flush();
			} catch (SQLException | RuntimeException e) {
				throw abortExport(id, e);
			}
		};

		HttpHeaders headers = new HttpHeaders();
		headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + id + ".csv\"");
		headers.setContentType(MediaType.parseMediaType("text/csv"));
		return ResponseEntity.ok().headers(headers).body(body);
	}

	/**
	 * Opens the row stream of a dataset. The stream is closed by the response body after streaming. It is also
	 * closed when the async request completes, which covers the case that the body is never invoked (e.g. if the
	 * async task is cancelled due to a timeout or a client disconnect before it started); closing twice is a no-op.
	 *
	 * @return Returns the open stream, or null if it could not be opened.
	 */
	private RowStream openRowStream(Dataset dataset, HttpServletRequest request) {
		RowStream stream;
		try {
			stream = dataset.streamAll();
		} catch (SQLException e) {
			log.error("Unable to read dataset {} for export", dataset.getId(), e);
			return null;
		}

		WebAsyncUtils.getAsyncManager(request).registerCallableInterceptor(RowStream.class.getName(),
				new CallableProcessingInterceptor() {
					@Override
					public <T> void afterCompletion(NativeWebRequest webRequest, Callable<T> task) {
						stream.close();
					}
				});
		return stream;
	}

	/**
	 * Logs a failure during streaming and returns an IOException to be thrown from the response body, which makes
	 * the servlet container abort the connection instead of completing the response.
	 */
	private IOException abortExport(String id, Exception e) {
		log.error("Export of dataset {} failed while streaming, aborting response", id, e);
		return new IOException("Export of dataset " + id + " failed", e);
	}

	/**
	 * Converts a row to CSV cells in the order of the column names. Rows are sparse if their CSV line had fewer
	 * cells than the header, missing cells are exported as empty strings.
	 */
	private static String[] toCsvRow(JSONObject json, List<String> columnNames) {
		String[] row = new String[columnNames.size()];
		for (int i = 0; i < row.length; i++) {
			row[i] = json.optString(columnNames.get(i), "");
		}
		return row;
	}

}
