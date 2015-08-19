package org.entrystore.rowstore.etl;

import com.opencsv.CSVReader;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

/**
 * @author Hannes Ebner
 */
public class ConverterUtil {

	private static Logger log = LoggerFactory.getLogger(ConverterUtil.class);

	protected static JSONObject csvLineToJsonObject(String[] line, String[] labels) throws JSONException {
		if (line.length != labels.length) {
			throw new IllegalArgumentException("Arrays must not be of different length");
		}

		JSONObject result = new JSONObject();
		for (int i = 0; i < line.length; i++) {
			result.put(labels[i], line[i]);
		}

		return result;
	}

	public static boolean csvFileToJsonFile(File input, File output) {
		CSVReader cr = null;
		BufferedWriter bw = null;
		try {
			try {
				cr = new CSVReader(new FileReader(input));
			} catch (FileNotFoundException e) {
				log.error(e.getMessage());
				return false;
			}

			try {
				bw = new BufferedWriter(new FileWriter(output));
				long lineCount = 0;
				String[] labels = null;
				String[] line;
				while ((line = cr.readNext()) != null) {
					if (lineCount == 0) {
						labels = line;
					} else {
						JSONObject jsonLine = csvLineToJsonObject(line, labels);
						if (lineCount > 0) {
							bw.append(", ");
							bw.newLine();
						}
						bw.append(jsonLine.toString());
					}
					lineCount++;
				}
			} catch (IOException e) {
				log.error(e.getMessage());
				return false;
			} catch (JSONException e) {
				log.error(e.getMessage());
				return false;
			}
		} finally {
			try {
				if (cr != null) {
					cr.close();
				}
				if (bw != null) {
					bw.close();
				}
			} catch (IOException e) {
				log.error(e.getMessage());
				return false;
			}
		}
		return true;
	}

}