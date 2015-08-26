package org.entrystore.rowstore.etl;

import org.entrystore.rowstore.store.Dataset;
import org.restlet.data.MediaType;

import java.io.File;

/**
 * @author Hannes Ebner
 */
public class EtlResource {

	Dataset dataset;

	File dataSource;

	MediaType format;

	EtlResource(Dataset dataset, File dataSource, MediaType format) {
		this.dataset = dataset;
		this.dataSource = dataSource;
		this.format = format;
	}

	Dataset getId() {
		return dataset;
	}

	File getDataSource() {
		return dataSource;
	}

	MediaType getFormat() {
		return format;
	}

}