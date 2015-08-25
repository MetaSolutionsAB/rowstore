package org.entrystore.rowstore.etl;

import org.restlet.data.MediaType;

import java.io.File;

/**
 * @author Hannes Ebner
 */
public class EtlResource {

	String id;

	File dataSource;

	MediaType format;

	EtlResource(String id, File dataSource, MediaType format) {
		this.id = id;
		this.dataSource = dataSource;
		this.format = format;
	}

	String getId() {
		return id;
	}

	File getDataSource() {
		return dataSource;
	}

	MediaType getFormat() {
		return format;
	}

}