package org.entrystore.rowstore.store.impl;

import org.entrystore.rowstore.store.Dataset;

/**
 * @author Hannes Ebner
 */
public class PgDataset implements Dataset {

	private String id;

	public PgDataset(String id) {
		this.id = id;
	}

	@Override
	public String getId() {
		return id;
	}

}
