package org.entrystore.rowstore.store.impl;

import org.entrystore.rowstore.store.Dataset;
import org.entrystore.rowstore.store.Datasets;
import org.entrystore.rowstore.store.RowStore;

import java.util.Set;

/**
 * @author Hannes Ebner
 */
public class PgDatasets implements Datasets {

	PgRowStore rowstore;

	protected PgDatasets(PgRowStore rowstore) {
		this.rowstore = rowstore;
	}

	@Override
	public Set<Dataset> getDatasets() {
		// TODO

		return null;
	}

	@Override
	public void addDataset(Dataset ds) {
		// TODO
	}

	@Override
	public void removeDataset(Dataset ds) {
		// TODO
	}

}