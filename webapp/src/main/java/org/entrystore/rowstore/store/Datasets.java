package org.entrystore.rowstore.store;

import java.util.Set;

/**
 * @author Hannes Ebner
 */
public interface Datasets {

	Set<Dataset> getDatasets();

	void addDataset(Dataset ds);

	void removeDataset(Dataset ds);

}