package org.entrystore.rowstore.store;

import java.util.Set;

/**
 * @author Hannes Ebner
 */
public interface Datasets {

	Set<Dataset> getAll();

	Dataset createDataset(String id);

	boolean purgeDataset(String id);

	Dataset getDataset(String id);

}