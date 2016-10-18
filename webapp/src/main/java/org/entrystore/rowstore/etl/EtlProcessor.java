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

package org.entrystore.rowstore.etl;

import org.entrystore.rowstore.store.Dataset;
import org.entrystore.rowstore.store.RowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Manages the ETL-queue. Accepts and submits ETL-requests to the processing pipeline.
 *
 * Uses one thread to manage the queue and one thread per running ETL-process.
 *
 * @author Hannes Ebner
 */
public class EtlProcessor {

	private static Logger log = LoggerFactory.getLogger(EtlProcessor.class);

	private int concurrentConversions = 5;

	private int runningConversions = 0;

	private Object mutex = new Object();

	private Thread datasetSubmitter;

	private RowStore rowstore;

	private final ConcurrentLinkedQueue<EtlResource> postQueue = new ConcurrentLinkedQueue<>();

	public class DatasetSubmitter extends Thread {

		@Override
		public void run() {
			while (!interrupted()) {
				if (!postQueue.isEmpty() && runningConversions <= concurrentConversions) {
					EtlResource res = postQueue.poll();
					if (res != null) {
						log.info("Starting dataset loader for " + res.getDataset().getId());
						new DatasetLoader(res).start();
						synchronized (mutex) {
							runningConversions++;
						}
					}
				} else {
					try {
						Thread.sleep(5000);
					} catch (InterruptedException ie) {
						log.info("Dataset submitter got interrupted, shutting down submitter thread");
						return;
					}
				}
			}
		}

	}

	public class DatasetLoader extends Thread {

		private EtlResource etlResource;

		DatasetLoader(EtlResource etlResource) {
			this.etlResource = etlResource;
		}

		@Override
		public void run() {
			try {
				File fileToLoad = etlResource.getDataSource();
				Dataset dataset = etlResource.getDataset();
				log.info("Populating dataset " + dataset.getId() + " with data from file " + fileToLoad);
				if (dataset.populate(fileToLoad, etlResource.isAppending())) {
					log.info("Dataset " + dataset.getId() + " successfully populated");
				} else {
					log.info("An error occured while populating dataset " + dataset.getId());
				}
			} catch (IOException e) {
				log.error(e.getMessage());
			} finally {
				notifyFinished(etlResource);
			}
		}

	}

	public EtlProcessor(RowStore rowstore) {
		this.rowstore = rowstore;
		this.concurrentConversions = rowstore.getConfig().getMaxEtlProcesses();
		datasetSubmitter = new DatasetSubmitter();
		log.info("Starting dataset submitter");
		datasetSubmitter.start();
	}

	public void submit(EtlResource etlResource) {
		log.info("Adding dataset " + etlResource.getDataset().getId() + " to ETL processing queue");
		postQueue.add(etlResource);
	}

	public void notifyFinished(EtlResource etlResource) {
		synchronized (mutex) {
			runningConversions--;
		}
	}

	public void shutdown() {
		log.info("Shutting down ETL processor");
		if (datasetSubmitter != null) {
			datasetSubmitter.interrupt();
		}
	}

	public int getActiveEtlProcesses() {
		return runningConversions;
	}

}