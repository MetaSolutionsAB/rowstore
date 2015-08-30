package org.entrystore.rowstore.etl;

import org.entrystore.rowstore.store.Dataset;
import org.entrystore.rowstore.store.RowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
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
				dataset.populate(fileToLoad);
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
		datasetSubmitter.start();
	}

	public void submit(EtlResource etlResource) {
		postQueue.add(etlResource);
	}

	public void notifyFinished(EtlResource etlResource) {
		synchronized (mutex) {
			runningConversions--;
		}
	}

	public void shutdown() {
		if (datasetSubmitter != null) {
			datasetSubmitter.interrupt();
		}
	}

	public int getActiveEtlProcesses() {
		return runningConversions;
	}

}