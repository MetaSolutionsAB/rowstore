package org.entrystore.rowstore.etl;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author Hannes Ebner
 */
public class EtlProcessor {

		private static Logger log = LoggerFactory.getLogger(EtlProcessor.class);

		private static int CONCURRENT_CONVERSIONS = 5;

		private int runningConversions = 0;

		private Thread datasetSubmitter;

		private final ConcurrentLinkedQueue<JSONObject> postQueue = new ConcurrentLinkedQueue<JSONObject>(); // FIXME should probably not be a JSONObject

		public class DatasetSubmitter extends Thread {

			@Override
			public void run() {
				while (!interrupted()) {
					if (!postQueue.isEmpty()) {
						for (; runningConversions < CONCURRENT_CONVERSIONS; runningConversions++) {
							// TODO
							// - get dataset from the queue
							// - submit dataset to a new conversion thread (in a new class?)

							// Note to self: a finished conversion thread should be able to report back
							// (finished()-method and an instance of EtlProcessor in the constructor of
							// the child thread?) to the EtlProcessor (e.g. to decrease the running count)
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

		public EtlProcessor() {
			datasetSubmitter = new DatasetSubmitter();
			datasetSubmitter.start();
		}

		public void shutdown() {
			if (datasetSubmitter != null) {
				datasetSubmitter.interrupt();
			}
		}

}