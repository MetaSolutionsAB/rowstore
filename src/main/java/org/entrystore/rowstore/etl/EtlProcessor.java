/*
 * Copyright (c) 2011-2026 MetaSolutions AB <info@metasolutions.se>
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages the ETL-queue. Accepts and submits ETL-requests to the processing pipeline.
 *
 * Uses virtual threads with a semaphore for concurrency limiting.
 *
 * @author Hannes Ebner
 */
public class EtlProcessor {

	private static final Logger log = LoggerFactory.getLogger(EtlProcessor.class);

	private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

	private final Semaphore concurrencyLimiter;

	private final AtomicInteger activeProcesses = new AtomicInteger(0);

	private final RowStore rowstore;

	public EtlProcessor(RowStore rowstore) {
		this.rowstore = rowstore;
		int maxConcurrent = rowstore.getConfig().getMaxEtlProcesses();
		if (maxConcurrent <= 0) {
			maxConcurrent = 5;
		}
		this.concurrencyLimiter = new Semaphore(maxConcurrent);
		log.info("ETL processor initialized with max {} concurrent processes", maxConcurrent);
	}

	public void submit(EtlResource etlResource) {
		log.info("Submitting dataset {} to ETL processing", etlResource.getDataset().getId());
		executor.submit(() -> processDataset(etlResource));
	}

	private void processDataset(EtlResource etlResource) {
		try {
			concurrencyLimiter.acquire();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			log.warn("ETL task interrupted while waiting for permit");
			return;
		}
		activeProcesses.incrementAndGet();
		File fileToLoad = etlResource.getDataSource();
		try {
			Dataset dataset = etlResource.getDataset();
			while (EtlStatus.PROCESSING == dataset.getStatus()) {
				log.info("Dataset is already being processed, retrying in 5 seconds");
				try {
					Thread.sleep(5000);
				} catch (InterruptedException ie) {
					Thread.currentThread().interrupt();
					log.error("Dataset loader got interrupted while waiting for dataset to finish processing");
					return;
				}
			}
			log.info("Populating dataset {} with data from file {}", dataset.getId(), fileToLoad);
			if (dataset.populate(fileToLoad, etlResource.isAppending())) {
				log.info("Dataset {} successfully populated", dataset.getId());
			} else {
				log.error("An error occurred while populating dataset {}", dataset.getId());
			}
		} catch (IOException e) {
			log.error(e.getMessage());
		} finally {
			activeProcesses.decrementAndGet();
			concurrencyLimiter.release();
			if (fileToLoad != null && fileToLoad.exists()) {
				if (fileToLoad.delete()) {
					log.debug("Deleted temporary file {}", fileToLoad);
				} else {
					log.warn("Failed to delete temporary file {}", fileToLoad);
				}
			}
		}
	}

	public void shutdown() {
		log.info("Shutting down ETL processor");
		executor.shutdown();
		try {
			if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
				executor.shutdownNow();
				log.warn("ETL processor did not terminate gracefully, forced shutdown");
			}
		} catch (InterruptedException e) {
			executor.shutdownNow();
			Thread.currentThread().interrupt();
		}
	}

	public int getActiveEtlProcesses() {
		return activeProcesses.get();
	}

}
