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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages the ETL-queue. Accepts and submits ETL-requests to the processing pipeline.
 *
 * Jobs are queued per dataset and run one at a time per dataset, in submission order, on a virtual thread that
 * exists while the dataset has jobs. Datasets are identified by their canonical ID. A semaphore permit is acquired
 * only for the job that is next in line for its dataset, so the number of concurrent loads is limited without
 * waiting jobs blocking the loads of other datasets.
 *
 * @author Hannes Ebner
 */
public class EtlProcessor {

	private static final Logger log = LoggerFactory.getLogger(EtlProcessor.class);

	private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

	private final int maxEtlProcesses;

	private final Semaphore concurrencyLimiter;

	private final AtomicInteger activeProcesses = new AtomicInteger(0);

	/**
	 * Jobs that have not started yet, per canonical dataset ID. A dataset has an entry as long as a worker is active
	 * for it; the worker removes the entry when no jobs are left. Guarded by itself.
	 */
	private final Map<String, Deque<Job>> waitingJobs = new HashMap<>();

	/**
	 * Set by {@link #shutdown()}, after which no jobs are accepted. Guarded by {@link #waitingJobs}.
	 */
	private boolean shutDown;

	public EtlProcessor(RowStore rowstore) {
		int maxConcurrent = rowstore.getConfig().getMaxEtlProcesses();
		if (maxConcurrent <= 0) {
			maxConcurrent = 5;
		}
		this.maxEtlProcesses = maxConcurrent;
		this.concurrencyLimiter = new Semaphore(maxConcurrent);
		log.info("ETL processor initialized with max {} concurrent processes", maxConcurrent);
	}

	/**
	 * Submits a CSV file to be loaded into a dataset. Loads of the same dataset run in submission order. A replace
	 * (no append) discards the loads of the same dataset that have not started yet, since it supersedes them.
	 */
	public void submit(EtlResource etlResource) {
		String datasetId = etlResource.getDataset().getCanonicalId();
		log.info("Submitting dataset {} to ETL processing", datasetId);
		enqueue(datasetId, new LoadJob(etlResource), !etlResource.isAppending());
	}

	/**
	 * Runs a task exclusively for a dataset: after all previously submitted jobs of the dataset and never
	 * concurrently with another job of the same dataset. Like a load, the task holds an ETL permit while running.
	 *
	 * @return Returns a future that is completed when the task has finished, exceptionally if the task threw an
	 * exception, and that is cancelled if the task is discarded without running (e.g., at shutdown).
	 * @throws RejectedExecutionException If the ETL processor is shut down.
	 */
	public CompletableFuture<Void> runExclusively(Dataset dataset, Runnable task) {
		return callExclusively(dataset, () -> {
			task.run();
			return null;
		});
	}

	/**
	 * Like {@link #runExclusively(Dataset, Runnable)}, for a task with a result.
	 *
	 * @return Returns a future that is completed with the task's result, exceptionally if the task threw an
	 * exception, and that is cancelled if the task is discarded without running (e.g., at shutdown).
	 * @throws RejectedExecutionException If the ETL processor is shut down.
	 */
	public <T> CompletableFuture<T> callExclusively(Dataset dataset, Callable<T> task) {
		String datasetId = dataset.getCanonicalId();
		TaskJob<T> job = new TaskJob<>(datasetId, task, new CompletableFuture<>());
		enqueue(datasetId, job, false);
		return job.done();
	}

	private void enqueue(String datasetId, Job job, boolean supersedesWaitingLoads) {
		List<Job> discarded = new ArrayList<>();
		boolean startWorker;
		synchronized (waitingJobs) {
			if (shutDown) {
				job.discard("the ETL processor is shut down");
				throw new RejectedExecutionException("The ETL processor is shut down");
			}
			Deque<Job> queue = waitingJobs.get(datasetId);
			startWorker = queue == null;
			if (startWorker) {
				queue = new ArrayDeque<>();
				waitingJobs.put(datasetId, queue);
			} else if (supersedesWaitingLoads) {
				for (Iterator<Job> it = queue.iterator(); it.hasNext(); ) {
					Job waiting = it.next();
					if (waiting instanceof LoadJob) {
						it.remove();
						discarded.add(waiting);
					}
				}
			}
			queue.addLast(job);
		}
		discarded.forEach(j -> j.discard("superseded by a subsequent replace"));
		if (startWorker) {
			try {
				executor.submit(() -> runJobs(datasetId));
			} catch (RejectedExecutionException e) {
				discardAll(datasetId, "the ETL processor is shut down");
				throw e;
			}
		}
	}

	/**
	 * Runs the jobs of a dataset one after another until its queue is empty.
	 *
	 * The worker waits for a permit while its next job is still in the queue, so that the job can be discarded by a
	 * subsequent replace until it starts. A permit is held from just before the head of the queue is taken until
	 * that job has finished; a worker never holds more than one permit.
	 *
	 * If the worker fails abnormally (e.g., an Error thrown by a job), the dataset's remaining jobs are discarded and
	 * its entry is removed, so that subsequently submitted jobs get a new worker.
	 */
	private void runJobs(String datasetId) {
		try {
			while (true) {
				synchronized (waitingJobs) {
					if (waitingJobs.get(datasetId).isEmpty()) {
						waitingJobs.remove(datasetId);
						return;
					}
				}
				concurrencyLimiter.acquire();
				Job job;
				synchronized (waitingJobs) {
					// not expected to be empty: a replace removes queued loads but adds itself
					job = waitingJobs.get(datasetId).pollFirst();
				}
				if (job == null) {
					concurrencyLimiter.release();
					continue;
				}
				activeProcesses.incrementAndGet();
				try {
					job.run();
				} catch (RuntimeException e) {
					log.error("ETL job for dataset {} failed", datasetId, e);
				} finally {
					activeProcesses.decrementAndGet();
					concurrencyLimiter.release();
				}
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			discardAll(datasetId, "the ETL processor was interrupted");
		} catch (RuntimeException | Error e) {
			log.error("ETL worker for dataset {} failed", datasetId, e);
			discardAll(datasetId, "the ETL worker failed");
			throw e;
		}
	}

	private void discardAll(String datasetId, String reason) {
		Deque<Job> queue;
		synchronized (waitingJobs) {
			queue = waitingJobs.remove(datasetId);
		}
		if (queue != null) {
			queue.forEach(j -> j.discard(reason));
		}
	}

	private void processDataset(EtlResource etlResource) {
		Dataset dataset = etlResource.getDataset();
		File fileToLoad = etlResource.getDataSource();
		try {
			log.info("Populating dataset {} with data from file {}", dataset.getId(), fileToLoad);
			if (dataset.populate(fileToLoad, etlResource.isAppending())) {
				log.info("Dataset {} successfully populated", dataset.getId());
			} else {
				// the reason (an error, or a deleted dataset) has been logged by populate()
				log.warn("Dataset {} was not populated", dataset.getId());
			}
		} catch (IOException | RuntimeException e) {
			log.error("Populating dataset {} failed", dataset.getId(), e);
			// populate() normally sets the status itself, this covers failures outside its status handling
			try {
				dataset.setStatusFromEtlJob(EtlStatus.ERROR);
			} catch (RuntimeException e1) {
				log.error("Unable to set status of dataset {} to error", dataset.getId(), e1);
			}
		} finally {
			deleteTempFile(fileToLoad);
		}
	}

	private static void deleteTempFile(File file) {
		if (file != null && file.exists()) {
			if (file.delete()) {
				log.debug("Deleted temporary file {}", file);
			} else {
				log.warn("Failed to delete temporary file {}", file);
			}
		}
	}

	/**
	 * Shuts the ETL processor down (DEPL-7.01): jobs that have not started yet are discarded (temp files deleted,
	 * logged at WARN level with their datasets), so that no new loads start; running jobs get 30 seconds to finish and
	 * are interrupted afterwards. A load that is interrupted is rolled back. The datasets of discarded and interrupted
	 * loads keep their status (ACCEPTED_DATA or PROCESSING) until the next startup sets it to ERROR (ETL-8.06).
	 */
	public void shutdown() {
		log.info("Shutting down ETL processor");
		Map<String, List<Job>> discarded = new HashMap<>();
		synchronized (waitingJobs) {
			shutDown = true;
			// the entries stay, their workers remove them when they find their queue empty
			waitingJobs.forEach((datasetId, queue) -> {
				if (!queue.isEmpty()) {
					discarded.put(datasetId, new ArrayList<>(queue));
					queue.clear();
				}
			});
		}
		discarded.values().forEach(jobs -> jobs.forEach(j -> j.discard("RowStore is shutting down")));
		List<String> datasetsWithLoads = discarded.entrySet().stream()
				.filter(e -> e.getValue().stream().anyMatch(j -> j instanceof LoadJob))
				.map(Map.Entry::getKey)
				.sorted()
				.toList();
		if (!datasetsWithLoads.isEmpty()) {
			log.warn("Discarded the queued uploads of {} dataset(s) at shutdown, they have to be uploaded again: {}",
					datasetsWithLoads.size(), datasetsWithLoads);
		}
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

	/**
	 * @return Returns the maximum number of jobs that run at the same time.
	 */
	public int getMaxEtlProcesses() {
		return maxEtlProcesses;
	}

	/**
	 * @return Returns the number of running jobs. Jobs waiting for their turn are not counted.
	 */
	public int getActiveEtlProcesses() {
		return activeProcesses.get();
	}

	/**
	 * @return Returns the number of jobs that are queued but have not started yet, including the next job of a
	 * dataset while it waits for a permit.
	 */
	public int getQueuedEtlProcesses() {
		synchronized (waitingJobs) {
			return waitingJobs.values().stream().mapToInt(Deque::size).sum();
		}
	}

	private interface Job {

		void run();

		/**
		 * Called instead of run() if the job is dropped before it started.
		 */
		void discard(String reason);

	}

	private final class LoadJob implements Job {

		private final EtlResource etlResource;

		LoadJob(EtlResource etlResource) {
			this.etlResource = etlResource;
		}

		@Override
		public void run() {
			processDataset(etlResource);
		}

		@Override
		public void discard(String reason) {
			log.info("Discarding queued load of file {} into dataset {}: {}",
					etlResource.getDataSource(), etlResource.getDataset().getId(), reason);
			deleteTempFile(etlResource.getDataSource());
		}

	}

	private record TaskJob<T>(String datasetId, Callable<T> task, CompletableFuture<T> done) implements Job {

		@Override
		public void run() {
			T result;
			try {
				result = task.call();
			} catch (RuntimeException | Error e) {
				done.completeExceptionally(e);
				throw e;
			} catch (Exception e) {
				done.completeExceptionally(e);
				throw new CompletionException(e);
			}
			done.complete(result);
		}

		@Override
		public void discard(String reason) {
			log.warn("Discarding queued task for dataset {}: {}", datasetId, reason);
			done.cancel(false);
		}

	}

}
