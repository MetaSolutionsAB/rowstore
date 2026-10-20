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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.entrystore.rowstore.store.Dataset;
import org.entrystore.rowstore.store.Datasets;
import org.entrystore.rowstore.store.QueryResult;
import org.entrystore.rowstore.store.RowStore;
import org.entrystore.rowstore.store.RowStoreConfig;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Tests the scheduling of {@link EtlProcessor}: per-dataset ordering, coalescing of waiting loads,
 * permits only for running jobs, and failure handling. Datasets are fakes whose loads are controlled by latches.
 */
class EtlProcessorTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    @TempDir
    Path tempDir;

    private final List<Load> loads = new CopyOnWriteArrayList<>();

    /** Names of loads/tasks in the order they started. */
    private final List<String> started = new CopyOnWriteArrayList<>();

    private final AtomicInteger running = new AtomicInteger();

    private final AtomicInteger maxRunning = new AtomicInteger();

    private EtlProcessor processor;

    private CapturingAppender appender;

    @AfterEach
    void tearDown() {
        loads.forEach(l -> l.release.countDown());
        if (processor != null) {
            processor.shutdown();
        }
        if (appender != null) {
            logger().removeAppender(appender);
            appender.stop();
        }
    }

    @Test
    @DisplayName("TC-ETL-001: Loads of the same dataset run one at a time in submission order")
    void loadsOfSameDataset_runInSubmissionOrder() throws Exception {
        processor = new EtlProcessor(rowStore(5));
        FakeDataset a = new FakeDataset("a");
        Load a1 = blockingLoad(a, "a1", true);
        Load a2 = load(a, "a2", true);
        Load a3 = load(a, "a3", true);
        Load a4 = load(a, "a4", true);

        submit(a1);
        awaitStarted(a1);
        submit(a2);
        submit(a3);
        submit(a4);
        Thread.sleep(300);
        assertThat(started).containsExactly("a1");

        a1.release.countDown();
        await().atMost(TIMEOUT).until(() -> started.size() == 4 && running.get() == 0);
        assertThat(started).containsExactly("a1", "a2", "a3", "a4");
        assertThat(a.maxRunning.get()).isEqualTo(1);
    }

    @Test
    @DisplayName("TC-ETL-002: Loads of different datasets run concurrently up to the permit limit")
    void loadsOfDifferentDatasets_runConcurrentlyUpToPermitLimit() throws Exception {
        processor = new EtlProcessor(rowStore(2));
        Load a1 = blockingLoad(new FakeDataset("a"), "a1", true);
        Load b1 = blockingLoad(new FakeDataset("b"), "b1", true);
        Load c1 = blockingLoad(new FakeDataset("c"), "c1", true);

        submit(a1);
        awaitStarted(a1);
        submit(b1);
        awaitStarted(b1);
        submit(c1);
        Thread.sleep(300);
        assertThat(started).containsExactlyInAnyOrder("a1", "b1");
        assertThat(processor.getActiveEtlProcesses()).isEqualTo(2);

        a1.release.countDown();
        awaitStarted(c1);
        assertThat(maxRunning.get()).isEqualTo(2);
    }

    @Test
    @DisplayName("TC-ETL-003: A load waiting for its dataset does not hold a permit")
    void waitingLoad_doesNotHoldPermit() throws Exception {
        processor = new EtlProcessor(rowStore(2));
        FakeDataset a = new FakeDataset("a");
        Load a1 = blockingLoad(a, "a1", true);
        Load a2 = load(a, "a2", true);
        Load b1 = blockingLoad(new FakeDataset("b"), "b1", true);

        submit(a1);
        awaitStarted(a1);
        submit(a2);
        submit(b1);

        // b1 gets the second permit although a2 was submitted before it
        awaitStarted(b1);
        assertThat(started).containsExactly("a1", "b1");
        assertThat(processor.getActiveEtlProcesses()).isEqualTo(2);

        a1.release.countDown();
        awaitStarted(a2);
        b1.release.countDown();
        await().atMost(TIMEOUT).until(() -> processor.getActiveEtlProcesses() == 0);
    }

    @Test
    @DisplayName("TC-ETL-004: A replace discards waiting loads of the same dataset but not the running one")
    void replace_discardsWaitingLoadsButNotRunningOne() throws Exception {
        processor = new EtlProcessor(rowStore(5));
        FakeDataset a = new FakeDataset("a");
        FakeDataset b = new FakeDataset("b");
        Load a1 = blockingLoad(a, "a1", true);
        Load a2 = load(a, "a2", true);
        Load a3 = load(a, "a3", false);
        Load b1 = blockingLoad(b, "b1", true);
        Load b2 = load(b, "b2", true);
        Load a4 = load(a, "a4", false);

        submit(a1);
        submit(b1);
        awaitStarted(a1);
        awaitStarted(b1);
        submit(a2);
        submit(a3);
        submit(b2);
        submit(a4);

        // a2 and a3 have not started and are superseded by a4, the waiting load of dataset b is kept
        assertThat(a2.file).doesNotExist();
        assertThat(a3.file).doesNotExist();
        assertThat(a1.file).exists();
        assertThat(b2.file).exists();

        a1.release.countDown();
        b1.release.countDown();
        await().atMost(TIMEOUT).until(() -> started.size() == 4 && running.get() == 0);
        assertThat(started).containsExactlyInAnyOrder("a1", "b1", "b2", "a4");
        assertThat(started.indexOf("a4")).isGreaterThan(started.indexOf("a1"));
        assertThat(loads).allSatisfy(l -> assertThat(l.file).doesNotExist());
    }

    @Test
    @DisplayName("TC-ETL-005: Failing loads are logged with stack trace, set to error, and do not block subsequent loads")
    void failingLoads_areLoggedAndDoNotBlockSubsequentLoads() throws Exception {
        appender = new CapturingAppender();
        appender.start();
        logger().addAppender(appender);

        processor = new EtlProcessor(rowStore(1));
        FakeDataset a = new FakeDataset("a");
        Load a1 = load(a, "a1", true);
        a1.failure = new IllegalStateException("simulated runtime failure");
        Load a2 = load(a, "a2", true);
        a2.failure = new IOException("simulated I/O failure");
        Load a3 = load(a, "a3", true);
        Load b1 = load(new FakeDataset("b"), "b1", true);

        submit(a1);
        submit(a2);
        submit(a3);
        submit(b1);

        await().atMost(TIMEOUT).until(() -> started.size() == 4 && processor.getActiveEtlProcesses() == 0);
        assertThat(started).containsSubsequence("a1", "a2", "a3");
        assertThat(started).contains("b1");
        assertThat(a.statuses).containsSubsequence(
                EtlStatus.PROCESSING, EtlStatus.ERROR,
                EtlStatus.PROCESSING, EtlStatus.ERROR,
                EtlStatus.PROCESSING, EtlStatus.AVAILABLE);
        assertThat(appender.events)
                .filteredOn(e -> e.getLevel() == Level.ERROR)
                .extracting(LogEvent::getThrown)
                .contains(a1.failure, a2.failure);
        assertThat(loads).allSatisfy(l -> assertThat(l.file).doesNotExist());
    }

    @Test
    @DisplayName("TC-ETL-006: A task run exclusively for a dataset runs in order with the dataset's loads")
    void exclusiveTask_runsInOrderWithLoads() throws Exception {
        processor = new EtlProcessor(rowStore(5));
        FakeDataset a = new FakeDataset("a");
        Load a1 = blockingLoad(a, "a1", true);
        Load a2 = load(a, "a2", false);

        submit(a1);
        awaitStarted(a1);
        processor.runExclusively(a.getId(), () -> {
            assertThat(a.running.get()).isZero();
            started.add("task");
        });
        submit(a2);
        Thread.sleep(300);
        assertThat(started).containsExactly("a1");

        a1.release.countDown();
        await().atMost(TIMEOUT).until(() -> started.size() == 3 && running.get() == 0);
        assertThat(started).containsExactly("a1", "task", "a2");
    }

    private Load load(FakeDataset dataset, String name, boolean append) throws IOException {
        File file = Files.createFile(tempDir.resolve(name + ".csv")).toFile();
        Load load = new Load(dataset, name, file, append);
        loads.add(load);
        return load;
    }

    private Load blockingLoad(FakeDataset dataset, String name, boolean append) throws IOException {
        Load load = load(dataset, name, append);
        load.blocking = true;
        return load;
    }

    private void submit(Load load) {
        processor.submit(new EtlResource(load.dataset, load.file, "text/csv", load.append));
    }

    private static void awaitStarted(Load load) throws InterruptedException {
        assertThat(load.started.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .as("load %s started", load.name).isTrue();
    }

    private static org.apache.logging.log4j.core.Logger logger() {
        return (org.apache.logging.log4j.core.Logger) LogManager.getLogger(EtlProcessor.class);
    }

    private static RowStore rowStore(int maxEtlProcesses) {
        JSONObject json = new JSONObject()
                .put("baseurl", "http://localhost/")
                .put("maxetlprocesses", maxEtlProcesses)
                .put("database", new JSONObject()
                        .put("host", "localhost")
                        .put("database", "unused")
                        .put("user", "unused")
                        .put("password", "unused"));
        RowStoreConfig config = new RowStoreConfig(json);
        return new RowStore() {
            @Override public Connection getConnection() { throw new UnsupportedOperationException(); }
            @Override public Connection getQueryConnection() { throw new UnsupportedOperationException(); }
            @Override public JdbcTemplate getJdbcTemplate() { throw new UnsupportedOperationException(); }
            @Override public JdbcTemplate getQueryJdbcTemplate() { throw new UnsupportedOperationException(); }
            @Override public Datasets getDatasets() { throw new UnsupportedOperationException(); }
            @Override public RowStoreConfig getConfig() { return config; }
            @Override public EtlProcessor getEtlProcessor() { throw new UnsupportedOperationException(); }
            @Override public void shutdown() { }
        };
    }

    private static final class Load {
        final FakeDataset dataset;
        final String name;
        final File file;
        final boolean append;
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        volatile boolean blocking;
        volatile Exception failure;

        Load(FakeDataset dataset, String name, File file, boolean append) {
            this.dataset = dataset;
            this.name = name;
            this.file = file;
            this.append = append;
        }
    }

    /**
     * A dataset whose populate() behaves like the real one with regard to status changes
     * (PROCESSING at start, AVAILABLE at the end) but is controlled by the test.
     */
    private final class FakeDataset implements Dataset {
        final String id;
        final List<Integer> statuses = new CopyOnWriteArrayList<>();
        final AtomicInteger running = new AtomicInteger();
        final AtomicInteger maxRunning = new AtomicInteger();
        volatile int status = EtlStatus.ACCEPTED_DATA;

        FakeDataset(String id) {
            this.id = id;
        }

        @Override
        public boolean populate(File csvFile, boolean append) throws IOException {
            Load load = loads.stream().filter(l -> l.file.equals(csvFile)).findFirst().orElseThrow();
            setStatus(EtlStatus.PROCESSING);
            started.add(load.name);
            maxRunning.accumulateAndGet(running.incrementAndGet(), Math::max);
            EtlProcessorTest.this.maxRunning.accumulateAndGet(EtlProcessorTest.this.running.incrementAndGet(), Math::max);
            try {
                load.started.countDown();
                if (load.blocking && !load.release.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                    throw new IllegalStateException("Load " + load.name + " was not released");
                }
                if (load.failure instanceof IOException ioe) {
                    throw ioe;
                } else if (load.failure instanceof RuntimeException re) {
                    throw re;
                }
                setStatus(EtlStatus.AVAILABLE);
                return true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            } finally {
                running.decrementAndGet();
                EtlProcessorTest.this.running.decrementAndGet();
            }
        }

        @Override public String getId() { return id; }
        @Override public int getStatus() { return status; }
        @Override public void setStatus(int status) { this.status = status; statuses.add(status); }
        @Override public Date getCreationDate() { return new Date(); }
        @Override public QueryResult query(Map<String, String> tuples, int limit, int offset) { throw new UnsupportedOperationException(); }
        @Override public ResultSet streamAll() { throw new UnsupportedOperationException(); }
        @Override public Set<String> getColumnNames() { return Set.of(); }
        @Override public long getRowCount() { return 0; }
        @Override public Set<String> getAliases() { return Set.of(); }
        @Override public boolean setAliases(Set<String> aliases) { return false; }
    }

    private static final class CapturingAppender extends AbstractAppender {
        final List<LogEvent> events = new CopyOnWriteArrayList<>();

        CapturingAppender() {
            super("capturing", null, null, true, Property.EMPTY_ARRAY);
        }

        @Override
        public void append(LogEvent event) {
            events.add(event.toImmutable());
        }
    }

}
