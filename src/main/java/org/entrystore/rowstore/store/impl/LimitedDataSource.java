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

package org.entrystore.rowstore.store.impl;

import org.entrystore.rowstore.store.ConnectionUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.DelegatingDataSource;

import javax.sql.DataSource;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A connection budget (ARCH-7): limits the number of connections of the target data source that are open at the
 * same time. {@link #getConnection()} waits for a free slot in FIFO order, for at most the wait timeout, and throws
 * {@link SQLTransientConnectionException} if no slot becomes free in time. The slot is released when the connection
 * is closed, exactly once, also if closing fails.
 * <p>
 * Statements and result sets created from a connection return the limited connection and statement from
 * getConnection() and getStatement(), so that closing a connection obtained from them releases the slot as well.
 * <p>
 * Waits longer than {@link #SLOW_WAIT_MILLIS} and timeouts are logged at WARN level, aggregated to at most one
 * message per {@link #WARN_INTERVAL_MILLIS}.
 */
public class LimitedDataSource extends DelegatingDataSource {

	private static final Logger log = LoggerFactory.getLogger(LimitedDataSource.class);

	static final long SLOW_WAIT_MILLIS = 1000;

	static final long WARN_INTERVAL_MILLIS = 60000;

	private final String name;

	private final int maxConnections;

	private final long waitTimeoutMillis;

	private final Semaphore slots;

	private final AtomicInteger slowWaits = new AtomicInteger();

	private final AtomicInteger timeouts = new AtomicInteger();

	private final AtomicLong longestWaitMillis = new AtomicLong();

	private final AtomicLong lastWarning = new AtomicLong();

	/**
	 * @param target The data source to limit.
	 * @param name The name of the budget, for logging (e.g., "database query").
	 * @param maxConnections The maximum number of open connections, must be greater than 0.
	 * @param waitTimeoutMillis The maximum time to wait for a free slot, must be greater than 0.
	 */
	public LimitedDataSource(DataSource target, String name, int maxConnections, long waitTimeoutMillis) {
		super(target);
		if (maxConnections <= 0 || waitTimeoutMillis <= 0) {
			throw new IllegalArgumentException("Maximum connections and wait timeout must be greater than 0");
		}
		this.name = name;
		this.maxConnections = maxConnections;
		this.waitTimeoutMillis = waitTimeoutMillis;
		this.slots = new Semaphore(maxConnections, true);
	}

	@Override
	public Connection getConnection() throws SQLException {
		acquire();
		try {
			return limit(super.getConnection());
		} catch (Throwable t) {
			slots.release();
			throw t;
		}
	}

	@Override
	public Connection getConnection(String username, String password) throws SQLException {
		acquire();
		try {
			return limit(super.getConnection(username, password));
		} catch (Throwable t) {
			slots.release();
			throw t;
		}
	}

	/**
	 * @return Returns the current usage of the budget.
	 */
	public ConnectionUsage getUsage() {
		return new ConnectionUsage(maxConnections - slots.availablePermits(), maxConnections, slots.getQueueLength());
	}

	public String getName() {
		return name;
	}

	public int getMaxConnections() {
		return maxConnections;
	}

	private void acquire() throws SQLException {
		long start = System.nanoTime();
		boolean acquired;
		try {
			// unlike tryAcquire() without timeout, this respects the fairness (FIFO order) of the semaphore
			acquired = slots.tryAcquire(waitTimeoutMillis, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new SQLTransientConnectionException("Interrupted while waiting for a " + name + " connection", e);
		}
		long waitedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
		if (!acquired) {
			recordSlowWait(waitedMillis, true);
			throw new SQLTransientConnectionException("No " + name + " connection available within " +
					waitTimeoutMillis + " ms, all " + maxConnections + " in use");
		}
		if (waitedMillis >= SLOW_WAIT_MILLIS) {
			recordSlowWait(waitedMillis, false);
		}
	}

	private void recordSlowWait(long waitedMillis, boolean timedOut) {
		(timedOut ? timeouts : slowWaits).incrementAndGet();
		longestWaitMillis.accumulateAndGet(waitedMillis, Math::max);
		long now = System.currentTimeMillis();
		long last = lastWarning.get();
		if (now - last >= WARN_INTERVAL_MILLIS && lastWarning.compareAndSet(last, now)) {
			ConnectionUsage usage = getUsage();
			log.warn("Waiting for {} connections: {} wait(s) longer than {} ms and {} timeout(s) after {} ms " +
							"since the last warning, longest wait {} ms; in use {}/{}, waiting {}",
					name, slowWaits.getAndSet(0), SLOW_WAIT_MILLIS, timeouts.getAndSet(0), waitTimeoutMillis,
					longestWaitMillis.getAndSet(0), usage.inUse(), usage.max(), usage.waiting());
		}
	}

	private Connection limit(Connection target) {
		AtomicBoolean released = new AtomicBoolean();
		return (Connection) wrap(Connection.class, target, null, () -> {
			if (released.compareAndSet(false, true)) {
				slots.release();
			}
		});
	}

	private static Object wrap(Class<?> type, Object target, Object parent, Runnable onClose) {
		return Proxy.newProxyInstance(LimitedDataSource.class.getClassLoader(), new Class<?>[]{type},
				new Handler(target, parent, onClose));
	}

	/**
	 * Delegates to a connection, a statement created from it or a result set of such a statement.
	 *
	 * @param parent The proxy of the connection of a statement, or of the statement of a result set.
	 * @param onClose Run when the connection is closed, null for statements and result sets.
	 */
	private record Handler(Object target, Object parent, Runnable onClose) implements InvocationHandler {

		@Override
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			String methodName = method.getName();
			boolean noArgs = method.getParameterCount() == 0;
			if (noArgs && ((target instanceof Statement && methodName.equals("getConnection")) ||
					(target instanceof ResultSet && methodName.equals("getStatement")))) {
				return parent;
			}
			Object result;
			try {
				result = method.invoke(target, args);
			} catch (InvocationTargetException e) {
				throw e.getCause();
			} finally {
				if (onClose != null && noArgs && methodName.equals("close")) {
					onClose.run();
				}
			}
			Class<?> returnType = method.getReturnType();
			if (result != null && (Statement.class.isAssignableFrom(returnType) ||
					(returnType == ResultSet.class && target instanceof Statement))) {
				return wrap(returnType, result, proxy, null);
			}
			return result;
		}

	}

}
