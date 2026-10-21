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
 * {@link ConnectionWaitTimeoutException} if no slot becomes free in time. The slot is released when the connection
 * is closed, exactly once, also if closing fails.
 * <p>
 * An optional session statement (e.g., {@code SET lock_timeout = ...}) is executed on every connection before it is
 * handed out.
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

	private final Slots slots;

	private final long waitTimeoutMillis;

	private final String sessionSql;

	/**
	 * Whether waits are recorded for the aggregated warnings; false for the data sources of {@link #withWaitTimeout(long)}.
	 */
	private final boolean recordWaits;

	/**
	 * @param target The data source to limit.
	 * @param name The name of the budget, for logging (e.g., "database query").
	 * @param maxConnections The maximum number of open connections, must be greater than 0.
	 * @param waitTimeoutMillis The maximum time to wait for a free slot, must be greater than 0.
	 * @param sessionSql A statement executed on every connection before it is handed out, or null.
	 */
	public LimitedDataSource(DataSource target, String name, int maxConnections, long waitTimeoutMillis, String sessionSql) {
		super(target);
		if (maxConnections <= 0 || waitTimeoutMillis <= 0) {
			throw new IllegalArgumentException("Maximum connections and wait timeout must be greater than 0");
		}
		this.slots = new Slots(name, maxConnections);
		this.waitTimeoutMillis = waitTimeoutMillis;
		this.sessionSql = sessionSql;
		this.recordWaits = true;
	}

	public LimitedDataSource(DataSource target, String name, int maxConnections, long waitTimeoutMillis) {
		this(target, name, maxConnections, waitTimeoutMillis, null);
	}

	private LimitedDataSource(LimitedDataSource budget, long waitTimeoutMillis) {
		super(budget.getTargetDataSource());
		this.slots = budget.slots;
		this.waitTimeoutMillis = waitTimeoutMillis;
		this.sessionSql = budget.sessionSql;
		this.recordWaits = false;
	}

	/**
	 * @return Returns a data source that uses the same slots (and session statement) as this one, but waits at most
	 * the given time for a free slot, e.g., for health checks that must not wait as long as requests. Its waits are not
	 * included in the aggregated warnings, which report the waits of this data source.
	 */
	public LimitedDataSource withWaitTimeout(long waitTimeoutMillis) {
		if (waitTimeoutMillis <= 0) {
			throw new IllegalArgumentException("Wait timeout must be greater than 0");
		}
		return new LimitedDataSource(this, waitTimeoutMillis);
	}

	@Override
	public Connection getConnection() throws SQLException {
		acquire();
		try {
			return limit(prepare(super.getConnection()));
		} catch (Throwable t) {
			slots.semaphore.release();
			throw t;
		}
	}

	@Override
	public Connection getConnection(String username, String password) throws SQLException {
		acquire();
		try {
			return limit(prepare(super.getConnection(username, password)));
		} catch (Throwable t) {
			slots.semaphore.release();
			throw t;
		}
	}

	/**
	 * @return Returns the current usage of the budget.
	 */
	public ConnectionUsage getUsage() {
		return new ConnectionUsage(slots.max - slots.semaphore.availablePermits(), slots.max, slots.semaphore.getQueueLength());
	}

	public String getName() {
		return slots.name;
	}

	public int getMaxConnections() {
		return slots.max;
	}

	/**
	 * @return Returns whether the throwable or one of its causes is a timeout while waiting for a free connection of a
	 * budget, which means that the database is in use, not that it is unreachable.
	 */
	public static boolean isWaitTimeout(Throwable t) {
		for (Throwable c = t; c != null; c = c.getCause()) {
			if (c instanceof ConnectionWaitTimeoutException) {
				return true;
			}
		}
		return false;
	}

	private void acquire() throws SQLException {
		long start = System.nanoTime();
		boolean acquired;
		try {
			// unlike tryAcquire() without timeout, this respects the fairness (FIFO order) of the semaphore
			acquired = slots.semaphore.tryAcquire(waitTimeoutMillis, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new SQLTransientConnectionException("Interrupted while waiting for a " + slots.name + " connection", e);
		}
		long waitedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
		if (!acquired) {
			if (recordWaits) {
				recordSlowWait(waitedMillis, true);
			}
			throw new ConnectionWaitTimeoutException("No " + slots.name + " connection available within " +
					waitTimeoutMillis + " ms, all " + slots.max + " in use");
		}
		if (recordWaits && waitedMillis >= SLOW_WAIT_MILLIS) {
			recordSlowWait(waitedMillis, false);
		}
	}

	private Connection prepare(Connection conn) throws SQLException {
		if (sessionSql != null) {
			try (Statement stmt = conn.createStatement()) {
				stmt.execute(sessionSql);
			} catch (SQLException | RuntimeException e) {
				conn.close();
				throw e;
			}
		}
		return conn;
	}

	private void recordSlowWait(long waitedMillis, boolean timedOut) {
		(timedOut ? slots.timeouts : slots.slowWaits).incrementAndGet();
		slots.longestWaitMillis.accumulateAndGet(waitedMillis, Math::max);
		long now = System.currentTimeMillis();
		long last = slots.lastWarning.get();
		if (now - last >= WARN_INTERVAL_MILLIS && slots.lastWarning.compareAndSet(last, now)) {
			ConnectionUsage usage = getUsage();
			log.warn("{} connections are scarce: since the last warning, {} request(s) waited longer than {} ms for a " +
							"connection and {} request(s) got none within {} ms; longest wait {} ms, in use {}/{}, waiting {}",
					slots.name, slots.slowWaits.getAndSet(0), SLOW_WAIT_MILLIS, slots.timeouts.getAndSet(0),
					waitTimeoutMillis, slots.longestWaitMillis.getAndSet(0), usage.inUse(), usage.max(), usage.waiting());
		}
	}

	private Connection limit(Connection target) {
		AtomicBoolean released = new AtomicBoolean();
		return (Connection) wrap(Connection.class, target, null, () -> {
			if (released.compareAndSet(false, true)) {
				slots.semaphore.release();
			}
		});
	}

	private static Object wrap(Class<?> type, Object target, Object parent, Runnable onClose) {
		return Proxy.newProxyInstance(LimitedDataSource.class.getClassLoader(), new Class<?>[]{type},
				new Handler(target, parent, onClose));
	}

	/**
	 * Thrown if no connection of a budget became free within the wait timeout.
	 */
	public static class ConnectionWaitTimeoutException extends SQLTransientConnectionException {

		public ConnectionWaitTimeoutException(String message) {
			super(message);
		}

	}

	/**
	 * The slots of a budget, shared by the data sources returned by {@link #withWaitTimeout(long)}.
	 */
	private static final class Slots {

		final String name;

		final int max;

		final Semaphore semaphore;

		final AtomicInteger slowWaits = new AtomicInteger();

		final AtomicInteger timeouts = new AtomicInteger();

		final AtomicLong longestWaitMillis = new AtomicLong();

		final AtomicLong lastWarning = new AtomicLong();

		Slots(String name, int max) {
			this.name = name;
			this.max = max;
			this.semaphore = new Semaphore(max, true);
		}

	}

	/**
	 * Delegates to a connection, a statement created from it or a result set of such a statement.
	 * <p>
	 * Only close() of the connection releases the slot; abort() does not, the connection has to be closed afterwards
	 * as usual. Objects that are not created by statements are not wrapped, e.g., getMetaData().getConnection()
	 * returns the connection of the target data source, which must not be closed. A proxy is equal only to itself.
	 *
	 * @param parent The proxy of the connection of a statement, or of the statement of a result set.
	 * @param onClose Run when the connection is closed, null for statements and result sets.
	 */
	private record Handler(Object target, Object parent, Runnable onClose) implements InvocationHandler {

		@Override
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			String methodName = method.getName();
			int parameterCount = method.getParameterCount();
			if (parameterCount == 0 && ((target instanceof Statement && methodName.equals("getConnection")) ||
					(target instanceof ResultSet && methodName.equals("getStatement")))) {
				return parent;
			}
			if (parameterCount == 1 && methodName.equals("equals") && method.getParameterTypes()[0] == Object.class) {
				return proxy == args[0];
			}
			if (parameterCount == 0 && methodName.equals("hashCode")) {
				return System.identityHashCode(proxy);
			}
			Object result;
			try {
				result = method.invoke(target, args);
			} catch (InvocationTargetException e) {
				throw e.getCause();
			} finally {
				if (onClose != null && parameterCount == 0 && methodName.equals("close")) {
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
