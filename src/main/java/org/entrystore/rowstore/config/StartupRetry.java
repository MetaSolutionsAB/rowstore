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

package org.entrystore.rowstore.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessResourceFailureException;

import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTransientConnectionException;
import java.util.Set;

/**
 * Retries the database work at startup (ARCH-7.08) while the database is not reachable, e.g., during a restart or a
 * failover of the database cluster, instead of failing the startup: with a delay of 1 second, doubled per attempt up
 * to 60 seconds, without limit. Each failed attempt is logged at WARN level with its cause. Only connectivity failures
 * are retried (see {@link #isConnectivityFailure(Throwable)}); any other failure, e.g., a failing migration or wrong
 * credentials, ends the retries and propagates, so that the startup fails as before.
 * <p>
 * The retries run in the startup thread before the web server accepts connections, so RowStore does not listen on its
 * port until the database is reachable. A shutdown of the JVM (e.g., SIGTERM) during the retries interrupts them and
 * fails the startup.
 */
public class StartupRetry {

	private static final Logger log = LoggerFactory.getLogger(StartupRetry.class);

	static final long INITIAL_DELAY_MILLIS = 1000;

	static final long MAX_DELAY_MILLIS = 60000;

	/**
	 * SQL states other than class 08 (connection exception) that mean that the database is temporarily not available:
	 * administrator or crash shutdown, the database is starting up, too many connections.
	 */
	private static final Set<String> UNAVAILABLE_SQL_STATES = Set.of("57P01", "57P02", "57P03", "53300");

	/**
	 * A task that needs the database.
	 */
	@FunctionalInterface
	public interface Task<T> {
		T run() throws Exception;
	}

	@FunctionalInterface
	interface Sleeper {
		void sleep(long millis) throws InterruptedException;
	}

	private final long initialDelayMillis;

	private final long maxDelayMillis;

	private final Sleeper sleeper;

	public StartupRetry() {
		this(INITIAL_DELAY_MILLIS, MAX_DELAY_MILLIS, Thread::sleep);
	}

	StartupRetry(long initialDelayMillis, long maxDelayMillis, Sleeper sleeper) {
		this.initialDelayMillis = initialDelayMillis;
		this.maxDelayMillis = maxDelayMillis;
		this.sleeper = sleeper;
	}

	/**
	 * Runs the task, and runs it again after a delay as long as it fails because the database is not reachable.
	 *
	 * @param description What the task does, for logging (e.g., "migrate the database schema").
	 * @return Returns the result of the task.
	 * @throws RuntimeException If the task failed for another reason (the exception of the task, wrapped in an
	 *                          IllegalStateException if it is checked), or if the retries were interrupted.
	 */
	public <T> T call(String description, Task<T> task) {
		Thread waiter = Thread.currentThread();
		Thread shutdownHook = null;
		try {
			for (int attempt = 1; ; attempt++) {
				try {
					T result = task.run();
					if (attempt > 1) {
						log.info("Database reachable again, managed to {} after {} attempts", description, attempt);
					}
					return result;
				} catch (Exception e) {
					if (!isConnectivityFailure(e)) {
						if (e instanceof RuntimeException re) {
							throw re;
						}
						throw new IllegalStateException("Unable to " + description + ": " + e.getMessage(), e);
					}
					long delay = delay(attempt);
					log.warn("Database not reachable, unable to {} (attempt {}), retrying in {} s: {}", description,
							attempt, delay / 1000.0, rootMessage(e));
					if (shutdownHook == null) {
						// a shutdown of the JVM must not wait for the database
						shutdownHook = new Thread(waiter::interrupt, "startup-retry-interrupter");
						Runtime.getRuntime().addShutdownHook(shutdownHook);
					}
					try {
						sleeper.sleep(delay);
					} catch (InterruptedException ie) {
						Thread.currentThread().interrupt();
						throw new IllegalStateException("Interrupted while waiting for the database to " + description, e);
					}
				}
			}
		} finally {
			if (shutdownHook != null) {
				try {
					Runtime.getRuntime().removeShutdownHook(shutdownHook);
				} catch (IllegalStateException e) {
					// the JVM is shutting down already
				}
			}
		}
	}

	/**
	 * @return Returns the delay after the given (1-based) failed attempt.
	 */
	long delay(int attempt) {
		long delay = initialDelayMillis;
		for (int i = 1; i < attempt && delay < maxDelayMillis; i++) {
			delay *= 2;
		}
		return Math.min(delay, maxDelayMillis);
	}

	/**
	 * @return Returns whether the throwable or one of its causes means that the database is (temporarily) not
	 * reachable: a connection exception (SQL state class 08, also for timeouts of connection attempts), the database
	 * shutting down or starting up (57P01, 57P02, 57P03), too many connections (53300), a failed connection attempt
	 * of the network or a connection pool, or Spring's DataAccessResourceFailureException. Authentication failures and
	 * all other SQL errors (e.g., of a migration) are not.
	 */
	public static boolean isConnectivityFailure(Throwable t) {
		for (Throwable c = t; c != null; c = c.getCause()) {
			if (c instanceof SQLException se && se.getSQLState() != null) {
				String state = se.getSQLState();
				if (state.startsWith("08") || UNAVAILABLE_SQL_STATES.contains(state)) {
					return true;
				}
			}
			if (c instanceof SQLTransientConnectionException || c instanceof SQLNonTransientConnectionException ||
					c instanceof DataAccessResourceFailureException || c instanceof ConnectException ||
					c instanceof SocketTimeoutException || c instanceof NoRouteToHostException ||
					c instanceof UnknownHostException) {
				return true;
			}
			if (c.getCause() == c) {
				break;
			}
		}
		return false;
	}

	/**
	 * @return Returns the message of the root cause, with the SQL state of the innermost SQLException, if any; the
	 * messages of wrapping exceptions (e.g., of Flyway) span several lines.
	 */
	private static String rootMessage(Throwable t) {
		Throwable root = t;
		String sqlState = null;
		for (Throwable c = t; c != null; c = c.getCause() == c ? null : c.getCause()) {
			root = c;
			if (c instanceof SQLException se && se.getSQLState() != null) {
				sqlState = se.getSQLState();
			}
		}
		return root.getClass().getSimpleName() + ": " + root.getMessage() + (sqlState != null ? " (SQL state " + sqlState + ")" : "");
	}

}
