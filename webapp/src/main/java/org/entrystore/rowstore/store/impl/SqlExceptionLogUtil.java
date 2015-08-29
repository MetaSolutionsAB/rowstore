package org.entrystore.rowstore.store.impl;

import org.slf4j.Logger;

import java.sql.SQLException;

/**
 * @author Hannes Ebner
 */
public class SqlExceptionLogUtil {

	public static void error(Logger log, SQLException exception) {
		if (log == null || exception == null) {
			throw new IllegalArgumentException("Arguments must not be null");
		}
		log.error(exception.getMessage());
		if (exception.getNextException() != null) {
			error(log, exception.getNextException());
		}
	}

}