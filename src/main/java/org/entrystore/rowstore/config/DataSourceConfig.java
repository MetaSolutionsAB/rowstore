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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.entrystore.rowstore.store.RowStoreConfig;
import org.flywaydb.core.Flyway;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import javax.sql.DataSource;
import java.util.concurrent.TimeUnit;

@Configuration
public class DataSourceConfig {

	private static final Logger log = LoggerFactory.getLogger(DataSourceConfig.class);

	@Bean
	@Primary
	public DataSource dataSource(RowStoreConfig config) {
		DataSource ds = createDataSource(config.getDatabase(), "rowstore-primary", false);

		// Run Flyway
		Flyway flyway = Flyway.configure()
				.dataSource(ds)
				.baselineOnMigrate(true)
				.baselineVersion("0")
				.locations("classpath:db/migration")
				.load();
		flyway.migrate();

		return ds;
	}

	@Bean
	@Qualifier("queryDataSource")
	public DataSource queryDataSource(RowStoreConfig config, DataSource dataSource) {
		if (config.getDatabase() == config.getQueryDatabase()) {
			return dataSource;
		}

		return createDataSource(config.getQueryDatabase(), "rowstore-query", true);
	}

	/**
	 * @param readOnly if true, connections are read-only also in auto-commit mode (readOnlyMode=always)
	 */
	private DataSource createDataSource(RowStoreConfig.Database dbConfig, String poolName, boolean readOnly) {
		if (dbConfig.getConnectionPoolMax() > 0) {
			return createHikariDataSource(dbConfig, poolName, readOnly);
		}
		return createSimpleDataSource(dbConfig, poolName, readOnly);
	}

	private PGSimpleDataSource createSimpleDataSource(RowStoreConfig.Database dbConfig, String poolName, boolean readOnly) {
		PGSimpleDataSource ds = new PGSimpleDataSource();
		ds.setUser(dbConfig.getUser());
		ds.setPassword(dbConfig.getPassword());
		ds.setServerNames(new String[]{dbConfig.getHost()});
		ds.setDatabaseName(dbConfig.getName());
		ds.setPortNumbers(new int[]{dbConfig.getPort()});
		// ssl=false disables SSL, since sslmode=prefer can cause connection timeouts if SSL is not wanted
		ds.setSsl(dbConfig.getSsl());
		ds.setSslMode(dbConfig.getSslMode());
		ds.setLogUnclosedConnections(log.isDebugEnabled());
		// 0 means no timeout, as in 1.x
		if (dbConfig.getConnectTimeout() >= 0) {
			ds.setConnectTimeout(dbConfig.getConnectTimeout());
		}
		if (dbConfig.getLoginTimeout() >= 0) {
			try {
				ds.setLoginTimeout(dbConfig.getLoginTimeout());
			} catch (Exception e) {
				log.warn("Failed to set login timeout on PGSimpleDataSource", e);
			}
		}
		if (dbConfig.getSocketTimeout() >= 0) {
			ds.setSocketTimeout(dbConfig.getSocketTimeout());
		}
		if (readOnly) {
			ds.setReadOnly(true);
			ds.setReadOnlyMode("always");
		}
		log.info("Configured PGSimpleDataSource '{}': host={}, port={}, database={} (no connection pool)",
				poolName, dbConfig.getHost(), dbConfig.getPort(), dbConfig.getName());
		return ds;
	}

	private HikariDataSource createHikariDataSource(RowStoreConfig.Database dbConfig, String poolName, boolean readOnly) {
		HikariConfig hikariConfig = new HikariConfig();
		hikariConfig.setPoolName(poolName);

		String jdbcUrl = String.format("jdbc:postgresql://%s:%d/%s",
				dbConfig.getHost(), dbConfig.getPort(), dbConfig.getName());
		hikariConfig.setJdbcUrl(jdbcUrl);
		hikariConfig.setUsername(dbConfig.getUser());
		hikariConfig.setPassword(dbConfig.getPassword());

		hikariConfig.setMaximumPoolSize(dbConfig.getConnectionPoolMax());
		hikariConfig.setMinimumIdle(dbConfig.getConnectionPoolInit() > 0 ? dbConfig.getConnectionPoolInit() : 0);

		// Lifecycle tuning
		hikariConfig.setMaxLifetime(TimeUnit.MINUTES.toMillis(25));
		hikariConfig.setIdleTimeout(TimeUnit.SECONDS.toMillis(10));
		hikariConfig.setKeepaliveTime(TimeUnit.SECONDS.toMillis(120));
		hikariConfig.setLeakDetectionThreshold(TimeUnit.SECONDS.toMillis(60));

		if (dbConfig.getConnectTimeout() > 0) {
			hikariConfig.setConnectionTimeout(dbConfig.getConnectTimeout() * 1000L);
		}

		// ssl=false disables SSL, since sslmode=prefer can cause connection timeouts if SSL is not wanted
		hikariConfig.addDataSourceProperty("ssl", String.valueOf(dbConfig.getSsl()));
		hikariConfig.addDataSourceProperty("sslmode", dbConfig.getSslMode());

		// Driver timeouts in seconds (0 means no timeout, as in 1.x); connectionTimeout above only limits
		// waiting for a pooled connection
		if (dbConfig.getConnectTimeout() >= 0) {
			hikariConfig.addDataSourceProperty("connectTimeout", String.valueOf(dbConfig.getConnectTimeout()));
		}
		if (dbConfig.getLoginTimeout() >= 0) {
			hikariConfig.addDataSourceProperty("loginTimeout", String.valueOf(dbConfig.getLoginTimeout()));
		}
		if (dbConfig.getSocketTimeout() >= 0) {
			hikariConfig.addDataSourceProperty("socketTimeout", String.valueOf(dbConfig.getSocketTimeout()));
		}

		// Must be configured before the pool is started
		if (readOnly) {
			hikariConfig.setReadOnly(true);
			hikariConfig.addDataSourceProperty("readOnlyMode", "always");
		}

		log.info("Configured HikariCP pool '{}': url={}, maxPoolSize={}, minIdle={}",
				poolName, jdbcUrl, hikariConfig.getMaximumPoolSize(), hikariConfig.getMinimumIdle());

		return new HikariDataSource(hikariConfig);
	}

}
