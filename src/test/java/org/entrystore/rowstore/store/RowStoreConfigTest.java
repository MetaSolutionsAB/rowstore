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

package org.entrystore.rowstore.store;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for parsing and validation of rowstore.json in {@link RowStoreConfig}.
 */
@DisplayName("RowStoreConfig Tests")
class RowStoreConfigTest {

    private static final String FULL_CONFIG = """
            {
                "baseurl": "https://rowstore.example.com/",
                "regexpqueries": "simple",
                "maxetlprocesses": 3,
                "loglevel": "debug",
                "legacyparser": true,
                "querytimeout": 30,
                "querymaxlimit": 500,
                "maxuploadsize": 209715200,
                "cors": { "allowedorigins": ["https://a.example.com", "https://b.example.com"] },
                "database": {
                    "type": "postgresql",
                    "host": "db-primary",
                    "port": 5433,
                    "database": "rowstore",
                    "user": "app",
                    "password": "secret",
                    "ssl": true,
                    "connectionPoolInit": 2,
                    "connectionPoolMax": 10,
                    "socketTimeout": 60,
                    "connectTimeout": 7,
                    "loginTimeout": 8
                },
                "queryDatabase": {
                    "host": "db-replica",
                    "database": "rowstore_ro",
                    "user": "reader",
                    "password": "readerpw"
                },
                "ratelimit": {
                    "type": "average",
                    "timerange": 60,
                    "global": 1000,
                    "dataset": 100,
                    "clientip": 50
                }
            }
            """;

    private static final String MINIMAL_CONFIG = """
            {
                "baseurl": "http://localhost:8282/",
                "database": { "host": "localhost", "database": "rowstore", "user": "rowstore", "password": "pw" }
            }
            """;

    @Test
    @DisplayName("TC-CFG-001: Valid full config is exposed by all getters")
    void validFullConfig_allGettersReturnConfiguredValues() {
        RowStoreConfig config = new RowStoreConfig(new JSONObject(FULL_CONFIG));

        assertThat(config.getBaseURL()).isEqualTo("https://rowstore.example.com/");
        assertThat(config.getRegexpQuerySupport()).isEqualTo(Dataset.REGEXP_QUERY_SIMPLE);
        assertThat(config.getMaxEtlProcesses()).isEqualTo(3);
        assertThat(config.getLogLevel()).isEqualTo("debug");
        assertThat(config.isLegacyParserEnabled()).isTrue();
        assertThat(config.getQueryTimeout()).isEqualTo(30);
        assertThat(config.getQueryMaxLimit()).isEqualTo(500);
        assertThat(config.getMaxUploadSize()).isEqualTo(209715200L);
        assertThat(config.getCorsAllowedOrigins()).containsExactly("https://a.example.com", "https://b.example.com");

        assertThat(config.getRateLimitType()).isEqualTo("average");
        assertThat(config.getRateLimitTimeRange()).isEqualTo(60);
        assertThat(config.getRateLimitRequestsGlobal()).isEqualTo(1000);
        assertThat(config.getRateLimitRequestsDataset()).isEqualTo(100);
        assertThat(config.getRateLimitRequestsClientIP()).isEqualTo(50);

        RowStoreConfig.Database db = config.getDatabase();
        assertThat(db.getType()).isEqualTo("postgresql");
        assertThat(db.getHost()).isEqualTo("db-primary");
        assertThat(db.getPort()).isEqualTo(5433);
        assertThat(db.getName()).isEqualTo("rowstore");
        assertThat(db.getUser()).isEqualTo("app");
        assertThat(db.getPassword()).isEqualTo("secret");
        assertThat(db.getSsl()).isTrue();
        assertThat(db.getConnectionPoolInit()).isEqualTo(2);
        assertThat(db.getConnectionPoolMax()).isEqualTo(10);
        assertThat(db.getSocketTimeout()).isEqualTo(60);
        assertThat(db.getConnectTimeout()).isEqualTo(7);
        assertThat(db.getLoginTimeout()).isEqualTo(8);

        RowStoreConfig.Database queryDb = config.getQueryDatabase();
        assertThat(queryDb).isNotSameAs(db);
        assertThat(queryDb.getHost()).isEqualTo("db-replica");
        assertThat(queryDb.getName()).isEqualTo("rowstore_ro");
        assertThat(queryDb.getUser()).isEqualTo("reader");
        assertThat(queryDb.getPassword()).isEqualTo("readerpw");
    }

    @Test
    @DisplayName("TC-CFG-002: Minimal config applies documented defaults")
    void minimalConfig_appliesDefaults() {
        RowStoreConfig config = new RowStoreConfig(new JSONObject(MINIMAL_CONFIG));

        assertThat(config.getRegexpQuerySupport()).isEqualTo(Dataset.REGEXP_QUERY_DISABLED);
        assertThat(config.getMaxEtlProcesses()).isEqualTo(5);
        assertThat(config.getLogLevel()).isEqualTo("info");
        assertThat(config.isLegacyParserEnabled()).isFalse();
        assertThat(config.getQueryTimeout()).isEqualTo(-1);
        assertThat(config.getQueryMaxLimit()).isEqualTo(100);
        assertThat(config.getMaxUploadSize()).isEqualTo(104857600L);
        assertThat(config.getCorsAllowedOrigins()).containsExactly("*");
        assertThat(config.getRateLimitTimeRange()).isEqualTo(-1);

        RowStoreConfig.Database db = config.getDatabase();
        assertThat(db.getType()).isEqualTo("postgresql");
        assertThat(db.getPort()).isEqualTo(5432);
        assertThat(db.getSsl()).isFalse();
        assertThat(db.getConnectionPoolInit()).isEqualTo(-1);
        assertThat(db.getConnectionPoolMax()).isEqualTo(-1);
        assertThat(db.getSocketTimeout()).isZero();
        assertThat(db.getConnectTimeout()).isEqualTo(5);
        assertThat(db.getLoginTimeout()).isEqualTo(5);

        // Without queryDatabase, queries use the primary database
        assertThat(config.getQueryDatabase()).isSameAs(db);
    }

    @Test
    @DisplayName("TC-CFG-003: Empty database password is accepted (e.g. trust authentication)")
    void emptyPassword_isAccepted() {
        JSONObject json = new JSONObject(MINIMAL_CONFIG);
        json.getJSONObject("database").put("password", "");

        assertThat(new RowStoreConfig(json).getDatabase().getPassword()).isEmpty();
    }

    @ParameterizedTest(name = "missing {0}")
    @ValueSource(strings = {
            "baseurl",
            "database",
            "database.host",
            "database.database",
            "database.user",
            "database.password",
            "queryDatabase.host",
            "queryDatabase.database",
            "queryDatabase.user",
            "queryDatabase.password"
    })
    @DisplayName("TC-CFG-004: Missing required key fails and names the key")
    void missingRequiredKey_failsNamingKey(String key) {
        JSONObject json = new JSONObject(FULL_CONFIG);
        remove(json, key);

        assertThatThrownBy(() -> new RowStoreConfig(json))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("missing required key '" + key + "'");
    }

    @ParameterizedTest(name = "{0} = {1}")
    @CsvSource(delimiter = '|', value = {
            "ratelimit              | \"yes\"",
            "cors                   | \"x\"",
            "cors.allowedorigins    | \"x\"",
            "cors.allowedorigins    | [1]",
            "database               | \"localhost\"",
            "queryDatabase          | true",
            "baseurl                | 42",
            "database.password      | 12345",
            "queryDatabase.host     | 5"
    })
    @DisplayName("TC-CFG-005: Key with wrong type fails instead of silently using defaults")
    void wrongType_failsNamingKey(String key, String jsonValue) {
        JSONObject json = new JSONObject(FULL_CONFIG);
        put(json, key, new JSONObject("{\"v\": " + jsonValue + "}").get("v"));

        assertThatThrownBy(() -> new RowStoreConfig(json))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageStartingWith("rowstore.json")
                .hasMessageContaining("'" + key + "'");
    }

    @ParameterizedTest(name = "{0} = {1}")
    @CsvSource({
            "querymaxlimit, 0",
            "querymaxlimit, -1",
            "maxuploadsize, 0",
            "maxuploadsize, -5"
    })
    @DisplayName("TC-CFG-006: Non-positive querymaxlimit or maxuploadsize fails and names the key")
    void nonPositiveLimit_failsNamingKey(String key, long value) {
        JSONObject json = new JSONObject(FULL_CONFIG);
        json.put(key, value);

        assertThatThrownBy(() -> new RowStoreConfig(json))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(key);
    }

    @Test
    @DisplayName("TC-CFG-007: Unknown and retired keys (e.g. exportpagesize) are ignored")
    void unknownKeys_areIgnored() {
        JSONObject json = new JSONObject(MINIMAL_CONFIG);
        json.put("exportpagesize", 50000);
        json.put("somethingelse", "value");

        assertThat(new RowStoreConfig(json).getBaseURL()).isEqualTo("http://localhost:8282/");
    }

    @Test
    @DisplayName("TC-CFG-008: trustedproxies defaults to private ranges, accepts IPs and CIDR ranges, empty array trusts nobody")
    void trustedProxies_defaultExplicitAndEmpty() {
        assertThat(new RowStoreConfig(new JSONObject(MINIMAL_CONFIG)).getTrustedProxies())
                .containsExactlyInAnyOrder("127.0.0.0/8", "10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16",
                        "169.254.0.0/16", "100.64.0.0/10", "::1", "fc00::/7", "fe80::/10");

        JSONObject json = new JSONObject(MINIMAL_CONFIG);
        json.put("trustedproxies", new JSONArray(List.of("192.0.2.10", "10.10.0.0/16", "2001:db8::/32", "::1")));
        assertThat(new RowStoreConfig(json).getTrustedProxies())
                .containsExactly("192.0.2.10", "10.10.0.0/16", "2001:db8::/32", "::1");

        json.put("trustedproxies", new JSONArray());
        assertThat(new RowStoreConfig(json).getTrustedProxies()).isEmpty();
    }

    @ParameterizedTest(name = "trustedproxies = {0}")
    @ValueSource(strings = {
            "\"10.0.0.1\"",
            "{}",
            "[1]",
            "[\"10.0.0.1\", null]",
            "[\"not-an-ip\"]",
            "[\"localhost\"]",
            "[\"10.0.0\"]",
            "[\"10.0.0.256\"]",
            "[\"10.0.0.0/33\"]",
            "[\"10.0.0.0/-1\"]",
            "[\"10.0.0.0/abc\"]",
            "[\"10.0.0.0/8;80\"]",
            "[\"::1/129\"]",
            "[\"\"]"
    })
    @DisplayName("TC-CFG-009: trustedproxies with wrong type or an entry that is not an IP address or CIDR range fails")
    void trustedProxies_invalid_failsNamingKey(String jsonValue) {
        JSONObject json = new JSONObject(MINIMAL_CONFIG);
        json.put("trustedproxies", new JSONObject("{\"v\": " + jsonValue + "}").get("v"));

        assertThatThrownBy(() -> new RowStoreConfig(json))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageStartingWith("rowstore.json")
                .hasMessageContaining("'trustedproxies'");
    }

    private static void remove(JSONObject json, String dottedKey) {
        int dot = dottedKey.indexOf('.');
        if (dot < 0) {
            json.remove(dottedKey);
        } else {
            json.getJSONObject(dottedKey.substring(0, dot)).remove(dottedKey.substring(dot + 1));
        }
    }

    private static void put(JSONObject json, String dottedKey, Object value) {
        int dot = dottedKey.indexOf('.');
        if (dot < 0) {
            json.put(dottedKey, value);
        } else {
            json.getJSONObject(dottedKey.substring(0, dot)).put(dottedKey.substring(dot + 1), value);
        }
    }

}
