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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the check of the PostgreSQL version at startup (DEPL-4.01).
 * Specification: specs/tests/10-configuration.md
 */
class PgRowStoreServerVersionTest {

    @Test
    @DisplayName("TC-DS-003: PostgreSQL older than 9.6 prevents the startup")
    void tooOldVersionFails() {
        assertThatThrownBy(() -> PgRowStore.checkServerVersion(90400))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("PostgreSQL 9.6 or later is required")
                .hasMessageContaining("runs PostgreSQL 9.4");
        assertThatThrownBy(() -> PgRowStore.checkServerVersion(90524))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("runs PostgreSQL 9.5");
    }

    @Test
    @DisplayName("TC-DS-003: PostgreSQL 9.6 and later and an unknown version pass the check")
    void supportedVersionPasses() {
        for (Integer version : new Integer[]{90600, 90624, 100023, 160004, null}) {
            assertThatCode(() -> PgRowStore.checkServerVersion(version)).as(String.valueOf(version)).doesNotThrowAnyException();
        }
    }

    @Test
    @DisplayName("TC-DS-003: Version names follow the numbering of PostgreSQL")
    void versionNames() {
        assertThat(PgRowStore.versionName(90624)).isEqualTo("9.6");
        assertThat(PgRowStore.versionName(100023)).isEqualTo("10");
        assertThat(PgRowStore.versionName(160004)).isEqualTo("16");
    }

}
