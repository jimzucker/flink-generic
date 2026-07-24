/*
 * Copyright 2021-2023 Ness Digital Engineering
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

package com.ness.flink.snapshot;

import com.ness.flink.snapshot.context.ContextMetadata;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SnapshotMapperTest {

    /** Minimal concrete mapper to exercise the base-class key-building logic. */
    private static final class TestMapper extends SnapshotMapper<String> {
        private static final long serialVersionUID = 1L;

        TestMapper(String delimiter) {
            super(delimiter);
        }

        @Override
        public String buildKey(String data, ContextMetadata ctx) {
            return buildSnapshotPrefix(ctx) + data;
        }

        @Override
        public String getValueFromData(String data) {
            return data;
        }
    }

    private static ContextMetadata ctx() {
        return ContextMetadata.builder()
            .contextId(1L)
            .date("20240101")
            .contextName("prices")
            .build();
    }

    @Test
    void shouldBuildSnapshotPrefixFromContextName() {
        TestMapper mapper = new TestMapper(":");
        Assertions.assertEquals("snapshot:prices", mapper.buildSnapshotPrefix(ctx()));
    }

    @Test
    void shouldBuildSnapshotIndexKeyFromPrefix() {
        TestMapper mapper = new TestMapper(":");
        Assertions.assertEquals("snapshot:prices:index", mapper.buildSnapshotIndexKey(ctx()));
    }

    @Test
    void shouldHonorCustomDelimiter() {
        TestMapper mapper = new TestMapper("|");
        Assertions.assertEquals("snapshot|prices", mapper.buildSnapshotPrefix(ctx()));
        Assertions.assertEquals("snapshot|prices|index", mapper.buildSnapshotIndexKey(ctx()));
    }
}
