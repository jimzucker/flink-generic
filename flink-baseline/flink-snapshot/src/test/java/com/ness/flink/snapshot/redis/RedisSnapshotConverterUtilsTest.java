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

package com.ness.flink.snapshot.redis;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;

class RedisSnapshotConverterUtilsTest {

    public static class Sample implements Serializable {
        private static final long serialVersionUID = 1L;
        private String name;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    @Test
    void shouldSplitContextIdFromJsonPayload() {
        byte[] bytes = "42:{\"name\":\"abc\"}".getBytes(StandardCharsets.UTF_8);

        SnapshotData<Sample> result = RedisSnapshotConverterUtils.convertTo(Sample.class, bytes);

        Assertions.assertEquals(42L, result.getContextId());
        Assertions.assertEquals("abc", result.getElement().getName());
    }

    @Test
    void shouldHandleJsonPayloadContainingDelimiter() {
        // The payload itself contains ':' — only the first delimiter separates the context id.
        byte[] bytes = "7:{\"name\":\"a:b:c\"}".getBytes(StandardCharsets.UTF_8);

        SnapshotData<Sample> result = RedisSnapshotConverterUtils.convertTo(Sample.class, bytes);

        Assertions.assertEquals(7L, result.getContextId());
        Assertions.assertEquals("a:b:c", result.getElement().getName());
    }

    @Test
    void shouldFailOnNonNumericContextId() {
        byte[] bytes = "notANumber:{\"name\":\"x\"}".getBytes(StandardCharsets.UTF_8);

        Assertions.assertThrows(NumberFormatException.class,
            () -> RedisSnapshotConverterUtils.convertTo(Sample.class, bytes));
    }
}
