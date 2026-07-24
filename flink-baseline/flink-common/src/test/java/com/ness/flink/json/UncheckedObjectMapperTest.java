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

package com.ness.flink.json;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

class UncheckedObjectMapperTest {

    public static class Sample {
        private String name;
        private int value;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getValue() {
            return value;
        }

        public void setValue(int value) {
            this.value = value;
        }
    }

    @Test
    void shouldReturnNullWhenReadingNullString() {
        Assertions.assertNull(UncheckedObjectMapper.MAPPER.readValue((String) null, Sample.class));
    }

    @Test
    void shouldRoundTripStringPayload() {
        Sample source = new Sample();
        source.setName("abc");
        source.setValue(42);

        String json = UncheckedObjectMapper.MAPPER.writeValueAsString(source);
        Sample parsed = UncheckedObjectMapper.MAPPER.readValue(json, Sample.class);

        Assertions.assertEquals("abc", parsed.getName());
        Assertions.assertEquals(42, parsed.getValue());
    }

    @Test
    void shouldRoundTripBytePayload() {
        Sample source = new Sample();
        source.setName("bytes");
        source.setValue(7);

        byte[] bytes = UncheckedObjectMapper.MAPPER.writeValueAsBytes(source);
        Sample parsed = UncheckedObjectMapper.MAPPER.readValue(bytes, Sample.class);

        Assertions.assertEquals("bytes", parsed.getName());
        Assertions.assertEquals(7, parsed.getValue());
    }

    @Test
    void shouldIgnoreUnknownProperties() {
        String json = "{\"name\":\"x\",\"value\":1,\"unexpected\":\"ignored\"}";

        Sample parsed = UncheckedObjectMapper.MAPPER.readValue(
            json.getBytes(StandardCharsets.UTF_8), Sample.class);

        Assertions.assertEquals("x", parsed.getName());
        Assertions.assertEquals(1, parsed.getValue());
    }
}
