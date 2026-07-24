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

package com.ness.flink.schema;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.Objects;

class PojoSchemaRoundTripTest {

    public static class Payload implements Serializable {
        private static final long serialVersionUID = 1L;
        private String id;
        private long amount;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public long getAmount() {
            return amount;
        }

        public void setAmount(long amount) {
            this.amount = amount;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Payload)) {
                return false;
            }
            Payload payload = (Payload) o;
            return amount == payload.amount && Objects.equals(id, payload.id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, amount);
        }
    }

    @Test
    void shouldRoundTripThroughSerializeAndDeserialize() {
        Payload source = new Payload();
        source.setId("abc-1");
        source.setAmount(123L);

        PojoSerializationSchema<Payload> serializer = new PojoSerializationSchema<>();
        PojoDeserializationSchema<Payload> deserializer = new PojoDeserializationSchema<>(Payload.class);

        byte[] bytes = serializer.serialize(source);
        Payload result = deserializer.deserialize(bytes);

        Assertions.assertEquals(source, result);
    }

    @Test
    void deserializerShouldNeverSignalEndOfStream() {
        PojoDeserializationSchema<Payload> deserializer = new PojoDeserializationSchema<>(Payload.class);
        Assertions.assertFalse(deserializer.isEndOfStream(new Payload()));
    }

    @Test
    void deserializerShouldExposeProducedType() {
        PojoDeserializationSchema<Payload> deserializer = new PojoDeserializationSchema<>(Payload.class);
        TypeInformation<Payload> type = deserializer.getProducedType();
        Assertions.assertEquals(Payload.class, type.getTypeClass());
    }
}
