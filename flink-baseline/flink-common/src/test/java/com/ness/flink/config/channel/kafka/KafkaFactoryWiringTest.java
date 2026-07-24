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

package com.ness.flink.config.channel.kafka;

import com.ness.flink.config.channel.EventTimeExtractor;
import com.ness.flink.config.channel.KeyExtractor;
import com.ness.flink.config.operator.DefaultSink;
import com.ness.flink.config.operator.DefaultSource;
import com.ness.flink.config.properties.WatermarkProperties;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.util.ParameterTool;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit coverage for the Kafka source/sink factory wiring ({@link KafkaSourceFactory#sourcePojo} /
 * {@link KafkaSinkFactory#sinkPojo}): they must build a correctly-named, non-null operator definition
 * from properties without touching a Kafka cluster (the actual {@code KafkaSource}/{@code KafkaSink} is
 * only constructed later when the operator is added to a job graph). The Avro-specific paths are exercised
 * end-to-end by the docker-stack ITs; only the POJO wiring is unit-tested here.
 */
class KafkaFactoryWiringTest {

    /** Minimal JSON/POJO domain type for the {@code sourcePojo}/{@code sinkPojo} generics. */
    public static class TestPojo implements Serializable {
        private static final long serialVersionUID = 1L;
        private long id;

        public long getId() {
            return id;
        }

        public void setId(long id) {
            this.id = id;
        }
    }

    /** Concrete factory that only exposes the POJO path (Avro path is out of scope for this unit test). */
    private static final class TestSourceFactory extends KafkaSourceFactory {
        @Override
        public <S extends SpecificRecordBase> DefaultSource<S> sourceAvroSpecific(String sourceName,
                Class<S> domainClass, ParameterTool parameterTool, WatermarkProperties watermarkProperties,
                TimestampAssignerSupplier<S> timestampAssignerFunction) {
            throw new UnsupportedOperationException("not under test");
        }
    }

    private static final class TestSinkFactory extends KafkaSinkFactory {
        @Override
        public <S extends SpecificRecordBase> DefaultSink<S> sinkAvroSpecific(String sinkName,
                Class<S> domainClass, ParameterTool parameterTool, KeyExtractor<S> keyExtractor,
                EventTimeExtractor<S> eventTimeExtractor) {
            throw new UnsupportedOperationException("not under test");
        }
    }

    @Test
    void sourcePojoBuildsNamedSourceWithoutCluster() {
        ParameterTool params = ParameterTool.fromMap(Map.of());
        DefaultSource<TestPojo> source = new TestSourceFactory()
            .sourcePojo("test.source", TestPojo.class, params, WatermarkProperties.from(params), null);

        Assertions.assertNotNull(source);
        Assertions.assertEquals("test.source", source.getName());
    }

    @Test
    void sinkPojoBuildsNamedSinkWithoutCluster() {
        ParameterTool params = ParameterTool.fromMap(Map.of());
        KeyExtractor<TestPojo> keyExtractor = pojo -> String.valueOf(pojo.getId()).getBytes(StandardCharsets.UTF_8);
        DefaultSink<TestPojo> sink = new TestSinkFactory()
            .sinkPojo("test.sink", TestPojo.class, params, keyExtractor, null);

        Assertions.assertNotNull(sink);
        Assertions.assertEquals("test.sink", sink.getName());
    }
}
