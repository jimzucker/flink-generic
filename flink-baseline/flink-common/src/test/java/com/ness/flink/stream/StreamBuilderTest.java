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

package com.ness.flink.stream;

import com.ness.flink.config.operator.DefaultSource;
import com.ness.flink.config.operator.KeyedProcessorDefinition;
import com.ness.flink.config.properties.OperatorProperties;
import com.ness.flink.stream.test.TestEventString;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.util.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author Khokhlov Pavel
 */
@Slf4j
class StreamBuilderTest {
    private static final Set<TestEventString> VALUES = new HashSet<>();

    @Test
    void shouldExecuteJob() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        ParameterTool params = ParameterTool.fromMap(Collections.emptyMap());
        DefaultSource<String> testSource = new DefaultSource<>("test.source") {
            @Override
            public SingleOutputStreamOperator<String> build(StreamExecutionEnvironment streamExecutionEnvironment) {
                // Flink 2.x: bounded source replacing the removed addSource(SourceFunction).
                return streamExecutionEnvironment.fromData("one", "two");
            }

            @Override
            public Optional<Integer> getMaxParallelism() {
                return Optional.empty();
            }
        };

        StreamBuilder.from(env, params)
            .stream()
            .source(testSource)
            .addKeyedProcessor(new KeyedProcessorDefinition<>(
                OperatorProperties.from("test.processor", params), v -> v, new TestProcessFunction()))
            .addToStream(stream -> stream.map(v -> {
                log.info("Read: value={}", v);
                return v;
            }))
            .addSink(CollectingSink::new).build().run("test.sink");

        Assertions.assertEquals(2, VALUES.size());
        Assertions.assertTrue(
            VALUES.containsAll(Set.of(TestEventString.fromKey("one"), TestEventString.fromKey("two"))));
    }

    /**
     * Sink2 sink that collects values into the static {@link #VALUES} set. Declared {@code static}
     * so it does not capture the (non-serializable) enclosing test instance when Flink serializes it.
     */
    private static final class CollectingSink implements Sink<TestEventString> {
        private static final long serialVersionUID = -2159861918086239581L;

        @Override
        public SinkWriter<TestEventString> createWriter(org.apache.flink.api.connector.sink2.WriterInitContext context) {
            return new SinkWriter<>() {
                @Override
                public void write(TestEventString value, Context ctx) {
                    VALUES.add(value);
                }

                @Override
                public void flush(boolean endOfInput) {
                    // no-op
                }

                @Override
                public void close() {
                    // no-op
                }
            };
        }
    }

    private static class TestProcessFunction extends KeyedProcessFunction<String, String, TestEventString> {
        private static final long serialVersionUID = -2235368612899508484L;

        @Override
        public void processElement(String value, Context ctx, Collector<TestEventString> out) {
            out.collect(TestEventString.fromKey(value));
        }
    }
}