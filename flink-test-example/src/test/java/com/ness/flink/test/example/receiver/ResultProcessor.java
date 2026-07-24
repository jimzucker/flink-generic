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

package com.ness.flink.test.example.receiver;

import lombok.AllArgsConstructor;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;

import java.util.function.BiFunction;

/**
 * Terminal Kafka Streams processor (Kafka 4.x processor.api): consumes {@code Record<String, T>} and
 * forwards nothing downstream (KOut/VOut = Void).
 *
 * @author Khokhlov Pavel
 */
@AllArgsConstructor
public class ResultProcessor<T, R> extends ContextualProcessor<String, T, Void, Void> {

    private final ResultService<R> resultService;
    private final BiFunction<Record<String, T>, T, R> transformFunction;
    private final BiFunction<String, R, String> keyTransformFunction;

    /**
     *
     * @param resultService service registers new message from Kafka
     * @param transformFunction transformation of incoming message: accepts the input {@link Record} and the
     *                          original Kafka message value, should return the transformed message
     */
    public ResultProcessor(ResultService<R> resultService, BiFunction<Record<String, T>, T, R> transformFunction) {
        this(resultService, transformFunction, null);
    }

    @Override
    public void process(Record<String, T> record) {
        R transformed = transformFunction.apply(record, record.value());
        String key = record.key();
        if (keyTransformFunction != null) {
            key = keyTransformFunction.apply(key, transformed);
        }
        resultService.process(key, transformed);
    }
}
