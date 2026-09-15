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

package com.ness.flink.sink.jdbc.core.output.keyed;

import java.io.Serial;
import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;

/**
 * Operator wrapper around {@link KeyedJdbcProcessFunction} that guarantees the final partial batch is
 * flushed and emitted when input ends.
 *
 * <p>The underlying function only flushes a key's buffered batch when it reaches {@code batchSize} or
 * when a per-key processing-time timer fires. On a bounded stream (or a drained/stopped job) the task
 * shuts down as soon as the source is exhausted, and pending processing-time timers are not guaranteed
 * to fire first — so records buffered in an incomplete batch would be silently dropped (and never
 * emitted downstream, since {@code KeyedProcessFunction.close()} has no {@code Collector}). This
 * mirrors the flush-on-close the non-keyed {@code JdbcBatchingOutputFormat} already performs.
 *
 * <p>{@link BoundedOneInput#endInput()} runs once all input is processed, with the output still open,
 * so here we iterate every key that has buffered state and drain it deterministically.
 */
@Internal
public class KeyedJdbcProcessOperator<K, I, O> extends KeyedProcessOperator<K, I, O> implements BoundedOneInput {

    @Serial
    private static final long serialVersionUID = 6845277752726684911L;

    private final KeyedJdbcProcessFunction<K, I, O> function;

    public KeyedJdbcProcessOperator(KeyedJdbcProcessFunction<K, I, O> function) {
        super(function);
        this.function = function;
    }

    @Override
    public void endInput() throws Exception {
        TimestampedCollector<O> collector = new TimestampedCollector<>(output);
        // applyToAllKeys sets the current key context before each callback, so the function's keyed
        // state (its per-key buffered batch) resolves to the right key inside flushRemaining.
        getKeyedStateBackend().applyToAllKeys(
            VoidNamespace.INSTANCE,
            VoidNamespaceSerializer.INSTANCE,
            function.getDataStateDescriptor(),
            (key, state) -> function.flushRemaining(collector));
    }
}
