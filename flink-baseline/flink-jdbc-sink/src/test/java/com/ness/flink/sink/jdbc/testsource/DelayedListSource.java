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

package com.ness.flink.sink.jdbc.testsource;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;

/**
 * Minimal bounded Flink 2 {@link Source} for integration tests: emits a fixed list of elements from a single subtask,
 * then stays alive (signalling {@link InputStatus#NOTHING_AVAILABLE}) for {@code keepAliveMs} before ending.
 *
 * <p>Flink 1.x tests used a {@code SourceFunction} that slept after emitting so processing-time timers registered by
 * the operator under test had wall-clock time to fire before the job ended. {@code SourceFunction} was removed in
 * Flink 2.0, and the replacement {@code env.fromData(...)} finishes in milliseconds — Flink does not fire pending
 * processing-time timers on bounded-source finish, so timer-driven emit paths never run. This source restores the
 * "emit then linger" behaviour with the Flink 2 {@code Source} API. Not checkpoint-safe (test scope only).
 *
 * @param <T> element type (must be {@link Serializable})
 */
public final class DelayedListSource<T extends Serializable>
        implements Source<T, DelayedListSource.SingleSplit, Void>, ResultTypeQueryable<T> {

    private static final long serialVersionUID = 1L;

    private final ArrayList<T> elements;
    private final long keepAliveMs;
    private final TypeInformation<T> typeInfo;

    private DelayedListSource(List<T> elements, long keepAliveMs, TypeInformation<T> typeInfo) {
        this.elements = new ArrayList<>(elements);
        this.keepAliveMs = keepAliveMs;
        this.typeInfo = typeInfo;
    }

    @SafeVarargs
    public static <T extends Serializable> DelayedListSource<T> of(Class<T> type, long keepAliveMs, T... elements) {
        return new DelayedListSource<>(List.of(elements), keepAliveMs, TypeInformation.of(type));
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return typeInfo;
    }

    @Override
    public SourceReader<T, SingleSplit> createReader(SourceReaderContext readerContext) {
        return new DelayedReader<>(keepAliveMs);
    }

    @Override
    public SplitEnumerator<SingleSplit, Void> createEnumerator(SplitEnumeratorContext<SingleSplit> enumContext) {
        // A single split carrying every element is handed to the first reader that registers; other subtasks get
        // nothing. The one active reader keeps the whole job alive for keepAliveMs.
        return new SingleSplitEnumerator(enumContext, new SingleSplit(elements));
    }

    @Override
    public SplitEnumerator<SingleSplit, Void> restoreEnumerator(SplitEnumeratorContext<SingleSplit> enumContext,
            Void checkpoint) {
        return createEnumerator(enumContext);
    }

    @Override
    public SimpleVersionedSerializer<SingleSplit> getSplitSerializer() {
        return new JavaSerializer<>();
    }

    @Override
    public SimpleVersionedSerializer<Void> getEnumeratorCheckpointSerializer() {
        return new JavaSerializer<>();
    }

    /** Split carrying the full element list (single-split source). */
    public static final class SingleSplit implements SourceSplit, Serializable {
        private static final long serialVersionUID = 1L;
        private final ArrayList<?> elements;

        SingleSplit(List<?> elements) {
            this.elements = new ArrayList<>(elements);
        }

        @Override
        public String splitId() {
            return "delayed-list-split";
        }
    }

    private static final class DelayedReader<T> implements SourceReader<T, SingleSplit> {
        private final long keepAliveMs;
        private List<T> pending;
        private int idx;
        private long lingerDeadline = -1L;
        private volatile boolean noMoreSplits;

        DelayedReader(long keepAliveMs) {
            this.keepAliveMs = keepAliveMs;
        }

        @Override
        public void start() {
            // no-op
        }

        @Override
        @SuppressWarnings("unchecked")
        public InputStatus pollNext(ReaderOutput<T> output) {
            if (pending != null && idx < pending.size()) {
                output.collect(pending.get(idx++));
                return InputStatus.MORE_AVAILABLE;
            }
            if (pending == null && !noMoreSplits) {
                // still waiting for the split (or for the "no more splits" signal)
                return InputStatus.NOTHING_AVAILABLE;
            }
            // All elements emitted (or this subtask got no split): linger so processing-time timers can fire.
            if (lingerDeadline < 0) {
                lingerDeadline = System.currentTimeMillis() + keepAliveMs;
            }
            if (System.currentTimeMillis() >= lingerDeadline) {
                return InputStatus.END_OF_INPUT;
            }
            return InputStatus.NOTHING_AVAILABLE;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void addSplits(List<SingleSplit> splits) {
            if (!splits.isEmpty()) {
                this.pending = (List<T>) new ArrayList<>(splits.get(0).elements);
            }
        }

        @Override
        public void notifyNoMoreSplits() {
            this.noMoreSplits = true;
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            if (pending != null && idx < pending.size()) {
                return CompletableFuture.completedFuture(null);
            }
            // Re-poll shortly so the linger deadline (and end-of-input) is re-evaluated without busy-spinning.
            return CompletableFuture.supplyAsync(() -> null,
                CompletableFuture.delayedExecutor(50, TimeUnit.MILLISECONDS));
        }

        @Override
        public List<SingleSplit> snapshotState(long checkpointId) {
            return List.of();
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            // no-op
        }

        @Override
        public void close() {
            // no-op
        }
    }

    private static final class SingleSplitEnumerator implements SplitEnumerator<SingleSplit, Void> {
        private final SplitEnumeratorContext<SingleSplit> context;
        private SingleSplit split;

        SingleSplitEnumerator(SplitEnumeratorContext<SingleSplit> context, SingleSplit split) {
            this.context = context;
            this.split = split;
        }

        @Override
        public void start() {
            // no-op
        }

        @Override
        public void handleSplitRequest(int subtaskId, String requesterHostname) {
            if (split != null) {
                context.assignSplit(split, subtaskId);
                split = null;
            }
            context.signalNoMoreSplits(subtaskId);
        }

        @Override
        public void addSplitsBack(List<SingleSplit> splits, int subtaskId) {
            if (!splits.isEmpty()) {
                this.split = splits.get(0);
            }
        }

        @Override
        public void addReader(int subtaskId) {
            // Push the split eagerly to the first registered reader so a source that never issues an explicit
            // split request still receives the data.
            if (split != null) {
                context.assignSplit(split, subtaskId);
                split = null;
            }
            context.signalNoMoreSplits(subtaskId);
        }

        @Override
        public Void snapshotState(long checkpointId) {
            return null;
        }

        @Override
        public void close() {
            // no-op
        }
    }

    /** Java-serialization based versioned serializer for the test split / {@code Void} checkpoint. */
    private static final class JavaSerializer<X> implements SimpleVersionedSerializer<X> {
        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(X obj) throws java.io.IOException {
            try (java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
                 java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(bos)) {
                oos.writeObject(obj);
                oos.flush();
                return bos.toByteArray();
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public X deserialize(int version, byte[] serialized) throws java.io.IOException {
            try (java.io.ObjectInputStream ois =
                     new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(serialized))) {
                return (X) ois.readObject();
            } catch (ClassNotFoundException e) {
                throw new java.io.IOException(e);
            }
        }
    }
}
