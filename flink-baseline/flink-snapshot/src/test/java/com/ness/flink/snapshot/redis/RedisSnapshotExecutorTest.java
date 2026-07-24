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

import com.ness.flink.domain.TimeAware;
import com.ness.flink.snapshot.SnapshotMapper;
import com.ness.flink.snapshot.context.ContextMetadata;
import com.ness.flink.snapshot.context.ContextService;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RedisSnapshotExecutorTest {

    /** Concrete mapper returning fixed key/value so the Redis interaction can be verified. */
    private static final class StubMapper extends SnapshotMapper<TimeAware> {
        private static final long serialVersionUID = 1L;

        StubMapper() {
            super(":");
        }

        @Override
        public String buildKey(TimeAware data, ContextMetadata ctx) {
            return "snapshot-key";
        }

        @Override
        public String getValueFromData(TimeAware data) {
            return "snapshot-value";
        }
    }

    private static ContextMetadata ctx() {
        return ContextMetadata.builder().contextId(5L).date("20240101").contextName("ctx").build();
    }

    @SuppressWarnings("unchecked")
    private static RedisCommands<byte[], byte[]> wire(StatefulRedisConnection<byte[], byte[]> connection,
                                                      boolean discarded) {
        RedisCommands<byte[], byte[]> commands = mock(RedisCommands.class);
        when(connection.sync()).thenReturn(commands);
        TransactionResult result = mock(TransactionResult.class);
        when(result.wasDiscarded()).thenReturn(discarded);
        when(commands.exec()).thenReturn(result);
        return commands;
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldWriteSnapshotInSingleTransaction() throws Exception {
        ContextService contextService = mock(ContextService.class);
        when(contextService.generate(any(), any())).thenReturn(ctx());
        StatefulRedisConnection<byte[], byte[]> connection = mock(StatefulRedisConnection.class);
        RedisCommands<byte[], byte[]> commands = wire(connection, false);

        RedisSnapshotExecutor<TimeAware> executor = new RedisSnapshotExecutor<>(new StubMapper());
        Assertions.assertDoesNotThrow(() ->
            executor.execute(() -> 1_000L, contextService, "ctx", connection));

        verify(commands).multi();
        verify(commands).psetex(any(byte[].class), anyLong(), any(byte[].class));
        verify(commands).zadd(any(byte[].class), any(), eq(5.0d), any(byte[].class));
        verify(commands).exec();
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldThrowWhenTransactionDiscarded() {
        ContextService contextService = mock(ContextService.class);
        when(contextService.generate(any(), any())).thenReturn(ctx());
        StatefulRedisConnection<byte[], byte[]> connection = mock(StatefulRedisConnection.class);
        wire(connection, true);

        RedisSnapshotExecutor<TimeAware> executor = new RedisSnapshotExecutor<>(new StubMapper());

        Assertions.assertThrows(IOException.class,
            () -> executor.execute(() -> 1_000L, contextService, "ctx", connection));
    }

    @Test
    void expireAtShouldBeInTheFuture() {
        long ttl = new RedisSnapshotExecutor<>(new StubMapper()).expireAt();
        Assertions.assertTrue(ttl > 0, "expireAt must return a positive TTL");
    }
}
