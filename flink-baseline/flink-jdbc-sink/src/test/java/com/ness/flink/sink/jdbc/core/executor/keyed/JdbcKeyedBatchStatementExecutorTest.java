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

package com.ness.flink.sink.jdbc.core.executor.keyed;

import com.ness.flink.sink.jdbc.config.JdbcExecutionOptions;
import com.ness.flink.sink.jdbc.core.executor.JdbcStatementBuilder;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ValueState;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class JdbcKeyedBatchStatementExecutorTest {

    private static final String SQL = "INSERT INTO t VALUES (?)";

    @SuppressWarnings("unchecked")
    private JdbcKeyedBatchStatementExecutor<String> executor(ListState<String> state, ValueState<Integer> count,
                                                             JdbcExecutionOptions options) {
        JdbcStatementBuilder<String> builder = (ps, value) -> ps.setString(1, value);
        return new JdbcKeyedBatchStatementExecutor<>(SQL, builder, options, state, count);
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldReportBatchCompletedWhenCountReachesBatchSize() throws Exception {
        ValueState<Integer> count = mock(ValueState.class);
        when(count.value()).thenReturn(50);
        JdbcExecutionOptions options = JdbcExecutionOptions.builder().withBatchSize(50).build();

        JdbcKeyedBatchStatementExecutor<String> executor = executor(mock(ListState.class), count, options);

        Assertions.assertTrue(executor.batchCompleted());
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldNotReportBatchCompletedBelowBatchSize() throws Exception {
        ValueState<Integer> count = mock(ValueState.class);
        when(count.value()).thenReturn(3);
        JdbcExecutionOptions options = JdbcExecutionOptions.builder().withBatchSize(50).build();

        JdbcKeyedBatchStatementExecutor<String> executor = executor(mock(ListState.class), count, options);

        Assertions.assertFalse(executor.batchCompleted());
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldReturnZeroBatchSizeWhenCountNull() throws Exception {
        ValueState<Integer> count = mock(ValueState.class);
        when(count.value()).thenReturn(null);

        JdbcKeyedBatchStatementExecutor<String> executor =
            executor(mock(ListState.class), count, JdbcExecutionOptions.builder().build());

        Assertions.assertEquals(0, executor.getCurrentBatchSize());
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldExecuteAndCommitBatchWhenAutoCommitDisabled() throws Exception {
        ListState<String> state = mock(ListState.class);
        when(state.get()).thenReturn(List.of("a", "b", "c"));
        ValueState<Integer> count = mock(ValueState.class);

        PreparedStatement statement = mock(PreparedStatement.class);
        Connection connection = mock(Connection.class);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(connection.getAutoCommit()).thenReturn(false);

        JdbcKeyedBatchStatementExecutor<String> executor =
            executor(state, count, JdbcExecutionOptions.builder().build());
        executor.init(connection);

        List<String> emitted = executor.executeBatch();

        Assertions.assertEquals(List.of("a", "b", "c"), emitted);
        verify(statement, times(3)).addBatch();
        verify(statement).executeBatch();
        verify(connection).commit();
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldReturnEmptyAndSkipExecuteWhenNoData() throws Exception {
        ListState<String> state = mock(ListState.class);
        when(state.get()).thenReturn(List.of());
        PreparedStatement statement = mock(PreparedStatement.class);
        Connection connection = mock(Connection.class);
        when(connection.prepareStatement(anyString())).thenReturn(statement);

        JdbcKeyedBatchStatementExecutor<String> executor =
            executor(state, mock(ValueState.class), JdbcExecutionOptions.builder().build());
        executor.init(connection);

        List<String> emitted = executor.executeBatch();

        Assertions.assertTrue(emitted.isEmpty());
        verify(statement, never()).executeBatch();
    }
}
