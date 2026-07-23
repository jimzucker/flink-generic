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

package com.ness.flink.sink.jdbc.core.executor;

import com.ness.flink.sink.jdbc.config.JdbcExecutionOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SimpleBatchStatementExecutorTest {

    private static final String SQL = "INSERT INTO t VALUES (?)";

    private SimpleBatchStatementExecutor<String, String> newExecutor(PreparedStatement statement) throws SQLException {
        Connection connection = mock(Connection.class);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        JdbcExecutionOptions options = JdbcExecutionOptions.builder().build();
        JdbcStatementBuilder<String> builder = (ps, value) -> ps.setString(1, value);
        SimpleBatchStatementExecutor<String, String> executor =
            new SimpleBatchStatementExecutor<>(SQL, builder, Function.identity(), options);
        executor.init(connection);
        return executor;
    }

    @Test
    void shouldPrepareStatementOnInit() throws SQLException {
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        when(connection.prepareStatement(SQL)).thenReturn(statement);

        JdbcExecutionOptions options = JdbcExecutionOptions.builder().build();
        SimpleBatchStatementExecutor<String, String> executor = new SimpleBatchStatementExecutor<>(
            SQL, (ps, value) -> ps.setString(1, value), Function.identity(), options);
        executor.init(connection);

        verify(connection).prepareStatement(SQL);
    }

    @Test
    void shouldExecuteBatchAndCommitWhenAutoCommitDisabled() throws SQLException {
        PreparedStatement statement = mock(PreparedStatement.class);
        Connection connection = mock(Connection.class);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(connection.getAutoCommit()).thenReturn(false);

        JdbcExecutionOptions options = JdbcExecutionOptions.builder().build();
        SimpleBatchStatementExecutor<String, String> executor = new SimpleBatchStatementExecutor<>(
            SQL, (ps, value) -> ps.setString(1, value), Function.identity(), options);
        executor.init(connection);

        executor.addToBatch("a");
        executor.addToBatch("b");
        executor.executeBatch();

        verify(statement, times(2)).addBatch();
        verify(statement).executeBatch();
        verify(connection).commit();
    }

    @Test
    void shouldNotCommitWhenAutoCommitEnabled() throws SQLException {
        PreparedStatement statement = mock(PreparedStatement.class);
        Connection connection = mock(Connection.class);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(connection.getAutoCommit()).thenReturn(true);

        JdbcExecutionOptions options = JdbcExecutionOptions.builder().build();
        SimpleBatchStatementExecutor<String, String> executor = new SimpleBatchStatementExecutor<>(
            SQL, (ps, value) -> ps.setString(1, value), Function.identity(), options);
        executor.init(connection);

        executor.addToBatch("only");
        executor.executeBatch();

        verify(statement).executeBatch();
        verify(connection, never()).commit();
    }

    @Test
    void shouldDoNothingWhenBatchEmpty() throws SQLException {
        PreparedStatement statement = mock(PreparedStatement.class);
        SimpleBatchStatementExecutor<String, String> executor = newExecutor(statement);

        executor.executeBatch();

        verify(statement, never()).addBatch();
        verify(statement, never()).executeBatch();
    }

    @Test
    void shouldCloseStatementOnClose() throws SQLException {
        PreparedStatement statement = mock(PreparedStatement.class);
        SimpleBatchStatementExecutor<String, String> executor = newExecutor(statement);

        executor.close();

        verify(statement).close();
    }
}
