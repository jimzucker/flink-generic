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

package com.ness.flink.sink.jdbc.connector;

import com.ness.flink.sink.jdbc.config.JdbcConnectionOptions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class SimpleJdbcConnectionProviderTest {

    private static final StubDriver STUB = new StubDriver();

    @BeforeAll
    static void registerDriver() throws SQLException {
        DriverManager.registerDriver(STUB);
    }

    @AfterAll
    static void deregisterDriver() throws SQLException {
        DriverManager.deregisterDriver(STUB);
    }

    @BeforeEach
    void reset() {
        StubDriver.nextConnection = null;
    }

    private static JdbcConnectionOptions.JdbcConnectionOptionsBuilder driverProvided() {
        return JdbcConnectionOptions.builder()
            .withDbURL("jdbc:stub://db")
            .withDriverName(StubDriver.class.getName())
            .withUsername("user");
    }

    @Test
    void shouldConnectViaProvidedDriverAndApplyAutoCommit() throws Exception {
        Connection connection = mock(Connection.class);
        StubDriver.nextConnection = connection;

        SimpleJdbcConnectionProvider provider =
            new SimpleJdbcConnectionProvider(driverProvided().withAutoCommit(false).build());

        Connection result = provider.getConnection();

        Assertions.assertSame(connection, result);
        verify(connection).setAutoCommit(false);
    }

    @Test
    void shouldThrowNoSuitableDriverWhenDriverReturnsNull() {
        StubDriver.nextConnection = null; // driver.connect(...) returns null

        SimpleJdbcConnectionProvider provider =
            new SimpleJdbcConnectionProvider(driverProvided().build());

        SQLException ex = Assertions.assertThrows(SQLException.class, provider::getConnection);
        Assertions.assertEquals("08001", ex.getSQLState());
    }

    /** Minimal JDBC driver whose connect() returns a configurable connection; acceptsURL is false
     * so it never hijacks DriverManager.getConnection for unrelated URLs. */
    public static final class StubDriver implements Driver {
        static Connection nextConnection;

        @Override
        public Connection connect(String url, Properties info) {
            return nextConnection;
        }

        @Override
        public boolean acceptsURL(String url) {
            return false;
        }

        @Override
        public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
            return new DriverPropertyInfo[0];
        }

        @Override
        public int getMajorVersion() {
            return 1;
        }

        @Override
        public int getMinorVersion() {
            return 0;
        }

        @Override
        public boolean jdbcCompliant() {
            return false;
        }

        @Override
        public Logger getParentLogger() {
            return Logger.getGlobal();
        }
    }
}
