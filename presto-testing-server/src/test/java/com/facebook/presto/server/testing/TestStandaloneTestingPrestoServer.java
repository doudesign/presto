/*
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
package com.facebook.presto.server.testing;

import org.testng.annotations.Test;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestStandaloneTestingPrestoServer
{
    @Test
    public void testServer()
            throws Exception
    {
        try (StandaloneTestingPrestoServer server = new StandaloneTestingPrestoServer()) {
            assertEquals(server.getJdbcUrl().substring(0, 12), "jdbc:presto:");
            assertEquals(server.getBaseUri().getPort(), URI.create(server.getJdbcUrl().substring(5)).getPort());

            try (Connection connection = DriverManager.getConnection(server.getJdbcUrl(), "test", "")) {
                try (Statement statement = connection.createStatement()) {
                    try (ResultSet rs = statement.executeQuery("SELECT count(*) FROM tpch.tiny.orders")) {
                        assertTrue(rs.next());
                        assertEquals(rs.getLong(1), 15000);
                        assertFalse(rs.next());
                    }
                }
            }
        }
    }
}
