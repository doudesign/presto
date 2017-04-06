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
package com.facebook.presto.raptorx;

import com.facebook.presto.Session;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.AbstractTestIntegrationSmokeTest;
import com.facebook.presto.transaction.TransactionId;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.intellij.lang.annotations.Language;
import org.testng.Assert.ThrowingRunnable;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.util.Set;

import static com.facebook.presto.raptorx.RaptorQueryRunner.createRaptorQueryRunner;
import static com.facebook.presto.spi.StandardErrorCode.TRANSACTION_CONFLICT;
import static com.facebook.presto.spi.transaction.IsolationLevel.REPEATABLE_READ;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.MoreFutures.unwrapCompletionException;
import static java.util.stream.Collectors.toSet;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import static org.testng.Assert.fail;

public class TestRaptorIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    public TestRaptorIntegrationSmokeTest()
            throws Exception
    {
        super(() -> createRaptorQueryRunner(ImmutableMap.of(), true, false));
    }

    @BeforeClass
    public void setupSchema()
    {
        assertUpdate("CREATE SCHEMA tpch");
    }

    @Test
    public void testSchemaOperations()
    {
        assertUpdate("CREATE SCHEMA new_schema");
        assertQueryFails("CREATE SCHEMA new_schema", ".* Schema 'raptor\\.new_schema' already exists");

        assertQueryFails("ALTER SCHEMA new_schema RENAME TO new_schema", ".* Target schema 'raptor\\.new_schema' already exists");

        assertUpdate("CREATE SCHEMA new_schema2");
        assertQueryFails("ALTER SCHEMA new_schema RENAME TO new_schema2", ".* Target schema 'raptor\\.new_schema2' already exists");
        assertUpdate("DROP SCHEMA new_schema2");

        assertUpdate("ALTER SCHEMA new_schema RENAME TO new_schema2");
        assertUpdate("ALTER SCHEMA new_schema2 RENAME TO new_schema3");

        assertUpdate("DROP SCHEMA new_schema3");
        assertQueryFails("DROP SCHEMA new_schema3", ".* Schema 'raptor\\.new_schema3' does not exist");
    }

    @Test
    public void testSchemaRollback()
    {
        assertFalse(queryColumn("SHOW SCHEMAS").contains("tx_rollback"));
        try (Transaction tx = new Transaction()) {
            tx.update("CREATE SCHEMA tx_rollback");
            tx.rollback();
        }
        assertFalse(queryColumn("SHOW SCHEMAS").contains("tx_rollback"));
    }

    @Test
    public void testSchemaConflict()
    {
        try (Transaction tx1 = new Transaction(); Transaction tx2 = new Transaction()) {
            tx1.update("CREATE SCHEMA tx_conflict");
            tx2.update("CREATE SCHEMA tx_conflict");

            assertTrue(tx1.queryColumn("SHOW SCHEMAS").contains("tx_conflict"));
            assertTrue(tx2.queryColumn("SHOW SCHEMAS").contains("tx_conflict"));
            assertFalse(queryColumn("SHOW SCHEMAS").contains("tx_conflict"));

            tx1.commit();
            assertConflict(tx2::commit);
        }

        assertTrue(queryColumn("SHOW SCHEMAS").contains("tx_conflict"));
        assertUpdate("DROP SCHEMA tx_conflict");
        assertFalse(queryColumn("SHOW SCHEMAS").contains("tx_conflict"));
    }

    @Test
    public void testSchemaListing()
    {
        assertUpdate("CREATE SCHEMA list1");
        assertUpdate("CREATE SCHEMA list2");
        assertUpdate("CREATE SCHEMA list3");

        assertSetPrefix(queryColumn("SHOW SCHEMAS"), "list", "list1", "list2", "list3");

        try (Transaction tx = new Transaction()) {
            tx.update("CREATE SCHEMA list4");
            tx.update("ALTER SCHEMA list2 RENAME TO list5");
            tx.update("DROP SCHEMA list3");
            assertSetPrefix(tx.queryColumn("SHOW SCHEMAS"), "list", "list1", "list4", "list5");

            tx.update("ALTER SCHEMA list5 RENAME TO list6");
            assertSetPrefix(tx.queryColumn("SHOW SCHEMAS"), "list", "list1", "list4", "list6");

            tx.update("CREATE SCHEMA list2");
            tx.update("CREATE SCHEMA list3");
            tx.update("DROP SCHEMA list1");
            tx.update("DROP SCHEMA list4");
            assertSetPrefix(tx.queryColumn("SHOW SCHEMAS"), "list", "list2", "list3", "list6");

            assertSetPrefix(queryColumn("SHOW SCHEMAS"), "list", "list1", "list2", "list3");

            tx.commit();
        }

        assertUpdate("DROP SCHEMA list2");
        assertUpdate("DROP SCHEMA list3");
        assertUpdate("DROP SCHEMA list6");
    }

    @Test
    public void testTableOperations()
    {
        assertUpdate("CREATE TABLE new_table (x bigint)");
        assertQueryFails("CREATE TABLE new_table (x bigint)", ".* Table 'raptor\\.tpch\\.new_table' already exists");

        assertQueryFails("ALTER TABLE new_table RENAME TO new_table", ".* Target table 'raptor\\.tpch\\.new_table' already exists");

        assertUpdate("CREATE TABLE new_table2 (x bigint)");
        assertQueryFails("ALTER TABLE new_table RENAME TO new_table2", ".* Target table 'raptor\\.tpch\\.new_table2' already exists");
        assertUpdate("DROP TABLE new_table2");

        assertUpdate("ALTER TABLE new_table RENAME TO new_table2");
        assertUpdate("ALTER TABLE new_table2 RENAME TO new_table3");

        assertUpdate("DROP TABLE new_table3");
        assertQueryFails("DROP TABLE new_table3", ".* Table 'raptor\\.tpch\\.new_table3' does not exist");
    }

    private Set<String> queryColumn(@Language("SQL") String sql)
    {
        return computeActual(sql).getOnlyColumnAsSet();
    }

    private static void assertSetPrefix(Set<String> actual, String prefix, String... expected)
    {
        actual = actual.stream()
                .filter(value -> value.startsWith(prefix))
                .collect(toSet());
        assertEquals(actual, ImmutableSet.copyOf(expected));
    }

    private static void assertConflict(ThrowingRunnable runnable)
    {
        PrestoException e = expectThrows(PrestoException.class, runnable);
        assertEquals(e.getErrorCode(), TRANSACTION_CONFLICT.toErrorCode());
    }

    private class Transaction
            implements Closeable
    {
        private final Session session;

        public Transaction()
        {
            TransactionId transactionId = getTransactionManager().beginTransaction(REPEATABLE_READ, false, false);
            session = getSession().beginTransactionId(transactionId, getTransactionManager(), getQueryRunner().getAccessControl());
        }

        public void commit()
        {
            execute(() -> getFutureValue(getTransactionManager().asyncCommit(session.getRequiredTransactionId())));
        }

        public void rollback()
        {
            execute(() -> getFutureValue(getTransactionManager().asyncAbort(session.getRequiredTransactionId())));
        }

        public MaterializedResult query(@Language("SQL") String sql)
        {
            return computeActual(session, sql);
        }

        public Set<String> queryColumn(@Language("SQL") String sql)
        {
            return query(sql).getOnlyColumnAsSet();
        }

        public void update(@Language("SQL") String sql)
        {
            assertUpdate(session, sql);
        }

        private void execute(Runnable runnable)
        {
            try {
                runnable.run();
            }
            catch (Throwable t) {
                t = unwrapCompletionException(t);
                throwIfUnchecked(t);
                throw new RuntimeException(t);
            }
        }

        @Override
        public void close()
        {
            if (getTransactionManager().transactionExists(session.getRequiredTransactionId())) {
                fail("Test did not commit or rollback");
            }
        }

        private TransactionManager getTransactionManager()
        {
            return getQueryRunner().getTransactionManager();
        }
    }
}
