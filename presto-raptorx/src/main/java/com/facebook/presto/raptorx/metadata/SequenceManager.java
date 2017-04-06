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
package com.facebook.presto.raptorx.metadata;

import com.facebook.presto.raptorx.util.Database;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.GetGeneratedKeys;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.Transaction;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.raptorx.util.DatabaseUtil.runIgnoringConstraintViolation;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

@ThreadSafe
public class SequenceManager
{
    private final SequenceDao dao;

    @GuardedBy("this")
    private final Map<String, CacheEntry> cache = new HashMap<>();

    @Inject
    public SequenceManager(Database database)
    {
        this.dao = createSequenceDao(database);
    }

    public synchronized long nextValue(String name, int cacheCount)
    {
        checkArgument(cacheCount > 0, "cacheCount must be greater than zero");

        CacheEntry entry = cache.get(name);
        if (entry == null) {
            long value = dao.getNext(name, cacheCount);
            if (cacheCount == 1) {
                return value;
            }

            entry = new CacheEntry(value, cacheCount);
            cache.put(name, entry);
        }

        long value = entry.nextValue();
        if (entry.isExhausted()) {
            cache.remove(name);
        }
        return value;
    }

    private static SequenceDao createSequenceDao(Database database)
    {
        IDBI dbi = new DBI(database.getMasterConnection());
        switch (database.getType()) {
            case H2:
                return dbi.onDemand(H2SequenceDao.class);
            case MYSQL:
                return dbi.onDemand(MySqlSequenceDao.class);
        }
        throw new AssertionError("Unhandled database: " + database.getType());
    }

    private interface SequenceDao
    {
        long getNext(String name, int increment);
    }

    private interface MySqlSequenceDao
            extends SequenceDao
    {
        @Override
        @SqlUpdate("INSERT INTO sequences (sequence_name, next_value)\n" +
                "VALUES (:name, last_insert_id(1) + :increment)\n" +
                "ON DUPLICATE KEY UPDATE\n" +
                "next_value = last_insert_id(next_value) + :increment")
        @GetGeneratedKeys
        long getNext(@Bind("name") String name, @Bind("increment") int increment);
    }

    private interface H2SequenceDao
            extends SequenceDao
    {
        @SqlUpdate("INSERT INTO sequences (sequence_name, next_value) VALUES (:name, 1)")
        void insertSequence(@Bind("name") String name);

        @SqlQuery("SELECT next_value FROM sequences WHERE sequence_name = :name FOR UPDATE")
        long getNextValueLocked(@Bind("name") String name);

        @SqlUpdate("UPDATE sequences SET next_value = :value WHERE sequence_name = :name")
        void updateNextValue(@Bind("name") String name, @Bind("value") long value);

        @Override
        @Transaction
        default long getNext(String name, int increment)
        {
            runIgnoringConstraintViolation(() -> insertSequence(name));
            long value = getNextValueLocked(name);
            updateNextValue(name, value + increment);
            return value;
        }
    }

    private static class CacheEntry
    {
        private long nextValue;
        private long remainingValues;

        public CacheEntry(long nextValue, long remainingValues)
        {
            checkArgument(nextValue > 0, "nextValue must be greater than zero");
            checkArgument(remainingValues >= 0, "remainingValues is negative");
            this.nextValue = nextValue;
            this.remainingValues = remainingValues;
        }

        public long nextValue()
        {
            checkState(!isExhausted(), "cache entry is exhausted");
            remainingValues--;
            long value = nextValue;
            nextValue++;
            return value;
        }

        public boolean isExhausted()
        {
            return remainingValues == 0;
        }
    }
}
