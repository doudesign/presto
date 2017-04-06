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
import com.facebook.presto.spi.PrestoException;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.exceptions.CallbackFailedException;

import javax.inject.Inject;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;

import static com.facebook.presto.spi.StandardErrorCode.TRANSACTION_CONFLICT;
import static com.google.common.base.Throwables.propagateIfPossible;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * The single writer of all transactional metadata.
 * <p>
 * The first step of each protocol action is to acquire a lock on the
 * {@code current_commit} row. This serves as a global lock within the
 * database and thus guarantees no intervening database writes are possible.
 * <p>
 * Commit protocol:
 * <ul>
 * <li>Starting a commit:</li>
 * <ul>
 * <li>Verify that {@code active_commit} row does not exist</li>
 * <li>Allocate {@code commitId} and verify it is larger than the current commit</li>
 * <li>Insert {@code active_commit} row</li>
 * </ul>
 * <li>Writing to the master:</li>
 * <ul>
 * <li>Verify {@code active_commit} row has correct {@code commitId} and is not rolling back</li>
 * <li>Perform write operation</li>
 * </ul>
 * <li>Finishing a commit:</li>
 * <ul>
 * <li>Verify {@code active_commit} row</li>
 * <li>Delete {@code active_commit} row</li>
 * <li>Insert {@code commits} row</li>
 * <li>Update {@code current_commit} row</li>
 * </ul>
 * </ul>
 * Rollback protocol:
 * <ul>
 * <li>Starting a rollback:</li>
 * <ul>
 * <li>Mark {@code active_commit} row as aborted</li>
 * </ul>
 * <li>Writing to the master:</li>
 * <ul>
 * <li>Verify {@code active_commit} row has correct {@code commitId}</li>
 * <li>Perform write operation</li>
 * </ul>
 * <li>Finishing a rollback:</li>
 * <ul>
 * <li>Verify {@code active_commit} row</li>
 * <li>Delete {@code active_commit} row</li>
 * </ul>
 * </ul>
 */
public class DatabaseMetadataWriter
        implements MetadataWriter
{
    private final SequenceManager sequenceManager;
    private final IDBI dbi;

    @Inject
    public DatabaseMetadataWriter(SequenceManager sequenceManager, Database database)
    {
        this.sequenceManager = requireNonNull(sequenceManager, "sequenceManager is null");
        this.dbi = new DBI(database.getMasterConnection());
    }

    @Override
    public void recover()
    {
        dbi.useTransaction((handle, status) -> {
            WriterDao dao = handle.attach(WriterDao.class);

            // acquire global lock
            dao.getLockedCurrentCommitId();

            // abort current commit
            ActiveCommit commit = dao.getActiveCommit();
            if (commit == null) {
                return;
            }
            if (!commit.isRollingBack()) {
                dao.abortActiveCommit();
            }

            // rollback master
            dao.rollback(commit.getCommitId());

            // finish rollback
            deleteActiveCommit(dao);
        });
    }

    @Override
    public long beginCommit()
    {
        return dbi.inTransaction((handle, status) -> {
            WriterDao dao = handle.attach(WriterDao.class);

            // acquire global lock
            long currentCommitId = dao.getLockedCurrentCommitId();

            // verify no commit in progress
            if (dao.getActiveCommit() != null) {
                throw new RuntimeException("Commit already in progress");
            }

            // allocate commit ID
            long commitId = sequenceManager.nextValue("commit_id", 10);
            if (commitId <= currentCommitId) {
                throw new RuntimeException("Invalid next commit ID: " + commitId);
            }

            // start commit
            dao.insertActiveCommit(commitId, System.currentTimeMillis());

            return commitId;
        });
    }

    @Override
    public void finishCommit(long commitId)
    {
        write(commitId, (dao, commit) -> {
            deleteActiveCommit(dao);

            dao.insertCommit(commitId, commit.getStartTime());

            if (dao.updateCurrentCommit(commitId) != 1) {
                throw new RuntimeException("Wrong row count for current_commit update");
            }
        });
    }

    @Override
    public void createSchema(long commitId, long schemaId, String schemaName)
    {
        write(commitId, (dao, commit) -> {
            byte[] name = schemaName.getBytes(UTF_8);
            checkConflict(!dao.schemaNameExists(name), "Schema already exists: %s", schemaName);
            dao.insertSchema(commitId, schemaId, name);
        });
    }

    @Override
    public void renameSchema(long commitId, long schemaId, String newSchemaName)
    {
        write(commitId, (dao, commit) -> {
            byte[] newName = newSchemaName.getBytes(UTF_8);
            checkConflict(dao.schemaIdExists(schemaId), "Schema no longer exists");
            checkConflict(!dao.schemaNameExists(newName), "Schema already exists: %s", newSchemaName);
            dao.deleteSchema(commitId, schemaId);
            dao.insertSchema(commitId, schemaId, newName);
        });
    }

    @Override
    public void dropSchema(long commitId, long schemaId)
    {
        write(commitId, (dao, commit) -> {
            checkConflict(dao.schemaIdExists(schemaId), "Schema no longer exists");
            dao.deleteSchema(commitId, schemaId);
        });
    }

    @Override
    public void createDistribution(long commitId, long distributionId, int bucketCount, List<Long> nodes)
    {
        write(commitId, (dao, commit) -> {
            dao.createDistribution(distributionId, bucketCount);
            dao.insertBucketNodes(
                    distributionId,
                    IntStream.range(0, bucketCount).boxed().collect(toList()),
                    nodes);
        });
    }

    @Override
    public void createTable(long commitId, TableInfo table)
    {
        write(commitId, (dao, commit) -> {
            checkConflict(dao.distributionIdExists(table.getDistributionId()), "Distribution no longer exists");
            checkConflict(!dao.tableNameExists(table.getSchemaId(), table.getTableName().getBytes(UTF_8)), "Table already exists: %s", table.getTableName());
            insertTable(dao, commitId, table);
        });
    }

    @Override
    public void renameTable(long commitId, long tableId, String tableName, long schemaId)
    {
        write(commitId, (dao, commit) -> {
            checkConflict(dao.tableIdExists(tableId), "Table no longer exists");
            checkConflict(!dao.tableNameExists(schemaId, tableName.getBytes(UTF_8)), "Table already exists: %s", tableName);
            TableInfo table = dao.getTableInfo(tableId);
            dao.deleteTable(commitId, tableId);
            insertTable(dao, commitId, new TableInfo.Builder(table)
                    .setTableId(tableId)
                    .setTableName(tableName)
                    .setSchemaId(schemaId)
                    .setUpdateTime(System.currentTimeMillis())
                    .build());
        });
    }

    @Override
    public void dropTable(long commitId, long tableId)
    {
        write(commitId, (dao, commit) -> {
            checkConflict(dao.tableIdExists(tableId), "Table no longer exists");
            dao.deleteTable(commitId, tableId);
        });
    }

    private void write(long commitId, BiConsumer<WriterDao, ActiveCommit> writer)
    {
        try {
            dbi.useTransaction((handle, status) -> {
                WriterDao dao = handle.attach(WriterDao.class);
                ActiveCommit commit = getLockedActiveCommit(dao, commitId);
                writer.accept(dao, commit);
            });
        }
        catch (CallbackFailedException e) {
            propagateIfPossible(e.getCause(), PrestoException.class);
            throw e;
        }
    }

    private static void insertTable(WriterDao dao, long commitId, TableInfo table)
    {
        dao.insertTable(
                commitId,
                table.getTableId(),
                table.getTableName().getBytes(UTF_8),
                table.getSchemaId(),
                table.getDistributionId(),
                null,
                table.getCreateTime(),
                table.getUpdateTime(),
                table.getRowCount());
    }

    private static void deleteActiveCommit(WriterDao dao)
    {
        if (dao.deleteActiveCommit() != 1) {
            throw new RuntimeException("Wrong row count for active_commit delete");
        }
    }

    private static ActiveCommit getLockedActiveCommit(WriterDao dao, long commitId)
    {
        // acquire global lock
        dao.getLockedCurrentCommitId();

        // verify that we are still committing
        ActiveCommit commit = dao.getActiveCommit();
        if ((commit == null) || (commit.getCommitId() != commitId) || commit.isRollingBack()) {
            throw new RuntimeException("Commit is no longer active: " + commitId);
        }

        return commit;
    }

    public static void checkConflict(boolean condition, String format, Object... args)
    {
        if (!condition) {
            throw new PrestoException(TRANSACTION_CONFLICT, format(format, args));
        }
    }
}
