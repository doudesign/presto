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

import javax.inject.Inject;

import java.util.Collection;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class DatabaseMetadata
        implements Metadata
{
    private final SequenceManager sequenceManager;
    private final ReaderDao dao;

    @Inject
    public DatabaseMetadata(SequenceManager sequenceManager, Database database)
    {
        this.sequenceManager = requireNonNull(sequenceManager, "sequenceManager is null");
        this.dao = new DBI(database.getMasterConnection()).onDemand(ReaderDao.class);
    }

    @Override
    public long getCurrentCommitId()
    {
        return dao.getCurrentCommitId();
    }

    @Override
    public long nextTransactionId()
    {
        return sequenceManager.nextValue("transaction_id", 1000);
    }

    @Override
    public long nextDistributionId()
    {
        return sequenceManager.nextValue("distribution_id", 1);
    }

    @Override
    public long nextSchemaId()
    {
        return sequenceManager.nextValue("schema_id", 1);
    }

    @Override
    public long nextTableId()
    {
        return sequenceManager.nextValue("table_id", 10);
    }

    @Override
    public long nextColumnId()
    {
        return sequenceManager.nextValue("column_id", 1000);
    }

    @Override
    public Optional<Long> getSchemaId(long commitId, String schemaName)
    {
        return Optional.ofNullable(dao.getSchemaId(commitId, schemaName.getBytes(UTF_8)));
    }

    @Override
    public Optional<Long> getTableId(long commitId, long schemaId, String tableName)
    {
        return Optional.ofNullable(dao.getTableId(commitId, schemaId, tableName.getBytes(UTF_8)));
    }

    @Override
    public Collection<String> listSchemas(long commitId)
    {
        return dao.listSchemas(commitId).stream()
                .map(bytes -> new String(bytes, UTF_8))
                .collect(toList());
    }

    @Override
    public Collection<String> listTables(long commitId, long schemaId)
    {
        return dao.listTables(commitId).stream()
                .map(bytes -> new String(bytes, UTF_8))
                .collect(toList());
    }

    @Override
    public Optional<TableInfo> getTableInfo(long commitId, long tableId)
    {
        return Optional.ofNullable(dao.getTableInfo(commitId, tableId));
    }
}
