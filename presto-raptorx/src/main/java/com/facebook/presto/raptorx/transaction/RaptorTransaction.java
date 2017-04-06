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
package com.facebook.presto.raptorx.transaction;

import com.facebook.presto.raptorx.RaptorTransactionHandle;
import com.facebook.presto.raptorx.metadata.Metadata;
import com.facebook.presto.raptorx.metadata.TableInfo;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.SetMultimap;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.facebook.presto.raptorx.RaptorErrorCode.RAPTOR_METADATA_ERROR;
import static com.facebook.presto.raptorx.util.Predicates.not;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

@NotThreadSafe
public class RaptorTransaction
{
    private final Metadata metadata;

    private final long transactionId;
    private final long commitId;

    private final List<Action> actions = new ArrayList<>();

    private final Map<String, Optional<Long>> schemaIds = new HashMap<>();
    private final Set<String> schemasAdded = new HashSet<>();
    private final Set<String> schemasRemoved = new HashSet<>();

    private final Map<Long, Optional<TableInfo>> tableInfos = new HashMap<>();
    private final Map<Long, Map<String, Optional<Long>>> tableIds = new HashMap<>();
    private final SetMultimap<Long, String> tablesAdded = HashMultimap.create();
    private final SetMultimap<Long, String> tablesRemoved = HashMultimap.create();

    public RaptorTransaction(Metadata metadata, long transactionId, long commitId)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.transactionId = transactionId;
        this.commitId = commitId;
    }

    public List<Action> getActions()
    {
        return ImmutableList.copyOf(actions);
    }

    public RaptorTransactionHandle getHandle()
    {
        return new RaptorTransactionHandle(transactionId, commitId);
    }

    public long nextDistributionId()
    {
        return metadata.nextDistributionId();
    }

    public long nextTableId()
    {
        return metadata.nextTableId();
    }

    public long nextColumnId()
    {
        return metadata.nextColumnId();
    }

    public Optional<Long> getTableId(SchemaTableName table)
    {
        return getSchemaId(table.getSchemaName())
                .flatMap(schemaId -> getTableId(schemaId, table.getTableName()));
    }

    public Optional<Long> getSchemaId(String schemaName)
    {
        return schemaIds.computeIfAbsent(schemaName, x -> metadata.getSchemaId(commitId, schemaName));
    }

    private Optional<Long> getTableId(long schemaId, String tableName)
    {
        return tableIds.computeIfAbsent(schemaId, x -> new HashMap<>())
                .computeIfAbsent(tableName, x -> metadata.getTableId(commitId, schemaId, tableName));
    }

    public List<String> listSchemas()
    {
        return Stream.concat(
                metadata.listSchemas(commitId).stream(),
                schemasAdded.stream())
                .filter(not(schemasRemoved::contains))
                .sorted()
                .collect(toList());
    }

    public List<SchemaTableName> listTables()
    {
        // TODO: this should fetch all tables at once
        return listSchemas().stream()
                .flatMap(schema -> listTables(schema).stream())
                .collect(toList());
    }

    public List<SchemaTableName> listTables(String schemaName)
    {
        return getSchemaId(schemaName)
                .map(schemaId -> Stream.concat(
                        metadata.listTables(commitId, schemaId).stream(),
                        tablesAdded.get(schemaId).stream())
                        .filter(not(tablesRemoved.get(schemaId)::contains))
                        .sorted()
                        .map(tableName -> new SchemaTableName(schemaName, tableName))
                        .collect(toList()))
                .orElse(ImmutableList.of());
    }

    public void createSchema(String schemaName)
    {
        long schemaId = metadata.nextSchemaId();
        actions.add(new CreateSchemaAction(schemaId, schemaName));

        schemaIds.put(schemaName, Optional.of(schemaId));
        schemasAdded.add(schemaName);
        schemasRemoved.remove(schemaName);
    }

    public void dropSchema(String schemaName)
    {
        long schemaId = getRequiredSchemaId(schemaName);
        actions.add(new DropSchemaAction(schemaId));

        schemaIds.put(schemaName, Optional.empty());
        schemasAdded.remove(schemaName);
        schemasRemoved.add(schemaName);
    }

    public void renameSchema(String source, String target)
    {
        long schemaId = getRequiredSchemaId(source);
        actions.add(new RenameSchemaAction(schemaId, target));

        schemaIds.put(source, Optional.empty());
        schemasAdded.remove(source);
        schemasRemoved.add(source);

        schemaIds.put(target, Optional.of(schemaId));
        schemasAdded.add(target);
        schemasRemoved.remove(target);
    }

    public void createDistribution(long distributionId, int bucketCount, List<Long> nodes)
    {
        checkArgument(bucketCount == nodes.size(), "bucket count does not match node count");
        actions.add(new CreateDistributionAction(distributionId, bucketCount, nodes));
    }

    public void createTable(long schemaId, String tableName, long distributionId, long createTime, long rowCount)
    {
        long tableId = metadata.nextTableId();
        TableInfo info = new TableInfo.Builder()
                .setTableId(tableId)
                .setTableName(tableName)
                .setSchemaId(schemaId)
                .setDistributionId(distributionId)
                .setCreateTime(createTime)
                .setUpdateTime(createTime)
                .setRowCount(rowCount)
                .build();
        actions.add(new CreateTableAction(info));

        tableInfos.put(tableId, Optional.of(info));
        tableIds.computeIfAbsent(schemaId, x -> new HashMap<>())
                .put(tableName, Optional.of(tableId));
        tablesAdded.put(schemaId, tableName);
        tablesRemoved.remove(schemaId, tableName);
    }

    public void dropTable(long tableId)
    {
        TableInfo info = getTableInfo(tableId);
        actions.add(new DropTableAction(tableId));

        tableInfos.put(tableId, Optional.empty());
        tableIds.computeIfAbsent(info.getSchemaId(), x -> new HashMap<>())
                .put(info.getTableName(), Optional.empty());
        tablesAdded.remove(info.getSchemaId(), info.getTableName());
        tablesRemoved.put(info.getSchemaId(), info.getTableName());
    }

    public void renameTable(long tableId, SchemaTableName newName)
    {
        TableInfo oldInfo = getTableInfo(tableId);
        long newSchemaId = getRequiredSchemaId(newName.getSchemaName());
        TableInfo newInfo = new TableInfo.Builder(oldInfo)
                .setTableName(newName.getTableName())
                .setSchemaId(newSchemaId)
                .setUpdateTime(System.currentTimeMillis())
                .build();

        actions.add(new RenameTableAction(tableId, newName.getTableName(), newSchemaId));

        tableInfos.put(tableId, Optional.of(newInfo));

        tableIds.computeIfAbsent(oldInfo.getSchemaId(), x -> new HashMap<>())
                .put(oldInfo.getTableName(), Optional.empty());
        tablesAdded.remove(oldInfo.getSchemaId(), oldInfo.getTableName());
        tablesRemoved.put(oldInfo.getSchemaId(), oldInfo.getTableName());

        tableIds.computeIfAbsent(newSchemaId, x -> new HashMap<>())
                .put(newName.getTableName(), Optional.of(tableId));
        tablesAdded.put(newSchemaId, newName.getTableName());
        tablesRemoved.remove(newSchemaId, newName.getTableName());
    }

    private TableInfo getTableInfo(long tableId)
    {
        return tableInfos.computeIfAbsent(tableId, x -> metadata.getTableInfo(commitId, tableId))
                .orElseThrow(() -> new PrestoException(RAPTOR_METADATA_ERROR, "Table ID is invalid: " + tableId));
    }

    private long getRequiredSchemaId(String schemaName)
    {
        return getSchemaId(schemaName)
                .orElseThrow(() -> new IllegalArgumentException("schema does not exist: " + schemaName));
    }
}
