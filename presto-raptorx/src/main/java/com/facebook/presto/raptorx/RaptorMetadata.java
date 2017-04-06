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

import com.facebook.presto.raptorx.metadata.NodeSupplier;
import com.facebook.presto.raptorx.metadata.RaptorNode;
import com.facebook.presto.raptorx.transaction.RaptorTransaction;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorNewTableLayout;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.airlift.slice.Slice;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static com.facebook.presto.raptorx.CreateDistributionProcedure.MAX_BUCKETS;
import static com.facebook.presto.raptorx.RaptorTableProperties.getBucketCount;
import static com.facebook.presto.raptorx.RaptorTableProperties.getSortColumns;
import static com.facebook.presto.raptorx.RaptorTableProperties.getTemporalColumn;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

public class RaptorMetadata
        implements ConnectorMetadata
{
    private final NodeSupplier nodeSupplier;
    private final RaptorTransaction transaction;
    private final AtomicReference<NewTableInfo> newTableInfo = new AtomicReference<>();

    public RaptorMetadata(NodeSupplier nodeSupplier, RaptorTransaction transaction)
    {
        this.nodeSupplier = requireNonNull(nodeSupplier, "nodeSupplier is null");
        this.transaction = requireNonNull(transaction, "transaction is null");
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        return transaction.getSchemaId(schemaName).isPresent();
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return transaction.listSchemas();
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties)
    {
        transaction.createSchema(schemaName);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        transaction.dropSchema(schemaName);
    }

    @Override
    public void renameSchema(ConnectorSession session, String source, String target)
    {
        transaction.renameSchema(source, target);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        return transaction.getTableId(tableName)
                .map(RaptorTableHandle::new)
                .orElse(null);
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        RaptorTableHandle handle = (RaptorTableHandle) table;
        ConnectorTableLayout layout = getTableLayout(session, handle, constraint.getSummary());
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        RaptorTableLayoutHandle raptorHandle = (RaptorTableLayoutHandle) handle;
        return getTableLayout(session, raptorHandle.getTable(), raptorHandle.getConstraint());
    }

    private ConnectorTableLayout getTableLayout(ConnectorSession session, RaptorTableHandle handle, TupleDomain<ColumnHandle> constraint)
    {
        return new ConnectorTableLayout(new RaptorTableLayoutHandle(handle, constraint, Optional.empty()));
    }

    @Override
    public Optional<ConnectorNewTableLayout> getNewTableLayout(ConnectorSession session, ConnectorTableMetadata metadata)
    {
        Set<Long> nodes = nodeSupplier.getRequiredWorkerNodes().stream()
                .map(RaptorNode::getNodeId)
                .collect(toSet());

        List<String> partitionColumns = ImmutableList.of();
        long distributionId = transaction.nextDistributionId();
        int bucketCount = nodes.size() * 8;
        OptionalInt declaredBucketCount = getBucketCount(metadata.getProperties());
        if (declaredBucketCount.isPresent()) {
            bucketCount = declaredBucketCount.getAsInt();
            if (bucketCount == 0) {
                throw new PrestoException(INVALID_TABLE_PROPERTY, "Bucket count must be greater than zero");
            }
            if (bucketCount > MAX_BUCKETS) {
                throw new PrestoException(INVALID_TABLE_PROPERTY, "Bucket count must be no more than " + MAX_BUCKETS);
            }
        }

        Iterator<Long> iterator = cyclingShuffledIterator(nodes);
        List<Long> newDistributionNodes = IntStream.range(0, bucketCount)
                .mapToObj(bucket -> iterator.next())
                .collect(toList());

        NewTableInfo info = new NewTableInfo(distributionId, bucketCount, newDistributionNodes);
        checkState(newTableInfo.compareAndSet(null, info), "newTableInfo was already set");

        Map<Integer, Long> bucketToNode = IntStream.range(0, bucketCount)
                .mapToObj(bucket -> bucket)
                .collect(toMap(bucket -> bucket, newDistributionNodes::get));

        ConnectorPartitioningHandle partitioning = new RaptorPartitioningHandle(distributionId, bucketToNode);
        return Optional.of(new ConnectorNewTableLayout(partitioning, partitionColumns));
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        if (schemaNameOrNull != null) {
            return transaction.listTables(schemaNameOrNull);
        }
        return transaction.listTables();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        RaptorColumnHandle column = (RaptorColumnHandle) columnHandle;
        return new ColumnMetadata(column.getColumnName(), column.getColumnType(), null, column.isHidden());
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        Optional<ConnectorNewTableLayout> layout = getNewTableLayout(session, tableMetadata);
        finishCreateTable(session, beginCreateTable(session, tableMetadata, layout), ImmutableList.of());
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        transaction.dropTable(((RaptorTableHandle) tableHandle).getTableId());
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        if (!schemaExists(session, newTableName.getSchemaName())) {
            throw new SchemaNotFoundException(newTableName.getSchemaName());
        }
        transaction.renameTable(((RaptorTableHandle) tableHandle).getTableId(), newTableName);
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        long schemaId = transaction.getSchemaId(tableMetadata.getTable().getSchemaName())
                .orElseThrow(() -> new SchemaNotFoundException(tableMetadata.getTable().getSchemaName()));
        long tableId = transaction.nextTableId();

        List<RaptorColumnHandle> columnHandles = tableMetadata.getColumns().stream()
                .map(column -> new RaptorColumnHandle(transaction.nextColumnId(), column.getName(), column.getType()))
                .collect(toList());

        Map<String, RaptorColumnHandle> columnHandleMap = uniqueIndex(columnHandles, RaptorColumnHandle::getColumnName);

        List<RaptorColumnHandle> sortColumnHandles = getSortColumnHandles(getSortColumns(tableMetadata.getProperties()), columnHandleMap);
        Optional<RaptorColumnHandle> temporalColumnHandle = getTemporalColumnHandle(getTemporalColumn(tableMetadata.getProperties()), columnHandleMap);

        if (temporalColumnHandle.isPresent()) {
            RaptorColumnHandle column = temporalColumnHandle.get();
            if (!column.getColumnType().equals(TIMESTAMP) && !column.getColumnType().equals(DATE)) {
                throw new PrestoException(NOT_SUPPORTED, "Temporal column must be of type timestamp or date: " + column.getColumnName());
            }
        }

        NewTableInfo info = newTableInfo.getAndSet(null);
        checkState(info != null, "newTableInfo was not set");

        // TODO allocate transaction ID
        long transactionId = 0;

        return new RaptorOutputTableHandle(
                transactionId,
                schemaId,
                tableId,
                tableMetadata.getTable().getTableName(),
                columnHandles,
                sortColumnHandles,
                temporalColumnHandle,
                info.getNewDistributionNodes(),
                info.getDistributionId(),
                info.getBucketCount(),
                ImmutableList.of());
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments)
    {
        if (!fragments.isEmpty()) {
            // TODO
            throw new UnsupportedOperationException("create with data not yet supported");
        }
        long rowCount = 0;

        RaptorOutputTableHandle table = (RaptorOutputTableHandle) tableHandle;

        if (!table.getNewDistributionNodes().isEmpty()) {
            transaction.createDistribution(
                    table.getDistributionId(),
                    table.getBucketCount(),
                    table.getNewDistributionNodes());
        }

        transaction.createTable(
                table.getSchemaId(),
                table.getTableName(),
                table.getDistributionId(),
                System.currentTimeMillis(),
                rowCount);

        return Optional.empty();
    }

    private static Optional<RaptorColumnHandle> getTemporalColumnHandle(String temporalColumn, Map<String, RaptorColumnHandle> columnHandleMap)
    {
        if (temporalColumn == null) {
            return Optional.empty();
        }

        RaptorColumnHandle handle = columnHandleMap.get(temporalColumn);
        if (handle == null) {
            throw new PrestoException(NOT_FOUND, "Temporal column does not exist: " + temporalColumn);
        }
        return Optional.of(handle);
    }

    private static List<RaptorColumnHandle> getSortColumnHandles(List<String> sortColumns, Map<String, RaptorColumnHandle> columnHandleMap)
    {
        ImmutableList.Builder<RaptorColumnHandle> columnHandles = ImmutableList.builder();
        for (String column : sortColumns) {
            if (!columnHandleMap.containsKey(column)) {
                throw new PrestoException(NOT_FOUND, "Ordering column does not exist: " + column);
            }
            columnHandles.add(columnHandleMap.get(column));
        }
        return columnHandles.build();
    }

    private static <T> Iterator<T> cyclingShuffledIterator(Collection<T> collection)
    {
        List<T> list = new ArrayList<>(collection);
        Collections.shuffle(list);
        return Iterables.cycle(list).iterator();
    }

    private static final class NewTableInfo
    {
        private final long distributionId;
        private final int bucketCount;
        private final List<Long> newDistributionNodes;

        public NewTableInfo(long distributionId, int bucketCount, List<Long> newDistributionNodes)
        {
            this.distributionId = requireNonNull(distributionId, "distributionId is null");
            this.bucketCount = requireNonNull(bucketCount, "bucketCount is null");
            this.newDistributionNodes = ImmutableList.copyOf(requireNonNull(newDistributionNodes, "newDistributionNodes is null"));
        }

        public long getDistributionId()
        {
            return distributionId;
        }

        public int getBucketCount()
        {
            return bucketCount;
        }

        public List<Long> getNewDistributionNodes()
        {
            return newDistributionNodes;
        }
    }
}
