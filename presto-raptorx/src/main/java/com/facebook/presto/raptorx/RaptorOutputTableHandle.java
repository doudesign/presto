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

import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class RaptorOutputTableHandle
        implements ConnectorOutputTableHandle
{
    private final long transactionId;
    private final long schemaId;
    private final long tableId;
    private final String tableName;
    private final List<RaptorColumnHandle> columnHandles;
    private final List<RaptorColumnHandle> sortColumnHandles;
    private final Optional<RaptorColumnHandle> temporalColumnHandle;
    private final List<Long> newDistributionNodes;
    private final long distributionId;
    private final int bucketCount;
    private final List<RaptorColumnHandle> bucketColumnHandles;

    @JsonCreator
    public RaptorOutputTableHandle(
            @JsonProperty("transactionId") long transactionId,
            @JsonProperty("schemaId") long schemaId,
            @JsonProperty("tableId") long tableId,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("columnHandles") List<RaptorColumnHandle> columnHandles,
            @JsonProperty("sortColumnHandles") List<RaptorColumnHandle> sortColumnHandles,
            @JsonProperty("temporalColumnHandle") Optional<RaptorColumnHandle> temporalColumnHandle,
            @JsonProperty("newDistributionNodes") List<Long> newDistributionNodes,
            @JsonProperty("distributionId") long distributionId,
            @JsonProperty("bucketCount") int bucketCount,
            @JsonProperty("bucketColumnHandles") List<RaptorColumnHandle> bucketColumnHandles)
    {
        this.transactionId = transactionId;
        this.schemaId = schemaId;
        this.tableId = tableId;
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.columnHandles = ImmutableList.copyOf(requireNonNull(columnHandles, "columnHandles is null"));
        this.sortColumnHandles = requireNonNull(sortColumnHandles, "sortColumnHandles is null");
        this.temporalColumnHandle = requireNonNull(temporalColumnHandle, "temporalColumnHandle is null");
        this.newDistributionNodes = ImmutableList.copyOf(requireNonNull(newDistributionNodes, "newDistributionNodes is null"));
        this.distributionId = distributionId;
        this.bucketCount = requireNonNull(bucketCount, "bucketCount is null");
        this.bucketColumnHandles = ImmutableList.copyOf(requireNonNull(bucketColumnHandles, "bucketColumnHandles is null"));
    }

    @JsonProperty
    public long getTransactionId()
    {
        return transactionId;
    }

    @JsonProperty
    public long getSchemaId()
    {
        return schemaId;
    }

    @JsonProperty
    public long getTableId()
    {
        return tableId;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public List<RaptorColumnHandle> getColumnHandles()
    {
        return columnHandles;
    }

    @JsonProperty
    public List<RaptorColumnHandle> getSortColumnHandles()
    {
        return sortColumnHandles;
    }

    @JsonProperty
    public Optional<RaptorColumnHandle> getTemporalColumnHandle()
    {
        return temporalColumnHandle;
    }

    @JsonProperty
    public List<Long> getNewDistributionNodes()
    {
        return newDistributionNodes;
    }

    @JsonProperty
    public long getDistributionId()
    {
        return distributionId;
    }

    @JsonProperty
    public int getBucketCount()
    {
        return bucketCount;
    }

    @JsonProperty
    public List<RaptorColumnHandle> getBucketColumnHandles()
    {
        return bucketColumnHandles;
    }

    @Override
    public String toString()
    {
        return String.valueOf(tableId);
    }
}
