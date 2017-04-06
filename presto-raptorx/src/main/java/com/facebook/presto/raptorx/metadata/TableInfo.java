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

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

import static com.facebook.presto.raptorx.util.DatabaseUtil.utf8String;
import static java.util.Objects.requireNonNull;

public class TableInfo
{
    private final long tableId;
    private final String tableName;
    private final long schemaId;
    private final long distributionId;
    private final long createTime;
    private final long updateTime;
    private final long rowCount;

    public TableInfo(
            long tableId,
            String tableName,
            long schemaId,
            long distributionId,
            long createTime,
            long updateTime,
            long rowCount)
    {
        this.tableId = tableId;
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.schemaId = schemaId;
        this.distributionId = distributionId;
        this.createTime = createTime;
        this.updateTime = updateTime;
        this.rowCount = rowCount;
    }

    public long getTableId()
    {
        return tableId;
    }

    public String getTableName()
    {
        return tableName;
    }

    public long getSchemaId()
    {
        return schemaId;
    }

    public long getDistributionId()
    {
        return distributionId;
    }

    public long getCreateTime()
    {
        return createTime;
    }

    public long getUpdateTime()
    {
        return updateTime;
    }

    public long getRowCount()
    {
        return rowCount;
    }

    public static class Mapper
            implements ResultSetMapper<TableInfo>
    {
        @Override
        public TableInfo map(int index, ResultSet rs, StatementContext context)
                throws SQLException
        {
            return new Builder()
                    .setTableId(rs.getLong("table_id"))
                    .setTableName(utf8String(rs.getBytes("table_name")))
                    .setSchemaId(rs.getLong("schema_id"))
                    .setDistributionId(rs.getLong("distribution_id"))
                    .setCreateTime(rs.getLong("create_time"))
                    .setUpdateTime(rs.getLong("update_time"))
                    .setRowCount(rs.getLong("row_count"))
                    .build();
        }
    }

    public static class Builder
    {
        private long tableId;
        private String tableName;
        private long schemaId;
        private long distributionId;
        private long createTime;
        private long updateTime;
        private long rowCount;

        public Builder() {}

        public Builder(TableInfo source)
        {
            this.tableId = source.getTableId();
            this.tableName = source.getTableName();
            this.schemaId = source.getSchemaId();
            this.distributionId = source.getDistributionId();
            this.createTime = source.getCreateTime();
            this.updateTime = source.getUpdateTime();
            this.rowCount = source.getRowCount();
        }

        public Builder setTableId(long tableId)
        {
            this.tableId = tableId;
            return this;
        }

        public Builder setTableName(String tableName)
        {
            this.tableName = tableName;
            return this;
        }

        public Builder setSchemaId(long schemaId)
        {
            this.schemaId = schemaId;
            return this;
        }

        public Builder setDistributionId(long distributionId)
        {
            this.distributionId = distributionId;
            return this;
        }

        public Builder setCreateTime(long createTime)
        {
            this.createTime = createTime;
            return this;
        }

        public Builder setUpdateTime(long updateTime)
        {
            this.updateTime = updateTime;
            return this;
        }

        public Builder setRowCount(long rowCount)
        {
            this.rowCount = rowCount;
            return this;
        }

        public TableInfo build()
        {
            return new TableInfo(
                    tableId,
                    tableName,
                    schemaId,
                    distributionId,
                    createTime,
                    updateTime,
                    rowCount);
        }
    }
}
