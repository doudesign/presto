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

import io.airlift.units.DataSize;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.OptionalLong;

import static com.google.common.base.MoreObjects.ToStringHelper;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class ChunkMetadata
{
    private final long tableId;
    private final long chunkId;
    private final int bucketNumber;
    private final long rowCount;
    private final long compressedSize;
    private final long uncompressedSize;
    private final OptionalLong rangeStart;
    private final OptionalLong rangeEnd;

    public ChunkMetadata(
            long tableId,
            long chunkId,
            int bucketNumber,
            long rowCount,
            long compressedSize,
            long uncompressedSize,
            OptionalLong rangeStart,
            OptionalLong rangeEnd)
    {
        checkArgument(tableId > 0, "tableId must be > 0");
        checkArgument(chunkId > 0, "chunkId must be > 0");
        checkArgument(rowCount >= 0, "rowCount must be >= 0");
        checkArgument(compressedSize >= 0, "compressedSize must be >= 0");
        checkArgument(uncompressedSize >= 0, "uncompressedSize must be >= 0");

        this.tableId = tableId;
        this.chunkId = chunkId;
        this.bucketNumber = bucketNumber;
        this.rowCount = rowCount;
        this.compressedSize = compressedSize;
        this.uncompressedSize = uncompressedSize;
        this.rangeStart = requireNonNull(rangeStart, "rangeStart is null");
        this.rangeEnd = requireNonNull(rangeEnd, "rangeEnd is null");
    }

    public long getTableId()
    {
        return tableId;
    }

    public long getChunkId()
    {
        return chunkId;
    }

    public int getBucketNumber()
    {
        return bucketNumber;
    }

    public long getRowCount()
    {
        return rowCount;
    }

    public long getCompressedSize()
    {
        return compressedSize;
    }

    public long getUncompressedSize()
    {
        return uncompressedSize;
    }

    public OptionalLong getRangeStart()
    {
        return rangeStart;
    }

    public OptionalLong getRangeEnd()
    {
        return rangeEnd;
    }

    public ChunkMetadata withTimeRange(long rangeStart, long rangeEnd)
    {
        return new ChunkMetadata(
                tableId,
                chunkId,
                bucketNumber,
                rowCount,
                compressedSize,
                uncompressedSize,
                OptionalLong.of(rangeStart),
                OptionalLong.of(rangeEnd));
    }

    @Override
    public String toString()
    {
        ToStringHelper builder = toStringHelper(this)
                .add("tableId", tableId)
                .add("chunkId", chunkId)
                .add("bucketNumber", bucketNumber)
                .add("rowCount", rowCount)
                .add("compressedSize", DataSize.succinctBytes(compressedSize))
                .add("uncompressedSize", DataSize.succinctBytes(uncompressedSize));
        if (rangeStart.isPresent()) {
            builder.add("rangeStart", rangeStart.getAsLong());
        }
        if (rangeEnd.isPresent()) {
            builder.add("rangeEnd", rangeEnd.getAsLong());
        }
        return builder.toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ChunkMetadata that = (ChunkMetadata) o;
        return Objects.equals(tableId, that.tableId) &&
                Objects.equals(chunkId, that.chunkId) &&
                Objects.equals(bucketNumber, that.bucketNumber) &&
                Objects.equals(rowCount, that.rowCount) &&
                Objects.equals(compressedSize, that.compressedSize) &&
                Objects.equals(uncompressedSize, that.uncompressedSize) &&
                Objects.equals(rangeStart, that.rangeStart) &&
                Objects.equals(rangeEnd, that.rangeEnd);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                tableId,
                chunkId,
                bucketNumber,
                rowCount,
                compressedSize,
                uncompressedSize,
                rangeStart,
                rangeEnd);
    }

    public static class Mapper
            implements ResultSetMapper<ChunkMetadata>
    {
        @Override
        public ChunkMetadata map(int index, ResultSet rs, StatementContext context)
                throws SQLException
        {
            return new ChunkMetadata(
                    rs.getLong("table_id"),
                    rs.getLong("chunkId"),
                    rs.getInt("bucket_number"),
                    rs.getLong("row_count"),
                    rs.getLong("compressed_size"),
                    rs.getLong("uncompressed_size"),
                    OptionalLong.empty(),
                    OptionalLong.empty());
        }
    }
}
