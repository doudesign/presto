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

import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.SqlBatch;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.customizers.Mapper;

import java.util.List;

public interface WriterDao
{
    // transaction

    @SqlQuery("SELECT commit_id FROM current_commit FOR UPDATE")
    Long getLockedCurrentCommitId();

    @SqlUpdate("INSERT INTO active_commit (singleton, commit_id, start_time, rolling_back)\n" +
            "VALUES ('X', :commitId, :startTime, FALSE)")
    void insertActiveCommit(
            @Bind("commitId") long commitId,
            @Bind("startTime") long startTime);

    @SqlQuery("SELECT * FROM active_commit")
    @Mapper(ActiveCommit.Mapper.class)
    ActiveCommit getActiveCommit();

    @SqlUpdate("DELETE FROM active_commit")
    int deleteActiveCommit();

    @SqlUpdate("UPDATE active_commit SET rolling_back = FALSE")
    void abortActiveCommit();

    @SqlUpdate("INSERT INTO commits (commit_id, commit_time)\n" +
            "VALUES (:commitId, :commitTime)")
    void insertCommit(
            @Bind("commitId") long commitId,
            @Bind("commitTime") long commitTime);

    @SqlUpdate("UPDATE current_commit SET commit_id = :commitId")
    int updateCurrentCommit(
            @Bind("commitId") long commitId);

    default void rollback(long commitId)
    {
        rollbackCreatedSchemas(commitId);
        rollbackDeletedSchemas(commitId);

        rollbackCreatedTables(commitId);
        rollbackDeletedTables(commitId);
    }

    // schema

    @SqlUpdate("INSERT INTO schemata (start_commit_id, schema_id, schema_name)\n" +
            "VALUES (:commitId, :schemaId, :schemaName)")
    void insertSchema(
            @Bind("commitId") long commitId,
            @Bind("schemaId") long schemaId,
            @Bind("schemaName") byte[] schemaName);

    @SqlUpdate("UPDATE schemata SET end_commit_id = :commitId\n" +
            "WHERE schema_id = :schemaId")
    void deleteSchema(
            @Bind("commitId") long commitId,
            @Bind("schemaId") long schemaId);

    @SqlUpdate("DELETE FROM schemata WHERE start_commit_id = :commitId")
    void rollbackCreatedSchemas(
            @Bind("commitId") long commitId);

    @SqlUpdate("UPDATE schemata SET end_commit_id = NULL\n" +
            "WHERE end_commit_id = :commitId")
    void rollbackDeletedSchemas(
            @Bind("commitId") long commitId);

    @SqlQuery("SELECT count(*)\n" +
            "FROM schemata\n" +
            "WHERE end_commit_id IS NULL\n" +
            "  AND schema_id = :schemaId")
    boolean schemaIdExists(
            @Bind("schemaId") long schemaId);

    @SqlQuery("SELECT count(*)\n" +
            "FROM schemata\n" +
            "WHERE end_commit_id IS NULL\n" +
            "  AND schema_name = :schemaName")
    boolean schemaNameExists(
            @Bind("schemaName") byte[] schemaName);

    // distribution

    @SqlQuery("SELECT count(*)\n" +
            "FROM distributions\n" +
            "WHERE distribution_id = :distributionId")
    boolean distributionIdExists(
            @Bind("distributionId") long distributionId);

    @SqlUpdate("INSERT INTO distributions (distribution_id, bucket_count)\n" +
            "VALUES (:distributionId, :bucketCount)")
    void createDistribution(
            @Bind("distributionId") long distributionId,
            @Bind("bucketCount") int bucketCount);

    @SqlBatch("INSERT INTO bucket_nodes (distribution_id, bucket_number, node_id)\n" +
            "VALUES (:distributionId, :bucketNumbers, :nodeIds)")
    void insertBucketNodes(
            @Bind("distributionId") long distributionId,
            @Bind("bucketNumbers") List<Integer> bucketNumbers,
            @Bind("nodeIds") List<Long> nodeIds);

    // table

    @SqlUpdate("INSERT INTO tables (\n" +
            "  start_commit_id,\n" +
            "  table_id,\n" +
            "  table_name,\n" +
            "  schema_id,\n" +
            "  distribution_id,\n" +
            "  temporal_column_id,\n" +
            "  create_time,\n" +
            "  update_time,\n" +
            "  row_count)\n" +
            "VALUES (\n" +
            "  :commitId,\n" +
            "  :tableId,\n" +
            "  :tableName,\n" +
            "  :schemaId,\n" +
            "  :distributionId,\n" +
            "  :temporalColumnId,\n" +
            "  :createTime,\n" +
            "  :updateTime,\n" +
            "  :rowCount)\n")
    void insertTable(
            @Bind("commitId") long commitId,
            @Bind("tableId") long tableId,
            @Bind("tableName") byte[] tableName,
            @Bind("schemaId") long schemaId,
            @Bind("distributionId") long distributionId,
            @Bind("temporalColumnId") Long temporalColumnId,
            @Bind("createTime") long createTime,
            @Bind("updateTime") long updateTime,
            @Bind("rowCount") long rowCount);

    @SqlUpdate("UPDATE tables SET end_commit_id = :commitId\n" +
            "WHERE table_id = :tableId")
    void deleteTable(
            @Bind("commitId") long commitId,
            @Bind("tableId") long tableId);

    @SqlUpdate("DELETE FROM tables WHERE start_commit_id = :commitId")
    void rollbackCreatedTables(
            @Bind("commitId") long commitId);

    @SqlUpdate("UPDATE tables SET end_commit_id = NULL\n" +
            "WHERE end_commit_id = :commitId")
    void rollbackDeletedTables(
            @Bind("commitId") long commitId);

    @SqlQuery("SELECT count(*)\n" +
            "FROM tables\n" +
            "WHERE end_commit_id IS NULL\n" +
            "  AND table_id = :tableId")
    boolean tableIdExists(
            @Bind("tableId") long tableId);

    @SqlQuery("SELECT count(*)\n" +
            "FROM tables\n" +
            "WHERE end_commit_id IS NULL\n" +
            "  AND schema_id = :schemaId\n" +
            "  AND table_name = :tableName")
    boolean tableNameExists(
            @Bind("schemaId") long schemaId,
            @Bind("tableName") byte[] name);

    @SqlQuery("SELECT *\n" +
            "FROM tables\n" +
            "WHERE end_commit_id IS NULL\n" +
            "  AND table_id = :tableId\n")
    @Mapper(TableInfo.Mapper.class)
    TableInfo getTableInfo(
            @Bind("tableId") long tableId);
}
