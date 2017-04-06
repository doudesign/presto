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
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.customizers.Mapper;

import java.util.Collection;

public interface ReaderDao
{
    @SqlQuery("SELECT commit_id FROM current_commit")
    long getCurrentCommitId();

    @SqlQuery("SELECT schema_id\n" +
            "FROM schemata\n" +
            "WHERE start_commit_id <= :commitId\n" +
            "  AND (end_commit_id > :commitId OR end_commit_id IS NULL)\n" +
            "  AND schema_name = :schemaName\n")
    Long getSchemaId(
            @Bind("commitId") long commitId,
            @Bind("schemaName") byte[] schemaName);

    @SqlQuery("SELECT schema_name\n" +
            "FROM schemata\n" +
            "WHERE start_commit_id <= :commitId\n" +
            "  AND (end_commit_id > :commitId OR end_commit_id IS NULL)")
    Collection<byte[]> listSchemas(
            @Bind("commitId") long commitId);

    @SqlQuery("SELECT table_id\n" +
            "FROM tables\n" +
            "WHERE start_commit_id <= :commitId\n" +
            "  AND (end_commit_id > :commitId OR end_commit_id IS NULL)\n" +
            "  AND schema_id = :schemaId\n" +
            "  AND table_name = :tableName\n")
    Long getTableId(
            @Bind("commitId") long commitId,
            @Bind("schemaId") long schemaId,
            @Bind("tableName") byte[] tableName);

    @SqlQuery("SELECT *\n" +
            "FROM tables\n" +
            "WHERE start_commit_id <= :commitId\n" +
            "  AND (end_commit_id > :commitId OR end_commit_id IS NULL)\n" +
            "  AND table_id = :tableId\n")
    @Mapper(TableInfo.Mapper.class)
    TableInfo getTableInfo(
            @Bind("commitId") long commitId,
            @Bind("tableId") long tableId);

    @SqlQuery("SELECT table_name\n" +
            "FROM tables\n" +
            "WHERE start_commit_id <= :commitId\n" +
            "  AND (end_commit_id > :commitId OR end_commit_id IS NULL)")
    Collection<byte[]> listTables(
            @Bind("commitId") long commitId);
}
