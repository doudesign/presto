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

import org.skife.jdbi.v2.sqlobject.SqlUpdate;

public interface MasterSchemaDao
{
    @SqlUpdate("CREATE TABLE IF NOT EXISTS sequences (\n" +
            "  sequence_name VARCHAR(50) PRIMARY KEY,\n" +
            "  next_value BIGINT NOT NULL\n" +
            ")")
    void createSequences();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS current_commit (\n" +
            "  singleton CHAR(1) PRIMARY KEY,\n" +
            "  commit_id BIGINT NOT NULL\n" +
            ")")
    void createCurrentCommit();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS active_commit (\n" +
            "  singleton CHAR(1) PRIMARY KEY,\n" +
            "  commit_id BIGINT NOT NULL,\n" +
            "  start_time BIGINT NOT NULL,\n" +
            "  rolling_back BOOLEAN NOT NULL,\n" +
            "  rollback_info LONGBLOB\n" +
            ")")
    void createActiveCommit();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS commits (\n" +
            "  commit_id BIGINT PRIMARY KEY,\n" +
            "  commit_time BIGINT NOT NULL\n" +
            ")")
    void createCommits();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS schemata (\n" +
            "  start_commit_id BIGINT NOT NULL,\n" +
            "  end_commit_id BIGINT,\n" +
            "  schema_id BIGINT,\n" +
            "  schema_name VARBINARY(512)\n" +
            ")")
    void createSchemata();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS tables (\n" +
            "  start_commit_id BIGINT NOT NULL,\n" +
            "  end_commit_id BIGINT,\n" +
            "  table_id BIGINT NOT NULL,\n" +
            "  table_name VARBINARY(512) NOT NULL,\n" +
            "  schema_id BIGINT NOT NULL,\n" +
            "  distribution_id BIGINT NOT NULL,\n" +
            "  temporal_column_id BIGINT,\n" +
            "  create_time BIGINT NOT NULL,\n" +
            "  update_time BIGINT NOT NULL,\n" +
            "  row_count BIGINT NOT NULL\n" +
            ")")
    void createTables();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS nodes (\n" +
            "  node_id BIGINT PRIMARY KEY AUTO_INCREMENT,\n" +
            "  identifier VARBINARY(100) NOT NULL,\n" +
            "  UNIQUE (identifier)\n" +
            ")")
    void createNodes();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS distributions (\n" +
            "  distribution_id BIGINT PRIMARY KEY,\n" +
            "  bucket_count INT NOT NULL\n" +
            ")")
    void createDistributions();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS bucket_nodes (\n" +
            "  distribution_id BIGINT NOT NULL,\n" +
            "  bucket_number INT NOT NULL,\n" +
            "  node_id BIGINT NOT NULL,\n" +
            "  UNIQUE (distribution_id, bucket_number, node_id)\n" +
            ")")
    void createBucketNodes();
}
