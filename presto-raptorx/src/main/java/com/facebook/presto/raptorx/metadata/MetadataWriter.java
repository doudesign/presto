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

import java.util.List;

public interface MetadataWriter
{
    void recover();

    long beginCommit();

    void finishCommit(long commitId);

    void createSchema(long commitId, long schemaId, String schemaName);

    void renameSchema(long commitId, long schemaId, String newSchemaName);

    void dropSchema(long commitId, long schemaId);

    void createDistribution(long commitId, long distributionId, int bucketCount, List<Long> nodes);

    void createTable(long commitId, TableInfo table);

    void renameTable(long commitId, long tableId, String tableName, long schemaId);

    void dropTable(long commitId, long tableId);
}
