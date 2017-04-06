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
import com.facebook.presto.raptorx.util.Database.Type;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

import javax.inject.Inject;

import static com.facebook.presto.raptorx.util.DatabaseUtil.runIgnoringConstraintViolation;

public class SchemaCreator
{
    @Inject
    public SchemaCreator(Database database)
    {
        new DBI(database.getMasterConnection()).useHandle(handle -> {
            MasterSchemaDao dao = handle.attach(MasterSchemaDao.class);

            dao.createSequences();
            dao.createCurrentCommit();
            dao.createActiveCommit();
            dao.createCommits();
            dao.createSchemata();
            dao.createTables();
            dao.createNodes();
            dao.createDistributions();
            dao.createBucketNodes();

            insertIgnore(database.getType(), handle, "current_commit (singleton, commit_id) VALUES ('X', 0)");
        });
    }

    @SuppressWarnings("SameParameterValue")
    private static void insertIgnore(Database.Type type, Handle handle, String insert)
    {
        if (type == Type.MYSQL) {
            handle.execute("INSERT IGNORE INTO " + insert);
        }
        else {
            runIgnoringConstraintViolation(() -> handle.execute("INSERT INTO " + insert));
        }
    }
}
