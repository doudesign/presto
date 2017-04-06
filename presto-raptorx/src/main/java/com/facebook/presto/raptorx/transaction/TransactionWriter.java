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

import com.facebook.presto.raptorx.metadata.MetadataWriter;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.Objects.requireNonNull;

@ThreadSafe
public class TransactionWriter
{
    private final MetadataWriter writer;
    private final ReentrantLock lock = new ReentrantLock(true);

    @GuardedBy("lock")
    private boolean needRecovery = true;

    @Inject
    public TransactionWriter(MetadataWriter writer)
    {
        this.writer = requireNonNull(writer, "writer is null");
    }

    public void write(List<Action> actions)
    {
        if (actions.isEmpty()) {
            return;
        }

        lock.lock();
        try {
            if (needRecovery) {
                writer.recover();
                needRecovery = false;
            }
            doWrite(actions);
        }
        catch (Throwable t) {
            needRecovery = true;
            throw t;
        }
        finally {
            lock.unlock();
        }
    }

    @GuardedBy("lock")
    private long doWrite(List<Action> actions)
    {
        long commitId = writer.beginCommit();

        ActionWriter actionWriter = new ActionWriter(writer, commitId);
        for (Action action : actions) {
            action.accept(actionWriter);
        }

        writer.finishCommit(commitId);

        return commitId;
    }

    private static class ActionWriter
            implements ActionVisitor
    {
        private final MetadataWriter writer;
        private final long commitId;

        public ActionWriter(MetadataWriter writer, long commitId)
        {
            this.writer = requireNonNull(writer, "writer is null");
            this.commitId = commitId;
        }

        @Override
        public void visitCreateSchema(CreateSchemaAction action)
        {
            writer.createSchema(commitId, action.getSchemaId(), action.getSchemaName());
        }

        @Override
        public void visitRenameSchema(RenameSchemaAction action)
        {
            writer.renameSchema(commitId, action.getSchemaId(), action.getNewSchemaName());
        }

        @Override
        public void visitDropSchema(DropSchemaAction action)
        {
            writer.dropSchema(commitId, action.getSchemaId());
        }

        @Override
        public void visitCreateDistribution(CreateDistributionAction action)
        {
            writer.createDistribution(commitId, action.getDistributionId(), action.getBucketCount(), action.getNodes());
        }

        @Override
        public void visitCreateTable(CreateTableAction action)
        {
            writer.createTable(commitId, action.getTableInfo());
        }

        @Override
        public void visitRenameTable(RenameTableAction action)
        {
            writer.renameTable(commitId, action.getTableId(), action.getTableName(), action.getSchemaId());
        }

        @Override
        public void visitDropTable(DropTableAction action)
        {
            writer.dropTable(commitId, action.getTableId());
        }
    }
}
