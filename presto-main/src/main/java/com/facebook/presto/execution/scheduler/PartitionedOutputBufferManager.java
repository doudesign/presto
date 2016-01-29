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
package com.facebook.presto.execution.scheduler;

import com.facebook.presto.OutputBuffers;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.sql.planner.PartitionFunctionHandle;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

import static com.facebook.presto.OutputBuffers.BufferType.RANDOM;
import static com.facebook.presto.OutputBuffers.BufferType.SHARED;
import static com.facebook.presto.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.facebook.presto.sql.planner.PartitionFunctionHandle.ROUND_ROBIN;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class PartitionedOutputBufferManager
        implements OutputBufferManager
{
    private final PartitionFunctionHandle functionHandle;
    private final Consumer<OutputBuffers> outputBufferTarget;
    @GuardedBy("this")
    private final Map<TaskId, Integer> partitions = new LinkedHashMap<>();
    @GuardedBy("this")
    private boolean noMoreBufferIds;

    public PartitionedOutputBufferManager(PartitionFunctionHandle functionHandle, Consumer<OutputBuffers> outputBufferTarget)
    {
        this.functionHandle = requireNonNull(functionHandle, "functionHandle is null");
        this.outputBufferTarget = requireNonNull(outputBufferTarget, "outputBufferTarget is null");
    }

    @Override
    public synchronized void addOutputBuffer(TaskId bufferId, int partition)
    {
        if (noMoreBufferIds) {
            // a stage can move to a final state (e.g., failed) while scheduling, so ignore
            // the new buffers
            return;
        }

        checkArgument(partition >= 0, "partition is negative");
        partitions.put(bufferId, partition);
    }

    @Override
    public void noMoreOutputBuffers()
    {
        synchronized (this) {
            if (noMoreBufferIds) {
                // already created the buffers
                return;
            }
            noMoreBufferIds = true;
        }

        OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(functionHandle.equals(ROUND_ROBIN) ? RANDOM : SHARED)
                .withBuffers(partitions)
                .withNoMoreBufferIds();

        outputBufferTarget.accept(outputBuffers);
    }
}
