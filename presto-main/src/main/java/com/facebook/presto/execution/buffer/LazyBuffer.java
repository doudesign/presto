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
package com.facebook.presto.execution.buffer;

import com.facebook.presto.OutputBuffers;
import com.facebook.presto.execution.StateMachine;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.execution.SystemMemoryUsageListener;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.spi.Page;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.DataSize;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static com.facebook.presto.execution.buffer.BufferState.FAILED;
import static com.facebook.presto.execution.buffer.BufferState.FINISHED;
import static com.facebook.presto.execution.buffer.BufferState.FLUSHING;
import static com.facebook.presto.execution.buffer.BufferState.NO_MORE_BUFFERS;
import static com.facebook.presto.execution.buffer.BufferState.NO_MORE_PAGES;
import static com.facebook.presto.execution.buffer.BufferState.OPEN;
import static com.facebook.presto.execution.buffer.BufferState.TERMINAL_BUFFER_STATES;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class LazyBuffer
        implements OutputBuffer
{
    private final StateMachine<BufferState> state;
    private final String taskInstanceId;
    private final SharedBufferMemoryManager memoryManager;

    @GuardedBy("this")
    private OutputBuffer delegate;

    @GuardedBy("this")
    private final Set<TaskId> abortedBuffers = new HashSet<>();

    @GuardedBy("this")
    private final List<PendingRead> pendingReads = new ArrayList<>();

    @GuardedBy("this")
    private final List<PendingWrite> pendingWrites = new ArrayList<>();

    public LazyBuffer(TaskId taskId, String taskInstanceId, Executor executor, DataSize maxBufferSize, SystemMemoryUsageListener systemMemoryUsageListener)
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(executor, "executor is null");
        this.taskInstanceId = requireNonNull(taskInstanceId, "taskInstanceId is null");
        state = new StateMachine<>(taskId + "-buffer", executor, OPEN, TERMINAL_BUFFER_STATES);
        requireNonNull(maxBufferSize, "maxBufferSize is null");
        checkArgument(maxBufferSize.toBytes() > 0, "maxBufferSize must be at least 1");
        requireNonNull(systemMemoryUsageListener, "systemMemoryUsageListener is null");
        this.memoryManager = new SharedBufferMemoryManager(maxBufferSize.toBytes(), systemMemoryUsageListener);
    }

    @Override
    public void addStateChangeListener(StateChangeListener<BufferState> stateChangeListener)
    {
        state.addStateChangeListener(stateChangeListener);
    }

    @Override
    public boolean isFinished()
    {
        return state.get() == FINISHED;
    }

    @Override
    public SharedBufferInfo getInfo()
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            if (delegate == null) {
                //
                // NOTE: this code must be lock free to we are not hanging state machine updates
                //
                BufferState state = this.state.get();

                return new SharedBufferInfo(
                        state,
                        state.canAddBuffers(),
                        state.canAddPages(),
                        0,
                        0,
                        0,
                        0,
                        ImmutableList.of());
            }
            outputBuffer = delegate;
        }
        return outputBuffer.getInfo();
    }

    @Override
    public void setOutputBuffers(OutputBuffers newOutputBuffers)
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            if (delegate == null) {
                delegate = new SharedBuffer(taskInstanceId, state, memoryManager);

                delegate.setOutputBuffers(newOutputBuffers);

                for (TaskId abortedBuffer : abortedBuffers) {
                    delegate.abort(abortedBuffer);
                }
                abortedBuffers.clear();

                for (PendingWrite pendingWrite : pendingWrites) {
                    pendingWrite.process(delegate);
                }
                pendingWrites.clear();

                for (PendingRead pendingRead : pendingReads) {
                    pendingRead.process(delegate);
                }
                pendingReads.clear();

                return;
            }
            outputBuffer = delegate;
        }
        outputBuffer.setOutputBuffers(newOutputBuffers);
    }

    @Override
    public ListenableFuture<?> enqueue(Page page)
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            if (delegate == null) {
                PendingWrite pendingWrite = new PendingWrite(page);
                pendingWrites.add(pendingWrite);
                return pendingWrite.getFutureResult();
            }
            outputBuffer = delegate;
        }
        return outputBuffer.enqueue(page);
    }

    @Override
    public ListenableFuture<?> enqueue(int partition, Page page)
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            if (delegate == null) {
                PendingWrite pendingWrite = new PendingWrite(partition, page);
                pendingWrites.add(pendingWrite);
                return pendingWrite.getFutureResult();
            }
            outputBuffer = delegate;
        }
        return outputBuffer.enqueue(partition, page);
    }

    @Override
    public CompletableFuture<BufferResult> get(TaskId outputId, long startingSequenceId, DataSize maxSize)
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            if (delegate == null) {
                PendingRead pendingRead = new PendingRead(outputId, startingSequenceId, maxSize);
                pendingReads.add(pendingRead);
                return pendingRead.getFutureResult();
            }
            outputBuffer = delegate;
        }
        return outputBuffer.get(outputId, startingSequenceId, maxSize);
    }

    @Override
    public void abort(TaskId outputId)
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            if (delegate == null) {
                abortedBuffers.add(outputId);
                return;
            }
            outputBuffer = delegate;
        }
        outputBuffer.abort(outputId);
    }

    @Override
    public void setNoMorePages()
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            if (delegate == null) {
                state.compareAndSet(OPEN, NO_MORE_PAGES);
                state.compareAndSet(NO_MORE_BUFFERS, FLUSHING);
                return;
            }
            outputBuffer = delegate;
        }
        outputBuffer.setNoMorePages();
    }

    @Override
    public void destroy()
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            if (delegate == null) {
                state.set(FINISHED);
                // todo free writers and readers;
                return;
            }
            outputBuffer = delegate;
        }
        outputBuffer.destroy();
    }

    @Override
    public void fail()
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            if (delegate == null) {
                // ignore fail if the buffer already in a terminal state.
                if (state.get().isTerminal()) {
                    return;
                }
                state.set(FAILED);
                // todo free writers;
                return;
            }
            outputBuffer = delegate;
        }
        outputBuffer.destroy();
    }

    private static class PendingRead
    {
        private final TaskId outputId;
        private final long startingSequenceId;
        private final DataSize maxSize;

        private final CompletableFuture<BufferResult> futureResult = new CompletableFuture<>();

        public PendingRead(TaskId outputId, long startingSequenceId, DataSize maxSize)
        {
            this.outputId = requireNonNull(outputId, "outputId is null");
            this.startingSequenceId = startingSequenceId;
            this.maxSize = requireNonNull(maxSize, "maxSize is null");
        }

        public CompletableFuture<BufferResult> getFutureResult()
        {
            return futureResult;
        }

        public void process(OutputBuffer delegate)
        {
            if (futureResult.isDone()) {
                return;
            }

            try {
                CompletableFuture<BufferResult> result = delegate.get(outputId, startingSequenceId, maxSize);
                result.whenComplete((value, exception) -> {
                    if (exception != null) {
                        futureResult.completeExceptionally(exception);
                    }
                    else {
                        futureResult.complete(value);
                    }
                });
            }
            catch (Exception e) {
                futureResult.completeExceptionally(e);
            }
        }
    }

    private static class PendingWrite
    {
        private final OptionalInt partition;
        private final Page page;
        private final SettableFuture<?> futureResult = SettableFuture.create();

        public PendingWrite(Page page)
        {
            this.partition = OptionalInt.empty();
            this.page = requireNonNull(page, "page is null");
        }

        public PendingWrite(int partition, Page page)
        {
            this.partition = OptionalInt.of(partition);
            this.page = requireNonNull(page, "page is null");
        }

        public ListenableFuture<?> getFutureResult()
        {
            return futureResult;
        }

        public void process(OutputBuffer delegate)
        {
            if (futureResult.isDone()) {
                return;
            }

            try {
                ListenableFuture<?> result;
                if (partition.isPresent()) {
                    result = delegate.enqueue(partition.getAsInt(), page);
                }
                else {
                    result = delegate.enqueue(page);
                }

                Futures.addCallback(result, new FutureCallback<Object>()
                {
                    @Override
                    public void onSuccess(Object result)
                    {
                        futureResult.set(null);
                    }

                    @Override
                    public void onFailure(Throwable t)
                    {
                        futureResult.setException(t);
                    }
                });
            }
            catch (Exception e) {
                futureResult.setException(e);
            }
        }
    }
}
