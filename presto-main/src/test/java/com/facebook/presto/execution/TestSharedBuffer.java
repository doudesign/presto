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
package com.facebook.presto.execution;

import com.facebook.presto.OutputBuffers;
import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.execution.buffer.BufferInfo;
import com.facebook.presto.execution.buffer.BufferResult;
import com.facebook.presto.execution.buffer.OutputBuffer;
import com.facebook.presto.execution.buffer.PageBufferInfo;
import com.facebook.presto.execution.buffer.SharedBuffer;
import com.facebook.presto.operator.PageAssertions;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.OutputBuffers.BufferType.SHARED;
import static com.facebook.presto.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.facebook.presto.execution.buffer.BufferResult.emptyResults;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestSharedBuffer
{
    private static final Duration NO_WAIT = new Duration(0, TimeUnit.MILLISECONDS);
    private static final Duration MAX_WAIT = new Duration(1, TimeUnit.SECONDS);
    private static final DataSize PAGE_SIZE = new DataSize(createPage(42).getSizeInBytes(), BYTE);
    private static final TaskId TASK_ID = new TaskId("query", "stage", "task");
    private static final int DEFAULT_PARTITION = 0;
    private static final String TASK_INSTANCE_ID = "task-instance-id";

    private static final ImmutableList<BigintType> TYPES = ImmutableList.of(BIGINT);
    public static final TaskId FIRST = new TaskId("query", "stage", "first_task");
    public static final TaskId SECOND = new TaskId("query", "stage", "second_task");
    public static final TaskId QUEUE = new TaskId("query", "stage", "queue");
    public static final TaskId FOO = new TaskId("foo", "bar", "baz");

    private static Page createPage(int i)
    {
        return new Page(BlockAssertions.createLongsBlock(i));
    }

    public static DataSize sizeOfPages(int count)
    {
        return new DataSize(PAGE_SIZE.toBytes() * count, BYTE);
    }

    private ScheduledExecutorService stateNotificationExecutor;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        stateNotificationExecutor = newScheduledThreadPool(5, daemonThreadsNamed("test-%s"));
    }

    @AfterClass
    public void tearDown()
            throws Exception
    {
        if (stateNotificationExecutor != null) {
            stateNotificationExecutor.shutdownNow();
            stateNotificationExecutor = null;
        }
    }

    @Test
    public void testInvalidConstructorArg()
            throws Exception
    {
        try {
            new SharedBuffer(TASK_ID, TASK_INSTANCE_ID, stateNotificationExecutor, new DataSize(0, BYTE));
            fail("Expected IllegalStateException");
        }
        catch (IllegalArgumentException ignored) {
        }
    }

    @Test
    public void testSimple()
            throws Exception
    {
        OutputBuffer outputBuffer = new SharedBuffer(TASK_ID, TASK_INSTANCE_ID, stateNotificationExecutor, sizeOfPages(10));

        // add three items
        for (int i = 0; i < 3; i++) {
            addPage(outputBuffer, createPage(i));
        }

        OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(SHARED).withBuffer(FIRST, 0);

        // add a queue
        outputBuffer.setOutputBuffers(outputBuffers);
        assertQueueState(outputBuffer, FIRST, DEFAULT_PARTITION, 3, 0, 3, 3, 0);

        // get the three elements
        assertBufferResultEquals(TYPES, getBufferResult(outputBuffer, FIRST, 0, sizeOfPages(10), NO_WAIT), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        // pages not acknowledged yet so state is the same
        assertQueueState(outputBuffer, FIRST, DEFAULT_PARTITION, 3, 0, 3, 3, 0);

        // acknowledge first three pages
        outputBuffer.get(FIRST, 3, sizeOfPages(10)).cancel(true);
        // pages now acknowledged
        assertQueueState(outputBuffer, FIRST, DEFAULT_PARTITION, 0, 3, 3, 3, 0);

        // fill the buffer (we already added 3 pages)
        for (int i = 3; i < 10; i++) {
            addPage(outputBuffer, createPage(i));
        }
        assertQueueState(outputBuffer, FIRST, DEFAULT_PARTITION, 7, 3, 10, 10, 0);

        // try to add one more page, which should block
        ListenableFuture<?> future = enqueuePage(outputBuffer, createPage(10));
        assertQueueState(outputBuffer, FIRST, DEFAULT_PARTITION, 7, 3, 10, 10, 1);

        // remove a page
        assertBufferResultEquals(TYPES, getBufferResult(outputBuffer, FIRST, 3, sizeOfPages(1), NO_WAIT), bufferResult(3, createPage(3)));
        // page not acknowledged yet so sent count is the same
        assertQueueState(outputBuffer, FIRST, DEFAULT_PARTITION, 7, 3, 10, 10, 1);

        // we should still be blocked
        assertFalse(future.isDone());

        //
        // add another buffer and verify it sees all pages
        outputBuffers = outputBuffers.withBuffer(SECOND, 0);
        outputBuffer.setOutputBuffers(outputBuffers);
        assertQueueState(outputBuffer, SECOND, DEFAULT_PARTITION, 10, 0, 10, 10, 1);
        assertBufferResultEquals(TYPES, getBufferResult(outputBuffer, SECOND, 0, sizeOfPages(10), NO_WAIT), bufferResult(0, createPage(0),
                createPage(1),
                createPage(2),
                createPage(3),
                createPage(4),
                createPage(5),
                createPage(6),
                createPage(7),
                createPage(8),
                createPage(9)));
        // page not acknowledged yet so sent count is still zero
        assertQueueState(outputBuffer, SECOND, DEFAULT_PARTITION, 10, 0, 10, 10, 1);
        // acknowledge the 10 pages
        outputBuffer.get(SECOND, 10, sizeOfPages(10)).cancel(true);
        assertQueueState(outputBuffer, SECOND, DEFAULT_PARTITION, 0, 10, 10, 10, 1);

        //
        // tell shared buffer there will be no more queues
        outputBuffers = outputBuffers.withNoMoreBufferIds();
        outputBuffer.setOutputBuffers(outputBuffers);

        // since both queues consumed the first three pages, the blocked page future from above should be done
        future.get(1, TimeUnit.SECONDS);

        // we should be able to add 3 more pages (the third will be queued)
        // although the first queue fetched the 4th page, the page has not been acknowledged yet
        addPage(outputBuffer, createPage(11));
        addPage(outputBuffer, createPage(12));
        future = enqueuePage(outputBuffer, createPage(13));
        assertQueueState(outputBuffer, FIRST, DEFAULT_PARTITION, 10, 3, 10, 13, 1);
        assertQueueState(outputBuffer, SECOND, DEFAULT_PARTITION, 3, 10, 10, 13, 1);

        // remove a page from the first queue
        assertBufferResultEquals(TYPES, getBufferResult(outputBuffer, FIRST, 4, sizeOfPages(1), NO_WAIT), bufferResult(4, createPage(4)));

        // the blocked page future above should be done
        future.get(1, TimeUnit.SECONDS);
        assertQueueState(outputBuffer, FIRST, DEFAULT_PARTITION, 10, 4, 10, 14, 0);
        assertQueueState(outputBuffer, SECOND, DEFAULT_PARTITION, 4, 10, 10, 14, 0);

        //
        // finish the buffer
        assertFalse(outputBuffer.isFinished());
        outputBuffer.setNoMorePages();
        assertQueueState(outputBuffer, FIRST, DEFAULT_PARTITION, 10, 4, 10, 14, 0);
        assertQueueState(outputBuffer, SECOND, DEFAULT_PARTITION, 4, 10, 10, 14, 0);

        // not fully finished until all pages are consumed
        assertFalse(outputBuffer.isFinished());

        // remove a page, not finished
        assertBufferResultEquals(TYPES, getBufferResult(outputBuffer, FIRST, 5, sizeOfPages(1), NO_WAIT), bufferResult(5, createPage(5)));
        assertQueueState(outputBuffer, FIRST, DEFAULT_PARTITION, 9, 5, 9, 14, 0);
        assertQueueState(outputBuffer, SECOND, DEFAULT_PARTITION, 4, 10, 9, 14, 0);
        assertFalse(outputBuffer.isFinished());

        // remove all remaining pages from first queue, should not be finished
        BufferResult x = getBufferResult(outputBuffer, FIRST, 6, sizeOfPages(10), NO_WAIT);
        assertBufferResultEquals(TYPES, x, bufferResult(6, createPage(6),
                createPage(7),
                createPage(8),
                createPage(9),
                createPage(10),
                createPage(11),
                createPage(12),
                createPage(13)));
        assertQueueState(outputBuffer, FIRST, DEFAULT_PARTITION, 8, 6, 8, 14, 0);
        assertBufferResultEquals(TYPES, getBufferResult(outputBuffer, FIRST, 14, sizeOfPages(10), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 14, true));

        // finish first queue
        outputBuffer.abort(FIRST);
        assertQueueClosed(outputBuffer, FIRST, 14);
        assertQueueState(outputBuffer, SECOND, DEFAULT_PARTITION, 4, 10, 4, 14, 0);
        assertFalse(outputBuffer.isFinished());

        // remove all remaining pages from second queue, should be finished
        assertBufferResultEquals(TYPES, getBufferResult(outputBuffer, SECOND, 10, sizeOfPages(10), NO_WAIT), bufferResult(10, createPage(10),
                createPage(11),
                createPage(12),
                createPage(13)));
        assertQueueState(outputBuffer, SECOND, DEFAULT_PARTITION, 4, 10, 4, 14, 0);
        assertBufferResultEquals(TYPES, getBufferResult(outputBuffer, SECOND, 14, sizeOfPages(10), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 14, true));
        outputBuffer.abort(SECOND);
        assertQueueClosed(outputBuffer, FIRST, 14);
        assertQueueClosed(outputBuffer, SECOND, 14);
        assertFinished(outputBuffer);

        assertBufferResultEquals(TYPES, getBufferResult(outputBuffer, FIRST, 14, sizeOfPages(10), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 14, true));
        assertBufferResultEquals(TYPES, getBufferResult(outputBuffer, SECOND, 14, sizeOfPages(10), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 14, true));
    }

    @Test
    public void testSharedBufferFull()
            throws Exception
    {
        int firstPartition = 0;
        int secondPartition = 1;
        OutputBuffer outputBuffer = new SharedBuffer(TASK_ID, TASK_INSTANCE_ID, stateNotificationExecutor, sizeOfPages(2));

        // Add two pages, buffer is full
        addPage(outputBuffer, createPage(1), firstPartition);
        addPage(outputBuffer, createPage(2), secondPartition);

        // third page is blocked
        enqueuePage(outputBuffer, createPage(3), secondPartition);
    }

    @Test
    public void testDeqeueueOnAcknowledgement()
            throws Exception
    {
        int firstPartition = 0;
        int secondPartition = 1;
        OutputBuffer outputBuffer = new SharedBuffer(TASK_ID, TASK_INSTANCE_ID, stateNotificationExecutor, sizeOfPages(2));
        OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(SHARED)
                .withBuffer(FIRST, firstPartition)
                .withBuffer(SECOND, secondPartition)
                .withNoMoreBufferIds();
        outputBuffer.setOutputBuffers(outputBuffers);

        // Add two pages, buffer is full
        addPage(outputBuffer, createPage(1), firstPartition);
        addPage(outputBuffer, createPage(2), firstPartition);
        assertQueueState(outputBuffer, FIRST, firstPartition, 2, 0, 2, 2, 0);

        // third page is blocked
        ListenableFuture<?> future = enqueuePage(outputBuffer, createPage(3), secondPartition);

        // we should be blocked
        assertFalse(future.isDone());
        assertQueueState(outputBuffer, FIRST, firstPartition, 2, 0, 2, 2, 0);   // 2 buffered pages
        assertQueueState(outputBuffer, SECOND, secondPartition, 0, 0, 0, 0, 1); // 1 queued page

        // acknowledge pages for first partition, make space in the shared buffer
        outputBuffer.get(FIRST, 2, sizeOfPages(10)).cancel(true);

        // page should be dequeued, we should not be blocked
        assertTrue(future.isDone());
        assertQueueState(outputBuffer, SECOND, secondPartition, 1, 0, 1, 1, 0); // no more queued pages
    }

    @Test
    public void testSimplePartitioned()
            throws Exception
    {
        int firstPartition = 0;
        int secondPartition = 1;
        OutputBuffer outputBuffer = new SharedBuffer(TASK_ID, TASK_INSTANCE_ID, stateNotificationExecutor, sizeOfPages(20));

        // add three items to each buffer
        for (int i = 0; i < 3; i++) {
            addPage(outputBuffer, createPage(i), firstPartition);
            addPage(outputBuffer, createPage(i), secondPartition);
        }

        OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(SHARED).withBuffer(FIRST, firstPartition);

        // add first partition
        outputBuffer.setOutputBuffers(outputBuffers);
        assertQueueState(outputBuffer, FIRST, firstPartition, 3, 0, 3, 3, 0);

        // get the three elements
        assertBufferResultEquals(TYPES, getBufferResult(outputBuffer, FIRST, 0, sizeOfPages(10), NO_WAIT), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        // pages not acknowledged yet so state is the same
        assertQueueState(outputBuffer, FIRST, firstPartition, 3, 0, 3, 3, 0);

        // acknowledge first three pages
        outputBuffer.get(FIRST, 3, sizeOfPages(10)).cancel(true);
        // pages now acknowledged
        assertQueueState(outputBuffer, FIRST, firstPartition, 0, 3, 3, 3, 0);

        // fill the buffer (we already added 3 pages)
        for (int i = 3; i < 10; i++) {
            addPage(outputBuffer, createPage(i), firstPartition);
            addPage(outputBuffer, createPage(i), secondPartition);
        }
        assertQueueState(outputBuffer, FIRST, firstPartition, 7, 3, 10, 10, 0);

        // try to add one more page, which should block
        ListenableFuture<?> future = enqueuePage(outputBuffer, createPage(10), firstPartition);
        assertQueueState(outputBuffer, FIRST, firstPartition, 7, 3, 10, 10, 1);

        // remove a page
        assertBufferResultEquals(TYPES, getBufferResult(outputBuffer, FIRST, 3, sizeOfPages(1), NO_WAIT), bufferResult(3, createPage(3)));
        // page not acknowledged yet so sent count is the same
        assertQueueState(outputBuffer, FIRST, firstPartition, 7, 3, 10, 10, 1);

        // we should still be blocked
        assertFalse(future.isDone());

        //
        // add second partition and verify it sees all pages
        outputBuffers = outputBuffers.withBuffer(SECOND, secondPartition);
        outputBuffer.setOutputBuffers(outputBuffers);
        assertQueueState(outputBuffer, SECOND, secondPartition, 10, 0, 10, 10, 0);
        assertBufferResultEquals(TYPES, getBufferResult(outputBuffer, SECOND, 0, sizeOfPages(10), NO_WAIT), bufferResult(0, createPage(0),
                createPage(1),
                createPage(2),
                createPage(3),
                createPage(4),
                createPage(5),
                createPage(6),
                createPage(7),
                createPage(8),
                createPage(9)));
        // page not acknowledged yet so sent count is still zero
        assertQueueState(outputBuffer, SECOND, secondPartition, 10, 0, 10, 10, 0);
        // acknowledge the 10 pages
        outputBuffer.get(SECOND, 10, sizeOfPages(10)).cancel(true);
        assertQueueState(outputBuffer, SECOND, secondPartition, 0, 10, 10, 10, 0);

        //
        // tell shared buffer there will be no more queues
        outputBuffers = outputBuffers.withNoMoreBufferIds();
        outputBuffer.setOutputBuffers(outputBuffers);

        // since both queues consumed some pages, the blocked page future from above should be done
        future.get(1, TimeUnit.SECONDS);
        assertQueueState(outputBuffer, FIRST, firstPartition, 8, 3, 8, 11, 0);
        assertQueueState(outputBuffer, SECOND, secondPartition, 0, 10, 0, 10, 0);

        // we should be able to add 3 more pages
        // although the first queue fetched the 4th page, the page has not been acknowledged yet
        addPage(outputBuffer, createPage(11), firstPartition);
        addPage(outputBuffer, createPage(12), firstPartition);
        addPage(outputBuffer, createPage(13), firstPartition);
        assertQueueState(outputBuffer, FIRST, firstPartition, 11, 3, 11, 14, 0);
        assertQueueState(outputBuffer, SECOND, secondPartition, 0, 10, 0, 10, 0);

        // remove a page from the first queue
        assertBufferResultEquals(TYPES, getBufferResult(outputBuffer, FIRST, 4, sizeOfPages(1), NO_WAIT), bufferResult(4, createPage(4)));

        // the blocked page future above should be done
        future.get(1, TimeUnit.SECONDS);
        assertQueueState(outputBuffer, FIRST, firstPartition, 10, 4, 10, 14, 0);
        assertQueueState(outputBuffer, SECOND, secondPartition, 0, 10, 0, 10, 0);

        //
        // finish the buffer
        assertFalse(outputBuffer.isFinished());
        outputBuffer.setNoMorePages();
        assertQueueState(outputBuffer, FIRST, firstPartition, 10, 4, 10, 14, 0);
        assertQueueState(outputBuffer, SECOND, secondPartition, 0, 10, 0, 10, 0);
        outputBuffer.abort(SECOND);
        assertQueueClosed(outputBuffer, SECOND, 10);

        // not fully finished until all pages are consumed
        assertFalse(outputBuffer.isFinished());

        // remove a page, not finished
        assertBufferResultEquals(TYPES, getBufferResult(outputBuffer, FIRST, 5, sizeOfPages(1), NO_WAIT), bufferResult(5, createPage(5)));
        assertQueueState(outputBuffer, FIRST, firstPartition, 9, 5, 9, 14, 0);
        assertFalse(outputBuffer.isFinished());

        // remove all remaining pages from first queue, should not be finished
        BufferResult x = getBufferResult(outputBuffer, FIRST, 6, sizeOfPages(10), NO_WAIT);
        assertBufferResultEquals(TYPES, x, bufferResult(6, createPage(6),
                createPage(7),
                createPage(8),
                createPage(9),
                createPage(10),
                createPage(11),
                createPage(12),
                createPage(13)));
        assertQueueState(outputBuffer, FIRST, firstPartition, 8, 6, 8, 14, 0);
        // acknowledge all pages from the first partition, should transition to finished state
        assertBufferResultEquals(TYPES, getBufferResult(outputBuffer, FIRST, 14, sizeOfPages(10), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 14, true));
        outputBuffer.abort(FIRST);
        assertQueueClosed(outputBuffer, FIRST, 14);
        assertFinished(outputBuffer);
    }

    public static BufferResult getBufferResult(OutputBuffer outputBuffer, TaskId outputId, long sequenceId, DataSize maxSize, Duration maxWait)
    {
        CompletableFuture<BufferResult> future = outputBuffer.get(outputId, sequenceId, maxSize);
        return getFuture(future, maxWait);
    }

    public static BufferResult getFuture(CompletableFuture<BufferResult> future, Duration maxWait)
    {
        return tryGetFutureValue(future, (int) maxWait.toMillis(), TimeUnit.MILLISECONDS).get();
    }

    @Test
    public void testDuplicateRequests()
            throws Exception
    {
        OutputBuffer outputBuffer = new SharedBuffer(TASK_ID, TASK_INSTANCE_ID, stateNotificationExecutor, sizeOfPages(10));
        // add three items
        for (int i = 0; i < 3; i++) {
            addPage(outputBuffer, createPage(i));
        }

        // add a queue
        OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(SHARED);
        outputBuffers = outputBuffers.withBuffer(FIRST, 0);
        outputBuffer.setOutputBuffers(outputBuffers);
        assertQueueState(outputBuffer, FIRST, DEFAULT_PARTITION, 3, 0, 3, 3, 0);

        // get the three elements
        assertBufferResultEquals(TYPES, getBufferResult(outputBuffer, FIRST, 0, sizeOfPages(10), NO_WAIT), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        // pages not acknowledged yet so state is the same
        assertQueueState(outputBuffer, FIRST, DEFAULT_PARTITION, 3, 0, 3, 3, 0);

        // get the three elements again
        assertBufferResultEquals(TYPES, getBufferResult(outputBuffer, FIRST, 0, sizeOfPages(10), NO_WAIT), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        // pages not acknowledged yet so state is the same
        assertQueueState(outputBuffer, FIRST, DEFAULT_PARTITION, 3, 0, 3, 3, 0);

        // acknowledge the pages
        outputBuffer.get(FIRST, 3, sizeOfPages(10)).cancel(true);

        // attempt to get the three elements again
        assertBufferResultEquals(TYPES, getBufferResult(outputBuffer, FIRST, 0, sizeOfPages(10), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 0, false));
        // pages not acknowledged yet so state is the same
        assertQueueState(outputBuffer, FIRST, DEFAULT_PARTITION, 0, 3, 3, 3, 0);
    }

    @Test
    public void testAddQueueAfterNoMoreQueues()
            throws Exception
    {
        OutputBuffer outputBuffer = new SharedBuffer(TASK_ID, TASK_INSTANCE_ID, stateNotificationExecutor, sizeOfPages(10));
        assertFalse(outputBuffer.isFinished());

        // tell buffer no more queues will be added
        outputBuffer.setOutputBuffers(createInitialEmptyOutputBuffers(SHARED).withNoMoreBufferIds());
        assertFalse(outputBuffer.isFinished());

        // set no more queues a second time to assure that we don't get an exception or such
        outputBuffer.setOutputBuffers(createInitialEmptyOutputBuffers(SHARED).withNoMoreBufferIds());
        assertFalse(outputBuffer.isFinished());

        // set no more queues a third time to assure that we don't get an exception or such
        outputBuffer.setOutputBuffers(createInitialEmptyOutputBuffers(SHARED).withNoMoreBufferIds());
        assertFalse(outputBuffer.isFinished());

        try {
            OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(SHARED)
                    .withBuffer(FOO, 0)
                    .withNoMoreBufferIds();

            outputBuffer.setOutputBuffers(outputBuffers);
            fail("Expected IllegalStateException from addQueue after noMoreQueues has been called");
        }
        catch (IllegalStateException ignored) {
        }
    }

    @Test
    public void testAddQueueAfterDestroy()
            throws Exception
    {
        OutputBuffer outputBuffer = new SharedBuffer(TASK_ID, TASK_INSTANCE_ID, stateNotificationExecutor, sizeOfPages(10));
        assertFalse(outputBuffer.isFinished());

        // destroy buffer
        outputBuffer.destroy();
        assertFinished(outputBuffer);

        // set no more queues to assure that we don't get an exception or such
        outputBuffer.setOutputBuffers(createInitialEmptyOutputBuffers(SHARED).withNoMoreBufferIds());
        assertFinished(outputBuffer);

        // set no more queues a second time to assure that we don't get an exception or such
        outputBuffer.setOutputBuffers(createInitialEmptyOutputBuffers(SHARED).withNoMoreBufferIds());
        assertFinished(outputBuffer);

        // add queue calls after finish should be ignored
        outputBuffer.setOutputBuffers(createInitialEmptyOutputBuffers(SHARED).withBuffer(FOO, 0).withNoMoreBufferIds());
    }

    @Test
    public void testGetBeforeCreate()
            throws Exception
    {
        OutputBuffer outputBuffer = new SharedBuffer(TASK_ID, TASK_INSTANCE_ID, stateNotificationExecutor, sizeOfPages(10));
        assertFalse(outputBuffer.isFinished());

        // get a page from a buffer that doesn't exist yet
        CompletableFuture<BufferResult> future = outputBuffer.get(FIRST, (long) 0, sizeOfPages(1));
        assertFalse(future.isDone());

        // add a page and verify the future is not complete
        addPage(outputBuffer, createPage(33));
        assertFalse(future.isDone());

        // add the buffer and verify the future completed
        outputBuffer.setOutputBuffers(createInitialEmptyOutputBuffers(SHARED).withBuffer(FIRST, 0));
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), bufferResult(0, createPage(33)));
    }

    @Test
    public void testAbortBeforeCreate()
            throws Exception
    {
        OutputBuffer outputBuffer = new SharedBuffer(TASK_ID, TASK_INSTANCE_ID, stateNotificationExecutor, sizeOfPages(10));
        assertFalse(outputBuffer.isFinished());

        // get a page from a buffer that doesn't exist yet
        CompletableFuture<BufferResult> future = outputBuffer.get(FIRST, (long) 0, sizeOfPages(1));
        assertFalse(future.isDone());

        // abort that buffer
        outputBuffer.abort(FIRST);

        // add a page and verify the future is not complete
        addPage(outputBuffer, createPage(33));
        assertFalse(future.isDone());

        // add the buffer and verify we did not get the page
        outputBuffer.setOutputBuffers(createInitialEmptyOutputBuffers(SHARED).withBuffer(FIRST, 0));
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), emptyResults(TASK_INSTANCE_ID, 0, true));

        // verify that a normal read returns a closed empty result
        assertBufferResultEquals(TYPES, getBufferResult(outputBuffer, FIRST, 0, sizeOfPages(10), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 0, true));
    }

    @Test
    public void testAddStateMachine()
            throws Exception
    {
        // add after finish
        OutputBuffer outputBuffer = new SharedBuffer(TASK_ID, TASK_INSTANCE_ID, stateNotificationExecutor, sizeOfPages(10));
        outputBuffer.setNoMorePages();
        addPage(outputBuffer, createPage(0));
        addPage(outputBuffer, createPage(0));
        assertEquals(outputBuffer.getInfo().getTotalPagesSent(), 0);

        // add after destroy
        outputBuffer = new SharedBuffer(TASK_ID, TASK_INSTANCE_ID, stateNotificationExecutor, sizeOfPages(10));
        outputBuffer.destroy();
        addPage(outputBuffer, createPage(0));
        addPage(outputBuffer, createPage(0));
        assertEquals(outputBuffer.getInfo().getTotalPagesSent(), 0);
    }

    @Test
    public void testAbort()
            throws Exception
    {
        OutputBuffer outputBuffer = new SharedBuffer(TASK_ID, TASK_INSTANCE_ID, stateNotificationExecutor, sizeOfPages(10));

        // fill the buffer
        for (int i = 0; i < 10; i++) {
            addPage(outputBuffer, createPage(i));
        }
        outputBuffer.setNoMorePages();

        OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(SHARED)
                .withBuffer(FIRST, 0);
        outputBuffer.setOutputBuffers(outputBuffers);
        assertBufferResultEquals(TYPES, getBufferResult(outputBuffer, FIRST, 0, sizeOfPages(1), NO_WAIT), bufferResult(0, createPage(0)));
        outputBuffer.abort(FIRST);
        assertQueueClosed(outputBuffer, FIRST, 0);
        assertBufferResultEquals(TYPES, getBufferResult(outputBuffer, FIRST, 1, sizeOfPages(1), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 1, true));

        outputBuffers = outputBuffers.withBuffer(SECOND, 0).withNoMoreBufferIds();
        outputBuffer.setOutputBuffers(outputBuffers);
        assertBufferResultEquals(TYPES, getBufferResult(outputBuffer, SECOND, 0, sizeOfPages(1), NO_WAIT), bufferResult(0, createPage(0)));
        outputBuffer.abort(SECOND);
        assertQueueClosed(outputBuffer, SECOND, 0);
        assertFinished(outputBuffer);
        assertBufferResultEquals(TYPES, getBufferResult(outputBuffer, SECOND, 1, sizeOfPages(1), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 0, true));
    }

    @Test
    public void testFinishClosesEmptyQueues()
            throws Exception
    {
        OutputBuffer outputBuffer = new SharedBuffer(TASK_ID, TASK_INSTANCE_ID, stateNotificationExecutor, sizeOfPages(10));
        outputBuffer.setOutputBuffers(createInitialEmptyOutputBuffers(SHARED)
                .withBuffer(FIRST, 0)
                .withBuffer(SECOND, 0));

        // finish while queues are empty
        outputBuffer.setNoMorePages();

        assertQueueState(outputBuffer, FIRST, 0, 0, 0, 0, 0, 0);
        assertQueueState(outputBuffer, SECOND, 0, 0, 0, 0, 0, 0);

        outputBuffer.abort(FIRST);
        outputBuffer.abort(SECOND);

        assertQueueClosed(outputBuffer, FIRST, 0);
        assertQueueClosed(outputBuffer, SECOND, 0);
    }

    @Test
    public void testAbortFreesReader()
            throws Exception
    {
        OutputBuffer outputBuffer = new SharedBuffer(TASK_ID, TASK_INSTANCE_ID, stateNotificationExecutor, sizeOfPages(5));
        outputBuffer.setOutputBuffers(createInitialEmptyOutputBuffers(SHARED).withBuffer(QUEUE, 0));
        assertFalse(outputBuffer.isFinished());

        // attempt to get a page
        CompletableFuture<BufferResult> future = outputBuffer.get(QUEUE, 0, sizeOfPages(10));

        // verify we are waiting for a page
        assertFalse(future.isDone());

        // add one item
        addPage(outputBuffer, createPage(0));

        // verify we got one page
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), bufferResult(0, createPage(0)));

        // attempt to get another page, and verify we are blocked
        future = outputBuffer.get(QUEUE, 1, sizeOfPages(10));
        assertFalse(future.isDone());

        // abort the buffer
        outputBuffer.abort(QUEUE);
        assertQueueClosed(outputBuffer, QUEUE, 1);

        // verify the future completed
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), emptyResults(TASK_INSTANCE_ID, 1, true));
    }

    @Test
    public void testFinishFreesReader()
            throws Exception
    {
        OutputBuffer outputBuffer = new SharedBuffer(TASK_ID, TASK_INSTANCE_ID, stateNotificationExecutor, sizeOfPages(5));
        outputBuffer.setOutputBuffers(createInitialEmptyOutputBuffers(SHARED).withBuffer(QUEUE, 0));
        assertFalse(outputBuffer.isFinished());

        // attempt to get a page
        CompletableFuture<BufferResult> future = outputBuffer.get(QUEUE, 0, sizeOfPages(10));

        // verify we are waiting for a page
        assertFalse(future.isDone());

        // add one item
        addPage(outputBuffer, createPage(0));

        // verify we got one page
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), bufferResult(0, createPage(0)));

        // attempt to get another page, and verify we are blocked
        future = outputBuffer.get(QUEUE, 1, sizeOfPages(10));
        assertFalse(future.isDone());

        // finish the buffer
        outputBuffer.setNoMorePages();
        assertQueueState(outputBuffer, QUEUE, 0, 0, 1, 1, 1, 0);
        outputBuffer.abort(QUEUE);
        assertQueueClosed(outputBuffer, QUEUE, 1);

        // verify the future completed
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), emptyResults(TASK_INSTANCE_ID, 1, true));
    }

    @Test
    public void testFinishFreesWriter()
            throws Exception
    {
        OutputBuffer outputBuffer = new SharedBuffer(TASK_ID, TASK_INSTANCE_ID, stateNotificationExecutor, sizeOfPages(5));
        outputBuffer.setOutputBuffers(createInitialEmptyOutputBuffers(SHARED)
                .withBuffer(QUEUE, 0)
                .withNoMoreBufferIds());
        assertFalse(outputBuffer.isFinished());

        // fill the buffer
        for (int i = 0; i < 5; i++) {
            addPage(outputBuffer, createPage(i));
        }

        // enqueue the addition two pages more pages
        ListenableFuture<?> firstEnqueuePage = enqueuePage(outputBuffer, createPage(5));
        ListenableFuture<?> secondEnqueuePage = enqueuePage(outputBuffer, createPage(6));

        // get and acknowledge one page
        assertBufferResultEquals(TYPES, getBufferResult(outputBuffer, QUEUE, 0, sizeOfPages(1), MAX_WAIT), bufferResult(0, createPage(0)));
        outputBuffer.get(QUEUE, 1, sizeOfPages(1)).cancel(true);

        // verify the first blocked page was accepted but the second one was not
        assertTrue(firstEnqueuePage.isDone());
        assertFalse(secondEnqueuePage.isDone());

        // finish the query
        outputBuffer.setNoMorePages();
        assertFalse(outputBuffer.isFinished());

        // verify second future was completed
        assertTrue(secondEnqueuePage.isDone());

        // get the last 5 page (page 6 was never accepted)
        assertBufferResultEquals(TYPES, getBufferResult(outputBuffer, QUEUE, 1, sizeOfPages(100), NO_WAIT),
                bufferResult(1, createPage(1), createPage(2), createPage(3), createPage(4), createPage(5)));
        assertBufferResultEquals(TYPES, getBufferResult(outputBuffer, QUEUE, 6, sizeOfPages(100), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 6, true));

        outputBuffer.abort(QUEUE);

        // verify finished
        assertFinished(outputBuffer);
    }

    @Test
    public void testDestroyFreesReader()
            throws Exception
    {
        OutputBuffer outputBuffer = new SharedBuffer(TASK_ID, TASK_INSTANCE_ID, stateNotificationExecutor, sizeOfPages(5));
        outputBuffer.setOutputBuffers(createInitialEmptyOutputBuffers(SHARED)
                .withBuffer(QUEUE, 0)
                .withNoMoreBufferIds());
        assertFalse(outputBuffer.isFinished());

        // attempt to get a page
        CompletableFuture<BufferResult> future = outputBuffer.get(QUEUE, 0, sizeOfPages(10));

        // verify we are waiting for a page
        assertFalse(future.isDone());

        // add one page
        addPage(outputBuffer, createPage(0));

        // verify we got one page
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), bufferResult(0, createPage(0)));

        // attempt to get another page, and verify we are blocked
        future = outputBuffer.get(QUEUE, 1, sizeOfPages(10));
        assertFalse(future.isDone());

        // destroy the buffer
        outputBuffer.destroy();
        assertQueueClosed(outputBuffer, QUEUE, 1);

        // verify the future completed
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), emptyResults(TASK_INSTANCE_ID, 1, true));
    }

    @Test
    public void testDestroyFreesWriter()
            throws Exception
    {
        OutputBuffer outputBuffer = new SharedBuffer(TASK_ID, TASK_INSTANCE_ID, stateNotificationExecutor, sizeOfPages(5));
        outputBuffer.setOutputBuffers(createInitialEmptyOutputBuffers(SHARED)
                .withBuffer(QUEUE, 0)
                .withNoMoreBufferIds());
        assertFalse(outputBuffer.isFinished());

        // fill the buffer
        for (int i = 0; i < 5; i++) {
            addPage(outputBuffer, createPage(i));
        }

        // add two pages to the buffer queue
        ListenableFuture<?> firstEnqueuePage = enqueuePage(outputBuffer, createPage(5));
        ListenableFuture<?> secondEnqueuePage = enqueuePage(outputBuffer, createPage(6));

        // get and acknowledge one page
        assertBufferResultEquals(TYPES, getBufferResult(outputBuffer, QUEUE, 0, sizeOfPages(1), MAX_WAIT), bufferResult(0, createPage(0)));
        outputBuffer.get(QUEUE, 1, sizeOfPages(1)).cancel(true);

        // verify the first blocked page was accepted but the second one was not
        assertTrue(firstEnqueuePage.isDone());
        assertFalse(secondEnqueuePage.isDone());

        // destroy the buffer (i.e., cancel the query)
        outputBuffer.destroy();
        assertFinished(outputBuffer);

        // verify the second future was completed
        assertTrue(secondEnqueuePage.isDone());
    }

    @Test
    public void testBufferCompletion()
            throws Exception
    {
        OutputBuffer outputBuffer = new SharedBuffer(TASK_ID, TASK_INSTANCE_ID, stateNotificationExecutor, sizeOfPages(5));
        outputBuffer.setOutputBuffers(createInitialEmptyOutputBuffers(SHARED)
                .withBuffer(QUEUE, 0)
                .withNoMoreBufferIds());

        assertFalse(outputBuffer.isFinished());

        // fill the buffer
        List<Page> pages = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Page page = createPage(i);
            addPage(outputBuffer, page);
            pages.add(page);
        }

        outputBuffer.setNoMorePages();

        // get and acknowledge 5 pages
        assertBufferResultEquals(TYPES, getBufferResult(outputBuffer, QUEUE, 0, sizeOfPages(5), MAX_WAIT), bufferResult(0, pages));

        // buffer is not finished
        assertFalse(outputBuffer.isFinished());

        // there are no more pages and no more buffers, but buffer is not finished because it didn't receive an acknowledgement yet
        assertFalse(outputBuffer.isFinished());

        // ask the buffer to finish
        outputBuffer.abort(QUEUE);

        // verify that the buffer is finished
        assertTrue(outputBuffer.isFinished());
    }

    private static ListenableFuture<?> enqueuePage(OutputBuffer outputBuffer, Page page)
    {
        return enqueuePage(outputBuffer, page, DEFAULT_PARTITION);
    }

    private static ListenableFuture<?> enqueuePage(OutputBuffer outputBuffer, Page page, int partition)
    {
        ListenableFuture<?> future = outputBuffer.enqueue(partition, page);
        assertFalse(future.isDone());
        return future;
    }

    private static void addPage(OutputBuffer outputBuffer, Page page)
    {
        addPage(outputBuffer, page, DEFAULT_PARTITION);
    }

    private static void addPage(OutputBuffer outputBuffer, Page page, int partition)
    {
        assertTrue(outputBuffer.enqueue(partition, page).isDone());
    }

    private static void assertQueueState(OutputBuffer outputBuffer, TaskId queueId, int partition, int bufferedPages, int pagesSent, int pageBufferBufferedPages, int pageBufferPagesSent, int pageBufferQueuedPages)
    {
        assertEquals(
                getBufferInfo(outputBuffer, queueId),
                new BufferInfo(queueId,
                        false, bufferedPages, pagesSent, new PageBufferInfo(partition,
                        pageBufferBufferedPages, pageBufferQueuedPages, sizeOfPages(pageBufferBufferedPages).toBytes(), pageBufferPagesSent)));
    }

    @SuppressWarnings("ConstantConditions")
    private static void assertQueueClosed(OutputBuffer outputBuffer, TaskId queueId, int pagesSent)
    {
        BufferInfo bufferInfo = getBufferInfo(outputBuffer, queueId);
        assertEquals(bufferInfo.getBufferedPages(), 0);
        assertEquals(bufferInfo.getPagesSent(), pagesSent);
        assertEquals(bufferInfo.isFinished(), true);
    }

    private static BufferInfo getBufferInfo(OutputBuffer outputBuffer, TaskId queueId)
    {
        for (BufferInfo bufferInfo : outputBuffer.getInfo().getBuffers()) {
            if (bufferInfo.getBufferId().equals(queueId)) {
                return bufferInfo;
            }
        }
        return null;
    }

    private static void assertFinished(OutputBuffer outputBuffer)
            throws Exception
    {
        assertTrue(outputBuffer.isFinished());
        for (BufferInfo bufferInfo : outputBuffer.getInfo().getBuffers()) {
            assertTrue(bufferInfo.isFinished());
            assertEquals(bufferInfo.getBufferedPages(), 0);
        }
    }

    private static void assertBufferResultEquals(List<? extends Type> types, BufferResult actual, BufferResult expected)
    {
        assertEquals(actual.getPages().size(), expected.getPages().size());
        assertEquals(actual.getToken(), expected.getToken());
        for (int i = 0; i < actual.getPages().size(); i++) {
            Page actualPage = actual.getPages().get(i);
            Page expectedPage = expected.getPages().get(i);
            assertEquals(actualPage.getChannelCount(), expectedPage.getChannelCount());
            PageAssertions.assertPageEquals(types, actualPage, expectedPage);
        }
        assertEquals(actual.isBufferComplete(), expected.isBufferComplete());
    }

    public static BufferResult bufferResult(long token, Page firstPage, Page... otherPages)
    {
        List<Page> pages = ImmutableList.<Page>builder().add(firstPage).add(otherPages).build();
        return bufferResult(token, pages);
    }

    public static BufferResult bufferResult(long token, List<Page> pages)
    {
        checkArgument(!pages.isEmpty(), "pages is empty");
        return new BufferResult(TASK_INSTANCE_ID, token, token + pages.size(), false, pages);
    }
}
