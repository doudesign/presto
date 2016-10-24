package com.facebook.presto.raptor.locks;

import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.concurrent.ExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.testing.Assertions.assertLessThan;
import static io.airlift.units.Duration.nanosSince;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestLockManager
{
    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("test-%s"));

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        executor.shutdownNow();
    }

    @Test
    public void testLocking()
            throws Exception
    {
        LockManager lockManager = new LockManager();

        // nothing is locked
        assertEquals(lockManager.waitingTransactions(), ImmutableSet.of());

        // acquire two locks on different resources
        lockManager.lock(1, new SchemaNameLockResource("a"), new Duration(0, SECONDS));
        lockManager.lock(2, new SchemaNameLockResource("b"), new Duration(0, SECONDS));

        assertEquals(lockManager.waitingTransactions(), ImmutableSet.of());

        // attempt to acquire already locked resource in separate thread
        executor.submit(() -> lockManager.lock(1, new SchemaNameLockResource("b"), new Duration(10, SECONDS)));

        // wait until thread is waiting
        waitForWaiters(lockManager, 1L);

        // unlock blocking transaction to unblock waiter
        lockManager.unlockAll(2);

        assertEquals(lockManager.waitingTransactions(), ImmutableSet.of());
    }

    @Test
    public void testTimeout()
            throws Exception
    {
        LockManager lockManager = new LockManager();

        lockManager.lock(1, new SchemaNameLockResource("a"), new Duration(0, SECONDS));

        try {
            lockManager.lock(2, new SchemaNameLockResource("a"), new Duration(0, SECONDS));
            fail("exception expected");
        }
        catch (LockTimeoutException ignored) {
        }
    }

    @Test
    public void testDeadlock()
            throws Exception
    {
        LockManager lockManager = new LockManager();

        // acquire three locks on different resources
        lockManager.lock(1, new SchemaNameLockResource("a"), new Duration(0, SECONDS));
        lockManager.lock(2, new SchemaNameLockResource("b"), new Duration(0, SECONDS));
        lockManager.lock(3, new SchemaNameLockResource("c"), new Duration(0, SECONDS));

        assertEquals(lockManager.waitingTransactions(), ImmutableSet.of());

        // attempt to acquire already locked resources in separate threads
        executor.submit(() -> lockManager.lock(1, new SchemaNameLockResource("b"), new Duration(10, SECONDS)));
        executor.submit(() -> lockManager.lock(2, new SchemaNameLockResource("c"), new Duration(10, SECONDS)));

        // wait until both threads are waiting
        waitForWaiters(lockManager, 1L, 2L);

        // create deadlock: 1 -> 2 -> 3 -> 1
        try {
            lockManager.lock(3, new SchemaNameLockResource("a"), new Duration(10, SECONDS));
            fail("expected exception");
        }
        catch (DeadlockDetectedException ignored) {
        }
    }

    private static void waitForWaiters(LockManager lockManager, Long... transactionIds)
            throws InterruptedException
    {
        long start = System.nanoTime();
        while (!lockManager.waitingTransactions().equals(ImmutableSet.copyOf(transactionIds))) {
            MILLISECONDS.sleep(10);
            assertLessThan(nanosSince(start), new Duration(10, SECONDS));
        }
    }
}
