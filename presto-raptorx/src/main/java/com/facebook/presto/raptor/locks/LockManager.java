package com.facebook.presto.raptor.locks;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import io.airlift.units.Duration;
import org.jgrapht.DirectedGraph;
import org.jgrapht.alg.CycleDetector;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;

import javax.annotation.concurrent.GuardedBy;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class LockManager
{
    @GuardedBy("this")
    private final Multimap<Long, Lock> locksByTransaction = ArrayListMultimap.create();

    @GuardedBy("this")
    private final Map<LockResource, Lock> locksByResource = new HashMap<>();

    @GuardedBy("this")
    private final DirectedGraph<Long, DefaultEdge> lockGraph = new DefaultDirectedGraph<>(DefaultEdge.class);

    private final Set<Long> waitingTransactions = newConcurrentHashSet();

    public void lock(long transactionId, LockResource resource, Duration timeout)
    {
        checkState(!waitingTransactions.contains(transactionId), "transaction is already waiting");

        while (true) {
            Lock lockedLock;

            synchronized (this) {
                // return if current transaction already owns resource (reentrant)
                if (locksByTransaction.get(transactionId).stream()
                        .anyMatch(lock -> lock.getResource().equals(resource))) {
                    return;
                }

                // get resource lock
                lockedLock = locksByResource.get(resource);

                // lock resource if not locked
                if (lockedLock == null) {
                    Lock lock = new Lock(transactionId, resource);
                    locksByTransaction.put(transactionId, lock);
                    locksByResource.put(resource, lock);
                    return;
                }

                // check for deadlock before waiting
                lockGraph.addVertex(transactionId);
                lockGraph.addVertex(lockedLock.getTransactionId());
                lockGraph.addEdge(transactionId, lockedLock.getTransactionId());
                if (new CycleDetector<>(lockGraph).detectCycles()) {
                    throw new DeadlockDetectedException();
                }
            }

            // wait for lock
            try {
                waitingTransactions.add(transactionId);
                lockedLock.waitForLock(timeout);
            }
            finally {
                waitingTransactions.remove(transactionId);
            }
        }
    }

    public synchronized void unlockAll(long transactionId)
    {
        for (Lock lock : locksByTransaction.removeAll(transactionId)) {
            lock.unlock();
            locksByResource.remove(lock.getResource());
            lockGraph.removeVertex(transactionId);
        }
    }

    @VisibleForTesting
    Set<Long> waitingTransactions()
    {
        return ImmutableSet.copyOf(waitingTransactions);
    }

    private static class Lock
    {
        private final long transactionId;
        private final LockResource resource;
        private final CountDownLatch latch = new CountDownLatch(1);

        public Lock(long transactionId, LockResource resource)
        {
            this.transactionId = transactionId;
            this.resource = requireNonNull(resource, "resource is null");
        }

        public long getTransactionId()
        {
            return transactionId;
        }

        public LockResource getResource()
        {
            return resource;
        }

        public void waitForLock(Duration timeout)
        {
            try {
                if (!latch.await(timeout.toMillis(), MILLISECONDS)) {
                    throw new LockTimeoutException();
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        public void unlock()
        {
            latch.countDown();
        }
    }
}
