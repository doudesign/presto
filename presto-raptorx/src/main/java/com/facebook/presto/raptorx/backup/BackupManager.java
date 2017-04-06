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
package com.facebook.presto.raptorx.backup;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.units.DataSize;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.File;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.Duration.nanosSince;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class BackupManager
{
    private final Optional<BackupStore> backupStore;
    private final ListeningExecutorService executorService;

    private final AtomicInteger pendingBackups = new AtomicInteger();
    private final BackupStats stats = new BackupStats();

    @Inject
    public BackupManager(Optional<BackupStore> backupStore, BackupConfig config)
    {
        this(backupStore, config.getBackupThreads());
    }

    public BackupManager(Optional<BackupStore> backupStore, int backupThreads)
    {
        checkArgument(backupThreads > 0, "backupThreads must be > 0");

        this.backupStore = requireNonNull(backupStore, "backupStore is null");
        this.executorService = listeningDecorator(newFixedThreadPool(backupThreads, daemonThreadsNamed("background-chunk-backup-%s")));
    }

    @PreDestroy
    public void shutdown()
    {
        executorService.shutdownNow();
    }

    public ListenableFuture<?> submit(long chunkId, File source)
    {
        requireNonNull(source, "source is null");

        if (!backupStore.isPresent()) {
            return immediateFuture(null);
        }

        pendingBackups.incrementAndGet();
        long queuedTime = System.nanoTime();

        return executorService.submit(() -> {
            try {
                stats.addQueuedTime(nanosSince(queuedTime));
                runBackup(backupStore.get(), chunkId, source);
            }
            finally {
                pendingBackups.decrementAndGet();
            }
        });
    }

    private void runBackup(BackupStore backupStore, long chunkId, File source)
    {
        try {
            long start = System.nanoTime();
            backupStore.backupChunk(chunkId, source);
            stats.addCopyChunkDataRate(new DataSize(source.length(), BYTE), nanosSince(start));
            stats.incrementBackupSuccess();
        }
        catch (Throwable t) {
            stats.incrementBackupFailure();
            throw t;
        }
    }

    @Managed
    public int getPendingBackupCount()
    {
        return pendingBackups.get();
    }

    @Managed
    @Flatten
    public BackupStats getStats()
    {
        return stats;
    }
}
