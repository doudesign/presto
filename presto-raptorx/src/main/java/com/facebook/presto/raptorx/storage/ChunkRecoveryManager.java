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
package com.facebook.presto.raptorx.storage;

import com.facebook.presto.raptorx.backup.BackupStore;
import com.facebook.presto.raptorx.metadata.ChunkMetadata;
import com.facebook.presto.raptorx.util.PrioritizedFifoExecutor;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PrestoException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static com.facebook.presto.raptorx.RaptorErrorCode.RAPTOR_RECOVERY_ERROR;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.succinctBytes;
import static io.airlift.units.DataSize.succinctDataSize;
import static io.airlift.units.Duration.nanosSince;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;

public class ChunkRecoveryManager
{
    private static final Logger log = Logger.get(ChunkRecoveryManager.class);

    private final StorageService storageService;
    private final Optional<BackupStore> backupStore;
    private final String nodeIdentifier;
    private final ChunkManager chunkManager;
    private final Duration missingChunkDiscoveryInterval;

    private final AtomicBoolean started = new AtomicBoolean();
    private final MissingChunksQueue chunkQueue;

    private final ScheduledExecutorService missingChunkExecutor = newScheduledThreadPool(1, daemonThreadsNamed("missing-chunk-discovery"));
    private final ExecutorService executorService = newCachedThreadPool(daemonThreadsNamed("chunk-recovery-%s"));
    private final ChunkRecoveryStats stats;

    @Inject
    public ChunkRecoveryManager(
            StorageService storageService,
            Optional<BackupStore> backupStore,
            NodeManager nodeManager,
            ChunkManager chunkManager,
            StorageManagerConfig config)
    {
        this(storageService,
                backupStore,
                nodeManager,
                chunkManager,
                config.getMissingChunkDiscoveryInterval(),
                config.getRecoveryThreads());
    }

    public ChunkRecoveryManager(
            StorageService storageService,
            Optional<BackupStore> backupStore,
            NodeManager nodeManager,
            ChunkManager chunkManager,
            Duration missingChunkDiscoveryInterval,
            int recoveryThreads)
    {
        this.storageService = requireNonNull(storageService, "storageService is null");
        this.backupStore = requireNonNull(backupStore, "backupStore is null");
        this.nodeIdentifier = requireNonNull(nodeManager, "nodeManager is null").getCurrentNode().getNodeIdentifier();
        this.chunkManager = requireNonNull(chunkManager, "chunkManager is null");
        this.missingChunkDiscoveryInterval = requireNonNull(missingChunkDiscoveryInterval, "missingChunkDiscoveryInterval is null");
        this.chunkQueue = new MissingChunksQueue(new PrioritizedFifoExecutor<>(executorService, recoveryThreads, new MissingChunkComparator()));
        this.stats = new ChunkRecoveryStats();
    }

    @PostConstruct
    public void start()
    {
        if (!backupStore.isPresent()) {
            return;
        }
        if (started.compareAndSet(false, true)) {
            scheduleRecoverMissingChunks();
        }
    }

    @PreDestroy
    public void shutdown()
    {
        executorService.shutdownNow();
        missingChunkExecutor.shutdownNow();
    }

    private void scheduleRecoverMissingChunks()
    {
        missingChunkExecutor.scheduleWithFixedDelay(() -> {
            try {
                // jitter to avoid overloading database
                long interval = missingChunkDiscoveryInterval.roundTo(SECONDS);
                SECONDS.sleep(ThreadLocalRandom.current().nextLong(1, interval));
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            enqueueMissingChunks();
        }, 0, missingChunkDiscoveryInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Managed
    public void recoverMissingChunks()
    {
        missingChunkExecutor.submit(this::enqueueMissingChunks);
    }

    private synchronized void enqueueMissingChunks()
    {
        try {
            for (ChunkMetadata chunk : getMissingChunks()) {
                stats.incrementBackgroundChunkRecovery();
                Futures.addCallback(
                        chunkQueue.submit(MissingChunk.createBackgroundMissingChunk(chunk.getChunkId(), chunk.getCompressedSize())),
                        failureCallback(t -> log.warn(t, "Error recovering chunk: %s", chunk.getChunkId())));
            }
        }
        catch (Throwable t) {
            log.error(t, "Error creating chunk recovery tasks");
        }
    }

    private Set<ChunkMetadata> getMissingChunks()
    {
        return chunkManager.getNodeChunks(nodeIdentifier).stream()
                .filter(chunk -> chunkNeedsRecovery(chunk.getChunkId(), chunk.getCompressedSize()))
                .collect(toSet());
    }

    private boolean chunkNeedsRecovery(long chunkId, long chunkSize)
    {
        File storageFile = storageService.getStorageFile(chunkId);
        return !storageFile.exists() || (storageFile.length() != chunkSize);
    }

    public Future<?> recoverChunk(long chunkId)
            throws ExecutionException
    {
        stats.incrementActiveChunkRecovery();
        return chunkQueue.submit(MissingChunk.createActiveMissingChunk(chunkId));
    }

    @VisibleForTesting
    void restoreFromBackup(long chunkId, OptionalLong chunkSize)
    {
        File storageFile = storageService.getStorageFile(chunkId);

        if (!backupStore.get().chunkExists(chunkId)) {
            stats.incrementChunkRecoveryBackupNotFound();
            throw new PrestoException(RAPTOR_RECOVERY_ERROR, "No backup file found for chunk: " + chunkId);
        }

        if (storageFile.exists()) {
            if (!chunkSize.isPresent() || (storageFile.length() == chunkSize.getAsLong())) {
                return;
            }
            log.warn("Local chunk file is corrupt. Deleting local file: %s", storageFile);
            storageFile.delete();
        }

        // create a temporary file in the staging directory
        File stagingFile = temporarySuffix(storageService.getStagingFile(chunkId));
        storageService.createParents(stagingFile);

        // copy to temporary file
        log.info("Copying chunk %s from backup...", chunkId);
        long start = System.nanoTime();

        try {
            backupStore.get().restoreChunk(chunkId, stagingFile);
        }
        catch (PrestoException e) {
            stats.incrementChunkRecoveryFailure();
            stagingFile.delete();
            throw e;
        }

        Duration duration = nanosSince(start);
        DataSize size = succinctBytes(stagingFile.length());
        DataSize rate = dataRate(size, duration).convertToMostSuccinctDataSize();
        stats.addChunkRecoveryDataRate(rate, size, duration);

        log.info("Copied chunk %s from backup in %s (%s at %s/s)", chunkId, duration, size, rate);

        // move to final location
        storageService.createParents(storageFile);
        try {
            Files.move(stagingFile.toPath(), storageFile.toPath(), ATOMIC_MOVE);
        }
        catch (FileAlreadyExistsException e) {
            // someone else already created it (should not happen, but safe to ignore)
        }
        catch (IOException e) {
            stats.incrementChunkRecoveryFailure();
            throw new PrestoException(RAPTOR_RECOVERY_ERROR, "Failed to move chunk: " + chunkId, e);
        }
        finally {
            stagingFile.delete();
        }

        if (!storageFile.exists() || (chunkSize.isPresent() && (storageFile.length() != chunkSize.getAsLong()))) {
            stats.incrementChunkRecoveryFailure();
            log.info("Files do not match after recovery. Deleting local file: " + chunkId);
            storageFile.delete();
            throw new PrestoException(RAPTOR_RECOVERY_ERROR, "File not recovered correctly: " + chunkId);
        }

        stats.incrementChunkRecoverySuccess();
    }

    @VisibleForTesting
    static class MissingChunkComparator
            implements Comparator<MissingChunkRunnable>
    {
        @Override
        public int compare(MissingChunkRunnable chunk1, MissingChunkRunnable chunk2)
        {
            if (chunk1.isActive() == chunk2.isActive()) {
                return 0;
            }
            return chunk1.isActive() ? -1 : 1;
        }
    }

    interface MissingChunkRunnable
            extends Runnable
    {
        boolean isActive();
    }

    private class MissingChunkRecovery
            implements MissingChunkRunnable
    {
        private final long chunkId;
        private final OptionalLong chunkSize;
        private final boolean active;

        public MissingChunkRecovery(long chunkId, OptionalLong chunkSize, boolean active)
        {
            this.chunkId = chunkId;
            this.chunkSize = requireNonNull(chunkSize, "chunkSize is null");
            this.active = active;
        }

        @Override
        public void run()
        {
            restoreFromBackup(chunkId, chunkSize);
        }

        @Override
        public boolean isActive()
        {
            return active;
        }
    }

    private static final class MissingChunk
    {
        private final long chunkId;
        private final OptionalLong chunkSize;
        private final boolean active;

        private MissingChunk(long chunkId, OptionalLong chunkSize, boolean active)
        {
            this.chunkId = chunkId;
            this.chunkSize = requireNonNull(chunkSize, "chunkSize is null");
            this.active = active;
        }

        public static MissingChunk createBackgroundMissingChunk(long chunkId, long chunkSize)
        {
            return new MissingChunk(chunkId, OptionalLong.of(chunkSize), false);
        }

        public static MissingChunk createActiveMissingChunk(long chunkId)
        {
            return new MissingChunk(chunkId, OptionalLong.empty(), true);
        }

        public long getChunkId()
        {
            return chunkId;
        }

        public OptionalLong getChunkSize()
        {
            return chunkSize;
        }

        public boolean isActive()
        {
            return active;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            MissingChunk other = (MissingChunk) o;
            return Objects.equals(this.chunkId, other.chunkId) &&
                    Objects.equals(this.active, other.active);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(chunkId, active);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("chunkId", chunkId)
                    .add("active", active)
                    .toString();
        }
    }

    private class MissingChunksQueue
    {
        private final LoadingCache<MissingChunk, ListenableFuture<?>> queuedMissingChunks;

        public MissingChunksQueue(PrioritizedFifoExecutor<MissingChunkRunnable> chunkRecoveryExecutor)
        {
            requireNonNull(chunkRecoveryExecutor, "chunkRecoveryExecutor is null");
            this.queuedMissingChunks = CacheBuilder.newBuilder().build(new CacheLoader<MissingChunk, ListenableFuture<?>>()
            {
                @Override
                public ListenableFuture<?> load(MissingChunk missingChunk)
                {
                    MissingChunkRecovery task = new MissingChunkRecovery(
                            missingChunk.getChunkId(),
                            missingChunk.getChunkSize(),
                            missingChunk.isActive());
                    ListenableFuture<?> future = chunkRecoveryExecutor.submit(task);
                    future.addListener(() -> queuedMissingChunks.invalidate(missingChunk), directExecutor());
                    return future;
                }
            });
        }

        public ListenableFuture<?> submit(MissingChunk chunk)
                throws ExecutionException
        {
            return queuedMissingChunks.get(chunk);
        }
    }

    static DataSize dataRate(DataSize size, Duration duration)
    {
        double rate = size.toBytes() / duration.getValue(SECONDS);
        if (Double.isNaN(rate) || Double.isInfinite(rate)) {
            rate = 0;
        }
        return succinctDataSize(rate, BYTE);
    }

    private static File temporarySuffix(File file)
    {
        return new File(file.getPath() + ".tmp-" + UUID.randomUUID());
    }

    @Managed
    @Flatten
    public ChunkRecoveryStats getStats()
    {
        return stats;
    }

    private static <T> FutureCallback<T> failureCallback(Consumer<Throwable> callback)
    {
        return new FutureCallback<T>()
        {
            @Override
            public void onSuccess(T result) {}

            @Override
            public void onFailure(Throwable throwable)
            {
                callback.accept(throwable);
            }
        };
    }
}
