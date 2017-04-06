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

import com.facebook.presto.orc.FileOrcDataSource;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcPredicate;
import com.facebook.presto.orc.OrcReader;
import com.facebook.presto.orc.OrcRecordReader;
import com.facebook.presto.orc.TupleDomainOrcPredicate;
import com.facebook.presto.orc.TupleDomainOrcPredicate.ColumnReference;
import com.facebook.presto.orc.memory.AggregatedMemoryContext;
import com.facebook.presto.orc.metadata.OrcMetadataReader;
import com.facebook.presto.raptorx.RaptorColumnHandle;
import com.facebook.presto.raptorx.backup.BackupManager;
import com.facebook.presto.raptorx.backup.BackupStore;
import com.facebook.presto.raptorx.metadata.ColumnInfo;
import com.facebook.presto.raptorx.storage.OrcFileRewriter.OrcFileInfo;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.facebook.presto.raptorx.RaptorColumnHandle.isBucketNumberColumn;
import static com.facebook.presto.raptorx.RaptorColumnHandle.isChunkIdColumn;
import static com.facebook.presto.raptorx.RaptorColumnHandle.isChunkRowIdColumn;
import static com.facebook.presto.raptorx.RaptorColumnHandle.isHiddenColumn;
import static com.facebook.presto.raptorx.RaptorErrorCode.RAPTOR_ERROR;
import static com.facebook.presto.raptorx.RaptorErrorCode.RAPTOR_LOCAL_DISK_FULL;
import static com.facebook.presto.raptorx.RaptorErrorCode.RAPTOR_RECOVERY_ERROR;
import static com.facebook.presto.raptorx.RaptorErrorCode.RAPTOR_RECOVERY_TIMEOUT;
import static com.facebook.presto.raptorx.storage.ChunkStats.computeColumnStats;
import static com.facebook.presto.raptorx.storage.OrcPageSource.BUCKET_NUMBER_COLUMN;
import static com.facebook.presto.raptorx.storage.OrcPageSource.NULL_COLUMN;
import static com.facebook.presto.raptorx.storage.OrcPageSource.ROW_ID_COLUMN;
import static com.facebook.presto.raptorx.storage.OrcPageSource.CHUNK_ID_COLUMN;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.propagateIfInstanceOf;
import static com.google.common.util.concurrent.Futures.whenAllComplete;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.lang.Math.min;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.stream.Collectors.toList;
import static org.joda.time.DateTimeZone.UTC;

public class OrcStorageManager
        implements StorageManager
{
    private static final JsonCodec<ChunkDelta> CHUNK_DELTA_CODEC = jsonCodec(ChunkDelta.class);

    private static final long MAX_CHUNK_ROWS = 1_000_000_000;
    private static final JsonCodec<OrcFileMetadata> METADATA_CODEC = jsonCodec(OrcFileMetadata.class);

    private final String nodeIdentifier;
    private final StorageService storageService;
    private final Optional<BackupStore> backupStore;
    private final ReaderAttributes defaultReaderAttributes;
    private final BackupManager backupManager;
    private final ChunkRecoveryManager recoveryManager;
    private final ChunkRecorder chunkRecorder;
    private final ChunkIdSequence chunkIdSequence;
    private final Duration recoveryTimeout;
    private final long maxChunkRows;
    private final DataSize maxChunkSize;
    private final DataSize minAvailableSpace;
    private final TypeManager typeManager;
    private final ExecutorService deletionExecutor;
    private final ExecutorService commitExecutor;

    @Inject
    public OrcStorageManager(
            NodeManager nodeManager,
            StorageService storageService,
            Optional<BackupStore> backupStore,
            ReaderAttributes readerAttributes,
            StorageManagerConfig config,
            BackupManager backgroundBackupManager,
            ChunkRecoveryManager recoveryManager,
            ChunkRecorder chunkRecorder,
            ChunkIdSequence chunkIdSequence,
            TypeManager typeManager)
    {
        this(nodeManager.getCurrentNode().getNodeIdentifier(),
                storageService,
                backupStore,
                readerAttributes,
                backgroundBackupManager,
                recoveryManager,
                chunkRecorder,
                chunkIdSequence,
                typeManager,
                config.getDeletionThreads(),
                config.getChunkRecoveryTimeout(),
                config.getMaxChunkRows(),
                config.getMaxChunkSize(),
                config.getMinAvailableSpace());
    }

    public OrcStorageManager(
            String nodeIdentifier,
            StorageService storageService,
            Optional<BackupStore> backupStore,
            ReaderAttributes readerAttributes,
            BackupManager backgroundBackupManager,
            ChunkRecoveryManager recoveryManager,
            ChunkRecorder chunkRecorder,
            ChunkIdSequence chunkIdSequence,
            TypeManager typeManager,
            int deletionThreads,
            Duration chunkRecoveryTimeout,
            long maxChunkRows,
            DataSize maxChunkSize,
            DataSize minAvailableSpace)
    {
        this.nodeIdentifier = requireNonNull(nodeIdentifier, "nodeId is null");
        this.storageService = requireNonNull(storageService, "storageService is null");
        this.backupStore = requireNonNull(backupStore, "backupStore is null");
        this.defaultReaderAttributes = requireNonNull(readerAttributes, "readerAttributes is null");

        backupManager = requireNonNull(backgroundBackupManager, "backgroundBackupManager is null");
        this.recoveryManager = requireNonNull(recoveryManager, "recoveryManager is null");
        this.recoveryTimeout = requireNonNull(chunkRecoveryTimeout, "chunkRecoveryTimeout is null");

        checkArgument(maxChunkRows > 0, "maxChunkRows must be greater than zero");
        this.maxChunkRows = min(maxChunkRows, MAX_CHUNK_ROWS);
        this.maxChunkSize = requireNonNull(maxChunkSize, "maxChunkSize is null");
        this.minAvailableSpace = requireNonNull(minAvailableSpace, "minAvailableSpace is null");
        this.chunkRecorder = requireNonNull(chunkRecorder, "chunkRecorder is null");
        this.chunkIdSequence = requireNonNull(chunkIdSequence, "chunkIdSequence is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.deletionExecutor = newFixedThreadPool(deletionThreads, daemonThreadsNamed("raptor-delete-%s"));
        this.commitExecutor = newCachedThreadPool(daemonThreadsNamed("raptor-commit-%s"));
    }

    @PreDestroy
    public void shutdown()
    {
        deletionExecutor.shutdownNow();
        commitExecutor.shutdown();
    }

    @Override
    public ConnectorPageSource getPageSource(
            long chunkId,
            int bucketNumber,
            List<Long> columnIds,
            List<Type> columnTypes,
            TupleDomain<RaptorColumnHandle> effectivePredicate,
            ReaderAttributes readerAttributes,
            OptionalLong transactionId)
    {
        OrcDataSource dataSource = openChunk(chunkId, readerAttributes);

        AggregatedMemoryContext systemMemoryUsage = new AggregatedMemoryContext();

        try {
            OrcReader reader = new OrcReader(dataSource, new OrcMetadataReader(), readerAttributes.getMaxMergeDistance(), readerAttributes.getMaxReadSize());

            Map<Long, Integer> indexMap = columnIdIndex(reader.getColumnNames());
            ImmutableMap.Builder<Integer, Type> includedColumns = ImmutableMap.builder();
            ImmutableList.Builder<Integer> columnIndexes = ImmutableList.builder();
            for (int i = 0; i < columnIds.size(); i++) {
                long columnId = columnIds.get(i);
                if (isHiddenColumn(columnId)) {
                    columnIndexes.add(toSpecialIndex(columnId));
                    continue;
                }

                Integer index = indexMap.get(columnId);
                if (index == null) {
                    columnIndexes.add(NULL_COLUMN);
                }
                else {
                    columnIndexes.add(index);
                    includedColumns.put(index, columnTypes.get(i));
                }
            }

            OrcPredicate predicate = getPredicate(effectivePredicate, indexMap);

            OrcRecordReader recordReader = reader.createRecordReader(includedColumns.build(), predicate, UTC, systemMemoryUsage);

            Optional<ChunkRewriter> chunkRewriter = Optional.empty();
            if (transactionId.isPresent()) {
                chunkRewriter = Optional.of(createChunkRewriter(transactionId.getAsLong(), bucketNumber, chunkId));
            }

            return new OrcPageSource(chunkRewriter, recordReader, dataSource, columnIds, columnTypes, columnIndexes.build(), chunkId, bucketNumber, systemMemoryUsage);
        }
        catch (IOException | RuntimeException e) {
            closeQuietly(dataSource);
            throw new PrestoException(RAPTOR_ERROR, "Failed to create page source for chunk " + chunkId, e);
        }
        catch (Throwable t) {
            closeQuietly(dataSource);
            throw t;
        }
    }

    private static int toSpecialIndex(long columnId)
    {
        if (isChunkRowIdColumn(columnId)) {
            return ROW_ID_COLUMN;
        }
        if (isChunkIdColumn(columnId)) {
            return CHUNK_ID_COLUMN;
        }
        if (isBucketNumberColumn(columnId)) {
            return BUCKET_NUMBER_COLUMN;
        }
        throw new PrestoException(RAPTOR_ERROR, "Invalid column ID: " + columnId);
    }

    @Override
    public StoragePageSink createStoragePageSink(long transactionId, int bucketNumber, List<Long> columnIds, List<Type> columnTypes, boolean checkSpace)
    {
        if (storageService.getAvailableBytes() < minAvailableSpace.toBytes()) {
            throw new PrestoException(RAPTOR_LOCAL_DISK_FULL, "Local disk is full on node " + nodeIdentifier);
        }
        return new OrcStoragePageSink(transactionId, columnIds, columnTypes, bucketNumber);
    }

    private ChunkRewriter createChunkRewriter(long transactionId, int bucketNumber, long chunkId)
    {
        return rowsToDelete -> {
            if (rowsToDelete.isEmpty()) {
                return completedFuture(ImmutableList.of());
            }
            return supplyAsync(() -> rewriteChunk(transactionId, bucketNumber, chunkId, rowsToDelete), deletionExecutor);
        };
    }

    private void writeChunk(long chunkId)
    {
        if (backupStore.isPresent() && !backupStore.get().chunkExists(chunkId)) {
            throw new PrestoException(RAPTOR_ERROR, "Backup does not exist after write");
        }

        File stagingFile = storageService.getStagingFile(chunkId);
        File storageFile = storageService.getStorageFile(chunkId);

        storageService.createParents(storageFile);

        try {
            Files.move(stagingFile.toPath(), storageFile.toPath(), ATOMIC_MOVE);
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_ERROR, "Failed to move chunk file", e);
        }
    }

    @VisibleForTesting
    OrcDataSource openChunk(long chunkId, ReaderAttributes readerAttributes)
    {
        File file = storageService.getStorageFile(chunkId).getAbsoluteFile();

        if (!file.exists() && backupStore.isPresent()) {
            try {
                Future<?> future = recoveryManager.recoverChunk(chunkId);
                future.get(recoveryTimeout.toMillis(), TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
            }
            catch (ExecutionException e) {
                propagateIfInstanceOf(e.getCause(), PrestoException.class);
                throw new PrestoException(RAPTOR_RECOVERY_ERROR, "Error recovering chunk " + chunkId, e.getCause());
            }
            catch (TimeoutException e) {
                throw new PrestoException(RAPTOR_RECOVERY_TIMEOUT, "Chunk is being recovered from backup. Please retry in a few minutes: " + chunkId);
            }
        }

        try {
            return fileOrcDataSource(readerAttributes, file);
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_ERROR, "Failed to open chunk file: " + file, e);
        }
    }

    private static FileOrcDataSource fileOrcDataSource(ReaderAttributes readerAttributes, File file)
            throws FileNotFoundException
    {
        return new FileOrcDataSource(file, readerAttributes.getMaxMergeDistance(), readerAttributes.getMaxReadSize(), readerAttributes.getStreamBufferSize());
    }

    private ChunkInfo createChunkInfo(long chunkId, int bucketNumber, File file, long rowCount, long uncompressedSize)
    {
        return new ChunkInfo(chunkId, bucketNumber, computeChunkStats(file), rowCount, file.length(), uncompressedSize);
    }

    private List<ColumnStats> computeChunkStats(File file)
    {
        try (OrcDataSource dataSource = fileOrcDataSource(defaultReaderAttributes, file)) {
            OrcReader reader = new OrcReader(dataSource, new OrcMetadataReader(), defaultReaderAttributes.getMaxMergeDistance(), defaultReaderAttributes.getMaxReadSize());

            ImmutableList.Builder<ColumnStats> list = ImmutableList.builder();
            for (ColumnInfo info : getColumnInfo(reader)) {
                computeColumnStats(reader, info.getColumnId(), info.getType()).ifPresent(list::add);
            }
            return list.build();
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_ERROR, "Failed to read file: " + file, e);
        }
    }

    @VisibleForTesting
    Collection<Slice> rewriteChunk(long transactionId, int bucketNumber, long chunkId, BitSet rowsToDelete)
    {
        if (rowsToDelete.isEmpty()) {
            return ImmutableList.of();
        }

        long newChunkId = chunkIdSequence.nextChunkId();
        File input = storageService.getStorageFile(chunkId);
        File output = storageService.getStagingFile(newChunkId);

        OrcFileInfo info = rewriteFile(input, output, rowsToDelete);
        long rowCount = info.getRowCount();

        if (rowCount == 0) {
            return chunkDelta(chunkId, Optional.empty());
        }

        chunkRecorder.recordCreatedChunk(transactionId, newChunkId);

        // submit for backup and wait until it finishes
        getFutureValue(backupManager.submit(newChunkId, output));

        long uncompressedSize = info.getUncompressedSize();

        ChunkInfo chunk = createChunkInfo(newChunkId, bucketNumber, output, rowCount, uncompressedSize);

        writeChunk(newChunkId);

        return chunkDelta(newChunkId, Optional.of(chunk));
    }

    private static Collection<Slice> chunkDelta(long oldChunkId, Optional<ChunkInfo> chunkInfo)
    {
        List<ChunkInfo> newChunks = chunkInfo.map(ImmutableList::of).orElse(ImmutableList.of());
        ChunkDelta delta = new ChunkDelta(ImmutableList.of(oldChunkId), newChunks);
        return ImmutableList.of(Slices.wrappedBuffer(CHUNK_DELTA_CODEC.toJsonBytes(delta)));
    }

    private static OrcFileInfo rewriteFile(File input, File output, BitSet rowsToDelete)
    {
        try {
            return OrcFileRewriter.rewrite(input, output, rowsToDelete);
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_ERROR, "Failed to rewrite chunk file: " + input, e);
        }
    }

    private List<ColumnInfo> getColumnInfo(OrcReader reader)
    {
        Slice slice = reader.getFooter().getUserMetadata().get(OrcFileMetadata.KEY);
        checkArgument(slice != null, "no metadata in file");
        OrcFileMetadata metadata = METADATA_CODEC.fromJson(slice.getBytes());
        return getColumnInfoFromOrcUserMetadata(metadata);
    }

    private List<ColumnInfo> getColumnInfoFromOrcUserMetadata(OrcFileMetadata orcFileMetadata)
    {
        return orcFileMetadata.getColumnTypes().entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey())
                .map(entry -> new ColumnInfo(entry.getKey(), typeManager.getType(entry.getValue())))
                .collect(toList());
    }

    private static OrcPredicate getPredicate(TupleDomain<RaptorColumnHandle> effectivePredicate, Map<Long, Integer> indexMap)
    {
        ImmutableList.Builder<ColumnReference<RaptorColumnHandle>> columns = ImmutableList.builder();
        for (RaptorColumnHandle column : effectivePredicate.getDomains().get().keySet()) {
            Integer index = indexMap.get(column.getColumnId());
            if (index != null) {
                columns.add(new ColumnReference<>(column, index, column.getColumnType()));
            }
        }
        return new TupleDomainOrcPredicate<>(effectivePredicate, columns.build(), false);
    }

    private static Map<Long, Integer> columnIdIndex(List<String> columnNames)
    {
        ImmutableMap.Builder<Long, Integer> map = ImmutableMap.builder();
        for (int i = 0; i < columnNames.size(); i++) {
            map.put(Long.valueOf(columnNames.get(i)), i);
        }
        return map.build();
    }

    private class OrcStoragePageSink
            implements StoragePageSink
    {
        private final long transactionId;
        private final List<Long> columnIds;
        private final List<Type> columnTypes;
        private final int bucketNumber;

        private final List<File> stagingFiles = new ArrayList<>();
        private final List<ChunkInfo> chunks = new ArrayList<>();
        private final List<ListenableFuture<?>> futures = new ArrayList<>();

        private boolean committed;
        private OrcFileWriter writer;
        private long chunkId;

        public OrcStoragePageSink(long transactionId, List<Long> columnIds, List<Type> columnTypes, int bucketNumber)
        {
            this.transactionId = transactionId;
            this.columnIds = ImmutableList.copyOf(requireNonNull(columnIds, "columnIds is null"));
            this.columnTypes = ImmutableList.copyOf(requireNonNull(columnTypes, "columnTypes is null"));
            this.bucketNumber = bucketNumber;
        }

        @Override
        public void appendPages(List<Page> pages)
        {
            createWriterIfNecessary();
            writer.appendPages(pages);
        }

        @Override
        public void appendPages(List<Page> inputPages, int[] pageIndexes, int[] positionIndexes)
        {
            createWriterIfNecessary();
            writer.appendPages(inputPages, pageIndexes, positionIndexes);
        }

        @Override
        public void appendRow(Row row)
        {
            createWriterIfNecessary();
            writer.appendRow(row);
        }

        @Override
        public boolean isFull()
        {
            if (writer == null) {
                return false;
            }
            return (writer.getRowCount() >= maxChunkRows) || (writer.getUncompressedSize() >= maxChunkSize.toBytes());
        }

        @Override
        public void flush()
        {
            if (writer != null) {
                writer.close();

                chunkRecorder.recordCreatedChunk(transactionId, chunkId);

                File stagingFile = storageService.getStagingFile(chunkId);
                futures.add(backupManager.submit(chunkId, stagingFile));

                long rowCount = writer.getRowCount();
                long uncompressedSize = writer.getUncompressedSize();

                chunks.add(createChunkInfo(chunkId, bucketNumber, stagingFile, rowCount, uncompressedSize));

                writer = null;
                chunkId = 0;
            }
        }

        @Override
        public ListenableFuture<List<ChunkInfo>> commit()
        {
            checkState(!committed, "already committed");
            committed = true;

            flush();

            return whenAllComplete(futures).call(() -> {
                for (ChunkInfo chunk : chunks) {
                    writeChunk(chunk.getChunkId());
                }
                return ImmutableList.copyOf(chunks);
            }, commitExecutor);
        }

        @SuppressWarnings("ResultOfMethodCallIgnored")
        @Override
        public void rollback()
        {
            try {
                if (writer != null) {
                    writer.close();
                    writer = null;
                }
            }
            finally {
                for (File file : stagingFiles) {
                    file.delete();
                }

                // cancel incomplete backup jobs
                futures.forEach(future -> future.cancel(true));

                // delete completed backup chunks
                backupStore.ifPresent(backupStore -> {
                    for (ChunkInfo chunk : chunks) {
                        backupStore.deleteChunk(chunk.getChunkId());
                    }
                });
            }
        }

        private void createWriterIfNecessary()
        {
            if (writer == null) {
                chunkId = chunkIdSequence.nextChunkId();
                File stagingFile = storageService.getStagingFile(chunkId);
                storageService.createParents(stagingFile);
                stagingFiles.add(stagingFile);
                writer = new OrcFileWriter(columnIds, columnTypes, stagingFile);
            }
        }
    }

    private static void closeQuietly(Closeable closeable)
    {
        try {
            closeable.close();
        }
        catch (IOException ignored) {
        }
    }
}
