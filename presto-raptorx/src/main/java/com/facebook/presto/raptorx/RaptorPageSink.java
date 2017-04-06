package com.facebook.presto.raptorx;

import com.facebook.presto.raptorx.storage.ChunkInfo;
import com.facebook.presto.raptorx.storage.PageBuffer;
import com.facebook.presto.raptorx.storage.StorageManager;
import com.facebook.presto.spi.BucketFunction;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.PageSorter;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.raptorx.RaptorNodePartitioningProvider.getBucketFunction;
import static com.facebook.presto.raptorx.util.Throwables.addSuppressed;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.allAsList;
import static com.google.common.util.concurrent.Futures.transform;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

public class RaptorPageSink
        implements ConnectorPageSink
{
    private static final JsonCodec<ChunkInfo> CHUNK_INFO_CODEC = jsonCodec(ChunkInfo.class);

    private final long transactionId;
    private final StorageManager storageManager;
    private final PageSorter pageSorter;
    private final List<Long> columnIds;
    private final List<Type> columnTypes;
    private final List<Integer> sortFields;
    private final List<SortOrder> sortOrders;
    private final int bucketCount;
    private final int[] bucketFields;
    private final long maxBufferBytes;
    private final OptionalInt temporalColumnIndex;
    private final Optional<Type> temporalColumnType;

    private final PageWriter pageWriter;

    public RaptorPageSink(
            PageSorter pageSorter,
            StorageManager storageManager,
            long transactionId,
            List<Long> columnIds,
            List<Type> columnTypes,
            List<Long> sortColumnIds,
            List<SortOrder> sortOrders,
            int bucketCount,
            List<Long> bucketColumnIds,
            Optional<RaptorColumnHandle> temporalColumnHandle,
            DataSize maxBufferSize)
    {
        this.transactionId = transactionId;
        this.pageSorter = requireNonNull(pageSorter, "pageSorter is null");
        this.columnIds = ImmutableList.copyOf(requireNonNull(columnIds, "columnIds is null"));
        this.columnTypes = ImmutableList.copyOf(requireNonNull(columnTypes, "columnTypes is null"));
        this.storageManager = requireNonNull(storageManager, "storageManager is null");
        this.maxBufferBytes = requireNonNull(maxBufferSize, "maxBufferSize is null").toBytes();

        this.sortFields = ImmutableList.copyOf(sortColumnIds.stream().map(columnIds::indexOf).collect(toList()));
        this.sortOrders = ImmutableList.copyOf(requireNonNull(sortOrders, "sortOrders is null"));

        this.bucketCount = bucketCount;
        this.bucketFields = bucketColumnIds.stream().mapToInt(columnIds::indexOf).toArray();

        if (temporalColumnHandle.isPresent() && columnIds.contains(temporalColumnHandle.get().getColumnId())) {
            temporalColumnIndex = OptionalInt.of(columnIds.indexOf(temporalColumnHandle.get().getColumnId()));
            temporalColumnType = Optional.of(columnTypes.get(temporalColumnIndex.getAsInt()));
            checkArgument(temporalColumnType.get().equals(DATE) || temporalColumnType.get().equals(TIMESTAMP),
                    "temporalColumnType can only be DATE or TIMESTAMP");
        }
        else {
            temporalColumnIndex = OptionalInt.empty();
            temporalColumnType = Optional.empty();
        }

        pageWriter = new PageWriter();
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        if (page.getPositionCount() == 0) {
            return NOT_BLOCKED;
        }

        pageWriter.appendPage(page);
        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return toCompletableFuture(transform(allAsList(commitBuffers()), RaptorPageSink::flatten));
    }

    private List<ListenableFuture<List<Slice>>> commitBuffers()
    {
        return pageWriter.getPageBuffers().stream()
                .map(RaptorPageSink::commitBuffer)
                .map(future -> transform(future, RaptorPageSink::serializeChunks))
                .collect(toList());
    }

    private static ListenableFuture<List<ChunkInfo>> commitBuffer(PageBuffer buffer)
    {
        buffer.flush();
        return buffer.getStoragePageSink().commit();
    }

    private static List<Slice> serializeChunks(List<ChunkInfo> chunks)
    {
        return chunks.stream()
                .map(CHUNK_INFO_CODEC::toJsonBytes)
                .map(Slices::wrappedBuffer)
                .collect(toList());
    }

    private static <T> List<T> flatten(List<List<T>> list)
    {
        return list.stream().flatMap(List::stream).collect(toList());
    }

    @Override
    public void abort()
    {
        RuntimeException error = new RuntimeException("Exception during rollback");
        for (PageBuffer pageBuffer : pageWriter.getPageBuffers()) {
            try {
                pageBuffer.getStoragePageSink().rollback();
            }
            catch (Throwable t) {
                addSuppressed(error, t);
            }
        }
        if (error.getSuppressed().length > 0) {
            throw error;
        }
    }

    private PageBuffer createPageBuffer(int bucketNumber)
    {
        return new PageBuffer(
                maxBufferBytes,
                storageManager.createStoragePageSink(transactionId, bucketNumber, columnIds, columnTypes, true),
                columnTypes,
                sortFields,
                sortOrders,
                pageSorter);
    }

    private class PageWriter
    {
        private final BucketFunction bucketFunction;
        private final Optional<TemporalFunction> temporalFunction;
        private final Long2ObjectMap<PageStore> pageStores = new Long2ObjectOpenHashMap<>();

        public PageWriter()
        {
            checkArgument(temporalColumnIndex.isPresent() == temporalColumnType.isPresent(),
                    "temporalColumnIndex and temporalColumnType must be both present or absent");

            List<Type> bucketTypes = Arrays.stream(bucketFields)
                    .mapToObj(columnTypes::get)
                    .collect(toList());

            this.bucketFunction = getBucketFunction(bucketTypes, bucketCount);
            this.temporalFunction = temporalColumnType.map(TemporalFunction::create);
        }

        public void appendPage(Page page)
        {
            Block temporalBlock = temporalColumnIndex.isPresent() ? page.getBlock(temporalColumnIndex.getAsInt()) : null;

            Page bucketArgs = getBucketArgsPage(page);

            for (int position = 0; position < page.getPositionCount(); position++) {
                int bucket = bucketFunction.getBucket(bucketArgs, position);
                int day = temporalFunction.isPresent() ? temporalFunction.get().getDay(temporalBlock, position) : 0;

                long partition = (((long) bucket) << 32) | (day & 0xFFFF_FFFFL);
                PageStore store = pageStores.get(partition);
                if (store == null) {
                    PageBuffer buffer = createPageBuffer(bucket);
                    store = new PageStore(buffer, columnTypes);
                    pageStores.put(partition, store);
                }

                store.appendPosition(page, position);
            }

            flushIfNecessary();
        }

        private Page getBucketArgsPage(Page page)
        {
            Block[] blocks = new Block[bucketFields.length];
            for (int i = 0; i < bucketFields.length; i++) {
                blocks[i] = page.getBlock(bucketFields[i]);
            }
            return new Page(page.getPositionCount(), blocks);
        }

        public List<PageBuffer> getPageBuffers()
        {
            ImmutableList.Builder<PageBuffer> list = ImmutableList.builder();
            for (PageStore store : pageStores.values()) {
                store.flushToPageBuffer();
                store.getPageBuffer().flush();
                list.add(store.getPageBuffer());
            }
            return list.build();
        }

        private void flushIfNecessary()
        {
            long totalBytes = 0;
            long maxBytes = 0;
            PageBuffer maxBuffer = null;

            for (PageStore store : pageStores.values()) {
                long bytes = store.getUsedMemoryBytes();
                totalBytes += bytes;

                if ((maxBuffer == null) || (bytes > maxBytes)) {
                    maxBuffer = store.getPageBuffer();
                    maxBytes = bytes;
                }
            }

            if ((totalBytes > maxBufferBytes) && (maxBuffer != null)) {
                maxBuffer.flush();
            }
        }
    }

    private static class PageStore
    {
        private final PageBuffer pageBuffer;
        private final PageBuilder pageBuilder;

        public PageStore(PageBuffer pageBuffer, List<Type> columnTypes)
        {
            this.pageBuffer = requireNonNull(pageBuffer, "pageBuffer is null");
            this.pageBuilder = new PageBuilder(columnTypes);
        }

        public long getUsedMemoryBytes()
        {
            return pageBuilder.getSizeInBytes() + pageBuffer.getUsedMemoryBytes();
        }

        public PageBuffer getPageBuffer()
        {
            return pageBuffer;
        }

        public void appendPosition(Page page, int position)
        {
            pageBuilder.declarePosition();
            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                Block block = page.getBlock(channel);
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(channel);
                pageBuilder.getType(channel).appendTo(block, position, blockBuilder);
            }

            if (pageBuilder.isFull()) {
                flushToPageBuffer();
            }
        }

        public void flushToPageBuffer()
        {
            if (!pageBuilder.isEmpty()) {
                pageBuffer.add(pageBuilder.build());
                pageBuilder.reset();
            }
        }
    }

    private interface TemporalFunction
    {
        int getDay(Block temporalBlock, int position);

        static TemporalFunction create(Type temporalColumnType)
        {
            if (temporalColumnType.equals(DATE)) {
                return (temporalBlock, position) -> toIntExact(DATE.getLong(temporalBlock, position));
            }

            if (temporalColumnType.equals(TIMESTAMP)) {
                return (temporalBlock, position) -> toIntExact(MILLISECONDS.toDays(temporalBlock.getLong(position, 0)));
            }

            throw new IllegalArgumentException("Wrong type for temporal column: " + temporalColumnType);
        }
    }
}
