package com.facebook.presto.raptorx;

import com.facebook.presto.raptorx.storage.StorageManager;
import com.facebook.presto.raptorx.storage.StorageManagerConfig;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PageSorter;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.type.Type;
import io.airlift.units.DataSize;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.spi.block.SortOrder.ASC_NULLS_FIRST;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class RaptorPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final StorageManager storageManager;
    private final PageSorter pageSorter;
    private final DataSize maxBufferSize;

    @Inject
    public RaptorPageSinkProvider(StorageManager storageManager, PageSorter pageSorter, StorageManagerConfig config)
    {
        this.storageManager = requireNonNull(storageManager, "storageManager is null");
        this.pageSorter = requireNonNull(pageSorter, "pageSorter is null");
        this.maxBufferSize = config.getMaxBufferSize();
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle)
    {
        RaptorOutputTableHandle handle = (RaptorOutputTableHandle) outputTableHandle;
        return new RaptorPageSink(
                pageSorter,
                storageManager,
                handle.getTransactionId(),
                toColumnIds(handle.getColumnHandles()),
                toColumnTypes(handle.getColumnHandles()),
                toColumnIds(handle.getSortColumnHandles()),
                nCopies(handle.getSortColumnHandles().size(), ASC_NULLS_FIRST),
                handle.getBucketCount(),
                toColumnIds(handle.getBucketColumnHandles()),
                handle.getTemporalColumnHandle(),
                maxBufferSize);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle)
    {
        throw new UnsupportedOperationException();
    }

    private static List<Long> toColumnIds(List<RaptorColumnHandle> columns)
    {
        return columns.stream()
                .map(RaptorColumnHandle::getColumnId)
                .collect(toList());
    }

    private static List<Type> toColumnTypes(List<RaptorColumnHandle> columns)
    {
        return columns.stream()
                .map(RaptorColumnHandle::getColumnType)
                .collect(toList());
    }
}
