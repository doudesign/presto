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
package com.facebook.presto.raptorx;

import com.facebook.presto.raptorx.metadata.Metadata;
import com.facebook.presto.raptorx.metadata.NodeSupplier;
import com.facebook.presto.raptorx.transaction.RaptorTransaction;
import com.facebook.presto.raptorx.transaction.TransactionWriter;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.procedure.Procedure;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.google.common.collect.ImmutableSet;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.facebook.presto.spi.transaction.IsolationLevel.REPEATABLE_READ;
import static com.facebook.presto.spi.transaction.IsolationLevel.checkConnectorSupports;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class RaptorConnector
        implements Connector
{
    private static final Logger log = Logger.get(RaptorConnector.class);

    private final LifeCycleManager lifeCycleManager;
    private final NodeSupplier nodeSupplier;
    private final Metadata metadata;
    private final TransactionWriter transactionWriter;
    private final ConnectorSplitManager splitManager;
    private final ConnectorPageSourceProvider pageSourceProvider;
    private final ConnectorPageSinkProvider pageSinkProvider;
    private final ConnectorNodePartitioningProvider nodePartitioningProvider;
    private final Set<Procedure> procedures;

    private final Map<ConnectorTransactionHandle, RaptorTransaction> transactions = new ConcurrentHashMap<>();

    @Inject
    public RaptorConnector(
            LifeCycleManager lifeCycleManager,
            NodeSupplier nodeSupplier,
            Metadata metadata,
            TransactionWriter transactionWriter,
            ConnectorSplitManager splitManager,
            ConnectorPageSourceProvider pageSourceProvider,
            ConnectorPageSinkProvider pageSinkProvider,
            ConnectorNodePartitioningProvider nodePartitioningProvider,
            Set<Procedure> procedures)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.nodeSupplier = requireNonNull(nodeSupplier, "nodeSupplier is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.transactionWriter = requireNonNull(transactionWriter, "transactionWriter is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.pageSinkProvider = requireNonNull(pageSinkProvider, "pageSinkProvider is null");
        this.nodePartitioningProvider = requireNonNull(nodePartitioningProvider, "nodePartitioningProvider is null");
        this.procedures = ImmutableSet.copyOf(requireNonNull(procedures, "procedures is null"));
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        checkConnectorSupports(REPEATABLE_READ, isolationLevel);

        long transactionId = metadata.nextTransactionId();
        long commitId = metadata.getCurrentCommitId();
        RaptorTransaction transaction = new RaptorTransaction(metadata, transactionId, commitId);

        RaptorTransactionHandle handle = transaction.getHandle();
        transactions.put(handle, transaction);
        return handle;
    }

    @Override
    public void commit(ConnectorTransactionHandle transactionHandle)
    {
        RaptorTransaction transaction = transactions.remove(transactionHandle);
        checkArgument(transaction != null, "no such transaction: %s", transactionHandle);
        transactionWriter.write(transaction.getActions());
    }

    @SuppressWarnings("RedundantMethodOverride")
    @Override
    public void rollback(ConnectorTransactionHandle transactionHandle)
    {
        RaptorTransaction transaction = transactions.remove(transactionHandle);
        checkArgument(transaction != null, "no such transaction: %s", transactionHandle);
        // nothing else is currently required for rollback
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        RaptorTransaction transaction = transactions.get(transactionHandle);
        checkArgument(transaction != null, "no such transaction: %s", transactionHandle);
        return new RaptorMetadata(nodeSupplier, transaction);
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return pageSinkProvider;
    }

    @Override
    public ConnectorNodePartitioningProvider getNodePartitioningProvider()
    {
        return nodePartitioningProvider;
    }

    @Override
    public Set<Procedure> getProcedures()
    {
        return procedures;
    }

    @Override
    public final void shutdown()
    {
        try {
            lifeCycleManager.stop();
        }
        catch (Exception e) {
            log.error(e, "Error shutting down connector");
        }
    }
}
