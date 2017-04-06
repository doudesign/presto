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

import com.facebook.presto.raptorx.metadata.DatabaseMetadata;
import com.facebook.presto.raptorx.metadata.DatabaseMetadataWriter;
import com.facebook.presto.raptorx.metadata.Metadata;
import com.facebook.presto.raptorx.metadata.MetadataWriter;
import com.facebook.presto.raptorx.metadata.NodeIdCache;
import com.facebook.presto.raptorx.metadata.NodeSupplier;
import com.facebook.presto.raptorx.metadata.RaptorNodeSupplier;
import com.facebook.presto.raptorx.metadata.SchemaCreator;
import com.facebook.presto.raptorx.metadata.SequenceManager;
import com.facebook.presto.raptorx.transaction.TransactionWriter;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.procedure.Procedure;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;

import static com.google.inject.multibindings.Multibinder.newSetBinder;

public class RaptorModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(Connector.class).to(RaptorConnector.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorSplitManager.class).to(RaptorSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSourceProvider.class).to(RaptorPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSinkProvider.class).to(RaptorPageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorNodePartitioningProvider.class).to(RaptorNodePartitioningProvider.class).in(Scopes.SINGLETON);

        binder.bind(Metadata.class).to(DatabaseMetadata.class).in(Scopes.SINGLETON);
        binder.bind(MetadataWriter.class).to(DatabaseMetadataWriter.class).in(Scopes.SINGLETON);
        binder.bind(SequenceManager.class).in(Scopes.SINGLETON);
        binder.bind(SchemaCreator.class).asEagerSingleton();

        binder.bind(TransactionWriter.class).in(Scopes.SINGLETON);

        binder.bind(NodeIdCache.class).in(Scopes.SINGLETON);
        binder.bind(NodeSupplier.class).to(RaptorNodeSupplier.class).in(Scopes.SINGLETON);


        Multibinder<Procedure> procedureBinder = newSetBinder(binder, Procedure.class);
        procedureBinder.addBinding().toProvider(CreateDistributionProcedure.class).in(Scopes.SINGLETON);
    }
}
