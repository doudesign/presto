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
package com.facebook.presto.raptorx.metadata;

import com.facebook.presto.raptorx.util.Database;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import io.airlift.dbpool.H2EmbeddedDataSourceModule;
import org.skife.jdbi.v2.tweak.ConnectionFactory;

import javax.inject.Singleton;
import javax.sql.DataSource;

import java.util.List;

public class DatabaseModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.install(new H2EmbeddedDataSourceModule("metadata", ForMetadata.class));
    }

    @Provides
    @Singleton
    public Database createDatabase(@ForMetadata DataSource dataSource)
    {
        return new Database()
        {
            @Override
            public Type getType()
            {
                return Type.H2;
            }

            @Override
            public ConnectionFactory getMasterConnection()
            {
                return dataSource::getConnection;
            }

            @Override
            public List<Shard> getShards()
            {
                return ImmutableList.of(new Shard()
                {
                    @Override
                    public String getName()
                    {
                        return "db";
                    }

                    @Override
                    public ConnectionFactory getConnection()
                    {
                        return dataSource::getConnection;
                    }
                });
            }
        };
    }
}
