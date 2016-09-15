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
package com.facebook.presto.server.testing;

import com.facebook.presto.plugin.blackhole.BlackHolePlugin;
import com.facebook.presto.tpch.TpchPlugin;
import io.airlift.log.Logger;

import java.io.Closeable;
import java.net.URI;

public final class StandaloneTestingPrestoServer
        implements Closeable
{
    private static final Logger log = Logger.get(StandaloneTestingPrestoServer.class);

    private final TestingPrestoServer server;

    public StandaloneTestingPrestoServer()
            throws Exception
    {
        log.info("Starting Presto server...");
        server = new TestingPrestoServer();

        try {
            server.installPlugin(new TpchPlugin());
            server.createCatalog("tpch", "tpch");

            server.installPlugin(new BlackHolePlugin());
            server.createCatalog("blackhole", "blackhole");

            server.refreshNodes();
        }
        catch (RuntimeException e) {
            server.close();
            throw e;
        }

        log.info("Presto server ready: %s", getJdbcUrl());
    }

    @Override
    public void close()
    {
        log.info("Stopping Presto server...");
        server.close();
        log.info("Presto server stopped.");
    }

    public URI getBaseUri()
    {
        return server.getBaseUrl();
    }

    public String getJdbcUrl()
    {
        return "jdbc:presto://" + server.getAddress();
    }
}
