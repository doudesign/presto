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
package com.facebook.presto.server;

import com.facebook.presto.execution.QueryId;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManager;
import com.facebook.presto.operator.ExchangeClient;
import com.facebook.presto.operator.ExchangeClientSupplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import java.net.URI;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

@Path("/v1/parallel")
public class ParallelDataResource
{
    private static final Duration MAX_WAIT_TIME = new Duration(1, SECONDS);
    private static final Ordering<Comparable<Duration>> WAIT_ORDERING = Ordering.natural().nullsLast();
    private static final long DESIRED_RESULT_BYTES = new DataSize(1, MEGABYTE).toBytes();

    private final TaskManager taskManager;
    private final ExchangeClientSupplier exchangeClientSupplier;

    private final ConcurrentMap<TaskId, Output> outputs = new ConcurrentHashMap<>();

    @Inject
    public ParallelDataResource(
            TaskManager taskManager,
            ExchangeClientSupplier exchangeClientSupplier)
    {
        this.taskManager = requireNonNull(taskManager, "taskManager is null");
        this.exchangeClientSupplier = requireNonNull(exchangeClientSupplier, "exchangeClientSupplier is null");
    }

    @GET
    @Path("{queryId}/data/{taskId}/{token}")
    @Produces(APPLICATION_JSON)
    public Response getOutput(
            @PathParam("queryId") QueryId queryId,
            @PathParam("taskId") TaskId taskId,
            @PathParam("token") long token,
            @QueryParam("maxWait") Duration maxWait,
            @Context UriInfo uriInfo)
    {
        TaskInfo taskInfo = taskManager.getTaskInfo()

        TaskId bufferId = Iterables.getOnlyElement(buffers).getBufferId();
        URI uri = uriBuilderFrom(taskInfo.getSelf()).appendPath("results").appendPath(bufferId.toString()).build();
        exchangeClient.addLocation(uri);
    }

    private static class Output
    {
        private final ExchangeClient client;

        private Output(ExchangeClient client)
        {
            this.client = requireNonNull(client, "client is null");
        }
    }
}
