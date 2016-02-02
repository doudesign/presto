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

import com.facebook.presto.Session;
import com.facebook.presto.client.ParallelStatus;
import com.facebook.presto.execution.QueryId;
import com.facebook.presto.execution.QueryIdGenerator;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.buffer.BufferInfo;
import com.facebook.presto.execution.buffer.SharedBufferInfo;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.operator.ExchangeClientSupplier;
import com.facebook.presto.security.AccessControl;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import java.net.URI;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.facebook.presto.SystemSessionProperties.HASH_PARTITION_COUNT;
import static com.facebook.presto.SystemSessionProperties.PARALLEL_OUTPUT;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_NEXT_URI;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PARALLEL_COUNT;
import static com.facebook.presto.server.ResourceUtil.assertRequest;
import static com.facebook.presto.server.ResourceUtil.badRequest;
import static com.facebook.presto.server.ResourceUtil.createSessionForRequest;
import static com.facebook.presto.server.ResourceUtil.trimEmptyToNull;
import static com.facebook.presto.server.StatementResource.Query.findCancelableLeafStage;
import static com.facebook.presto.server.StatementResource.Query.toQueryError;
import static com.facebook.presto.server.StatementResource.Query.toStatementStats;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.Response.Status.GONE;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.NO_CONTENT;

@Path("/v1/parallel")
public class ParallelResource
{
    private static final Duration MAX_WAIT_TIME = new Duration(1, SECONDS);
    private static final Ordering<Comparable<Duration>> WAIT_ORDERING = Ordering.natural().nullsLast();
    private static final long DESIRED_RESULT_BYTES = new DataSize(1, MEGABYTE).toBytes();

    private final QueryManager queryManager;
    private final AccessControl accessControl;
    private final SessionPropertyManager sessionPropertyManager;
    private final ExchangeClientSupplier exchangeClientSupplier;
    private final QueryIdGenerator queryIdGenerator;

    private final ConcurrentMap<QueryId, ParallelQuery> queries = new ConcurrentHashMap<>();

    @Inject
    public ParallelResource(
            QueryManager queryManager,
            AccessControl accessControl,
            SessionPropertyManager sessionPropertyManager,
            ExchangeClientSupplier exchangeClientSupplier,
            QueryIdGenerator queryIdGenerator)
    {
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.exchangeClientSupplier = requireNonNull(exchangeClientSupplier, "exchangeClientSupplier is null");
        this.queryIdGenerator = requireNonNull(queryIdGenerator, "queryIdGenerator is null");
    }

    @POST
    @Produces(APPLICATION_JSON)
    public Response createQuery(
            String statement,
            @Context HttpServletRequest request,
            @Context UriInfo uriInfo)
            throws InterruptedException
    {
        assertRequest(!isNullOrEmpty(statement), "SQL statement is empty");
        int parallelCount = getParallelCount(request);

        QueryId queryId = queryIdGenerator.createNextQueryId();
        Session session = createSessionForRequest(request, accessControl, sessionPropertyManager, queryId)
                .withSystemProperty(PARALLEL_OUTPUT, String.valueOf(true))
                .withSystemProperty(HASH_PARTITION_COUNT, String.valueOf(parallelCount));

        ParallelQuery query = new ParallelQuery(session, statement);

        queries.put(queryId, query);

        ImmutableList.Builder<URI> dataUris = ImmutableList.builder();
        for (int i = 0; i < parallelCount; i++) {
            dataUris.add(uriInfo.getRequestUriBuilder()
                    .path(queryId.toString())
                    .path(String.valueOf(i))
                    .replaceQuery("")
                    .build());
        }

        URI nextUri = uriInfo.getRequestUriBuilder()
                .path(queryId.toString())
                .replaceQuery("")
                .build();

        ParallelStatus status = new ParallelStatus(queryId.toString(), null, null, nextUri, dataUris.build(), null, null);

        return Response.ok(status).build();
    }

    @GET
    @Path("{queryId}")
    @Produces(APPLICATION_JSON)
    public Response getStatus(
            @PathParam("queryId") QueryId queryId,
            @QueryParam("maxWait") Duration maxWait,
            @Context UriInfo uriInfo)
            throws InterruptedException
    {
        Duration wait = WAIT_ORDERING.min(MAX_WAIT_TIME, maxWait);

        ParallelQuery query = queries.get(queryId);
        if (query == null) {
            return Response.status(NOT_FOUND).build();
        }

        Optional<QueryInfo> queryInfo = findQueryInfo(queryId);
        if (queryInfo.isPresent()) {
            queryManager.recordHeartbeat(queryId);
            queryManager.waitForStateChange(queryId, queryInfo.get().getState(), wait);
        }
        else {
            Thread.sleep(wait.toMillis());
        }
        queryInfo = findQueryInfo(queryId);

        URI nextUri = uriInfo.getRequestUriBuilder().replaceQuery("").build();

        if (!queryInfo.isPresent()) {
            ParallelStatus status = new ParallelStatus(queryId.toString(), null, null, nextUri, null, null, null);
            return Response.ok(status).build();
        }

        QueryInfo info = queryInfo.get();
        if (info.getState().isDone()) {
            nextUri = null;
        }

        ParallelStatus status = new ParallelStatus(
                queryId.toString(),
                uriInfo.getRequestUriBuilder()
                        .replacePath(info.getSelf().getPath())
                        .replaceQuery("")
                        .build(),
                findCancelableLeafStage(info),
                nextUri,
                null,
                toStatementStats(info),
                toQueryError(info));

        return Response.ok(status).build();
    }

    @GET
    @Path("{queryId}/{output}")
    @Produces(APPLICATION_JSON)
    public Response getOutputStart(
            @PathParam("queryId") QueryId queryId,
            @PathParam("output") int output,
            @Context UriInfo uriInfo)
            throws InterruptedException
    {
        ParallelQuery query = queries.get(queryId);
        if (query == null) {
            return Response.status(NOT_FOUND).build();
        }

        query.ensureStarted(queryManager);

        QueryInfo info = queryManager.getQueryInfo(queryId);

        if (info.getState().isDone()) {
            // TODO: can this miss data?
            return Response.status(GONE).build();
        }

        URI nextUri = uriInfo.getRequestUriBuilder().replaceQuery("").build();
        Response response = Response.status(NO_CONTENT)
                .header(PRESTO_NEXT_URI, nextUri)
                .build();

        if (info.getOutputStage() == null) {
            return response;
        }

        List<TaskInfo> tasks = info.getOutputStage().getTasks();
        if (tasks.size() <= output) {
            return response;
        }
        TaskInfo taskInfo = tasks.get(output);

        SharedBufferInfo outputBuffers = taskInfo.getOutputBuffers();
        List<BufferInfo> buffers = outputBuffers.getBuffers();
        if (buffers.isEmpty() || outputBuffers.getState().canAddBuffers()) {
            // output buffer has not been created yet
            return response;
        }

        checkState(buffers.size() == 1, "Expected a single output buffer for task %s, but found %s", taskInfo.getTaskId(), buffers);

        nextUri = uriBuilderFrom(taskInfo.getSelf())
                .replacePath("/v1/parallel")
                .appendPath(queryId.toString())
                .appendPath("data")
                .appendPath(getOnlyElement(buffers).getBufferId().toString())
                .appendPath("0")
                .build();

        return Response.status(NO_CONTENT)
                .header(PRESTO_NEXT_URI, nextUri)
                .build();
    }

    private Optional<QueryInfo> findQueryInfo(QueryId queryId)
    {
        try {
            return Optional.of(queryManager.getQueryInfo(queryId));
        }
        catch (NoSuchElementException e) {
            return Optional.empty();
        }
    }

    private static int getParallelCount(HttpServletRequest request)
    {
        String countHeader = trimEmptyToNull(request.getHeader(PRESTO_PARALLEL_COUNT));
        assertRequest(countHeader != null, "%s header must be set");
        try {
            int count = Integer.parseInt(countHeader);
            assertRequest(count > 0, "Parallel count must be > 0");
            return count;
        }
        catch (NumberFormatException e) {
            throw badRequest(PRESTO_PARALLEL_COUNT + " header is invalid");
        }
    }

    private static class ParallelQuery
    {
        private final Session session;
        private final String statement;

        private ParallelQuery(Session session, String statement)
        {
            this.session = requireNonNull(session, "session is null");
            this.statement = requireNonNull(statement, "statement is null");
        }

        public Session getSession()
        {
            return session;
        }

        public String getStatement()
        {
            return statement;
        }

        public synchronized void ensureStarted(QueryManager queryManager)
        {
            if (!queryManager.getQueryState(session.getQueryId()).isPresent()) {
                queryManager.createQuery(session, statement);
            }
        }
    }
}
