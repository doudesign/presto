package com.facebook.presto.raptorx.metadata;

import com.facebook.presto.raptorx.util.Database;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ExecutionError;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.tweak.ConnectionFactory;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.facebook.presto.raptorx.util.DatabaseUtil.runIgnoringConstraintViolation;
import static com.facebook.presto.raptorx.util.DatabaseUtil.utf8String;
import static com.google.common.base.Strings.repeat;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Sets.difference;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

public class NodeIdCache
{
    private final ConnectionFactory master;
    private final NodeDao dao;

    private final LoadingCache<String, Long> cache = CacheBuilder.newBuilder()
            .maximumSize(10_000)
            .build(new CacheLoader<String, Long>()
            {
                @Override
                public Long load(String identifier)
                {
                    return loadAll(ImmutableSet.of(identifier)).get(identifier);
                }

                @Override
                public Map<String, Long> loadAll(Iterable<? extends String> identifiers)
                {
                    return loadNodeIds(ImmutableSet.copyOf(identifiers));
                }
            });

    @Inject
    public NodeIdCache(Database database)
    {
        this.master = database.getMasterConnection();
        this.dao = createNodeDao(database);
    }

    public ImmutableMap<String, Long> getNodeIds(Set<String> identifiers)
    {
        try {
            return cache.getAll(identifiers);
        }
        catch (ExecutionException | UncheckedExecutionException | ExecutionError e) {
            throwIfUnchecked(e.getCause());
            throw new RuntimeException(e.getCause());
        }
    }

    private Map<String, Long> loadNodeIds(Set<String> identifiers)
    {
        Map<String, Long> map = new HashMap<>();
        fetchNodeIdsInto(identifiers, map);

        for (String identifier : difference(identifiers, map.keySet()).immutableCopy()) {
            dao.insertNode(identifier.getBytes(UTF_8));

            Long id = dao.getNodeId(identifier.getBytes(UTF_8));
            verify(id != null, "node does not exist after insert");
            map.put(identifier, id);
        }

        return map;
    }

    private void fetchNodeIdsInto(Set<String> identifiers, Map<String, Long> map)
    {
        String sql = format(
                "SELECT node_id, identifier FROM nodes WHERE identifier IN (%s)",
                repeat("?", identifiers.size()));

        try (Connection connection = master.openConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            int index = 1;
            for (String identifier : identifiers) {
                statement.setBytes(index, identifier.getBytes(UTF_8));
                index++;
            }

            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    map.put(utf8String(rs.getBytes("identifier")), rs.getLong("node_id"));
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static NodeDao createNodeDao(Database database)
    {
        IDBI dbi = new DBI(database.getMasterConnection());
        switch (database.getType()) {
            case H2:
                return dbi.onDemand(H2NodeDao.class);
            case MYSQL:
                return dbi.onDemand(MySqlNodeDao.class);
        }
        throw new AssertionError("Unhandled database: " + database.getType());
    }

    private interface NodeDao
    {
        void insertNode(byte[] identifier);

        @SqlQuery("SELECT node_id FROM nodes WHERE identifier = :identifier")
        Long getNodeId(@Bind("identifier") byte[] identifier);
    }

    private interface MySqlNodeDao
            extends NodeDao
    {
        @Override
        @SqlUpdate("INSERT IGNORE INTO nodes (identifier) VALUES (:identifier)")
        void insertNode(@Bind("identifier") byte[] identifier);
    }

    private interface H2NodeDao
            extends NodeDao
    {
        @SqlUpdate("INSERT INTO nodes (identifier) VALUES (:identifier)")
        void doInsertNode(@Bind("identifier") byte[] identifier);

        @Override
        default void insertNode(byte[] identifier)
        {
            runIgnoringConstraintViolation(() -> doInsertNode(identifier));
        }
    }
}
