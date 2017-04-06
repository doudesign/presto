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
package com.facebook.presto.raptorx.util;

import com.facebook.presto.spi.PrestoException;
import com.google.common.base.Throwables;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.exceptions.DBIException;

import java.lang.reflect.InvocationTargetException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.function.Consumer;

import static com.facebook.presto.raptorx.RaptorErrorCode.RAPTOR_METADATA_ERROR;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.reflect.Reflection.newProxy;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.sql.Types.INTEGER;
import static java.util.Objects.requireNonNull;

public final class DatabaseUtil
{
    private DatabaseUtil() {}

    public static <T> T onDemandDao(IDBI dbi, Class<T> daoType)
    {
        requireNonNull(dbi, "dbi is null");
        return newProxy(daoType, (proxy, method, args) -> {
            try (Handle handle = dbi.open()) {
                T dao = handle.attach(daoType);
                return method.invoke(dao, args);
            }
            catch (DBIException e) {
                throw metadataError(e);
            }
            catch (InvocationTargetException e) {
                throw metadataError(e.getCause());
            }
        });
    }

    public static <T> T runTransaction(IDBI dbi, TransactionCallback<T> callback)
    {
        try {
            return dbi.inTransaction(callback);
        }
        catch (DBIException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throw metadataError(e);
        }
    }

    public static <T> void daoTransaction(IDBI dbi, Class<T> daoType, Consumer<T> callback)
    {
        runTransaction(dbi, (handle, status) -> {
            callback.accept(handle.attach(daoType));
            return null;
        });
    }

    public static PrestoException metadataError(Throwable cause)
    {
        return new PrestoException(RAPTOR_METADATA_ERROR, "Failed to perform metadata operation", cause);
    }

    /**
     * Run a SQL query as ignoring any constraint violations.
     * This allows idempotent inserts (equivalent to INSERT IGNORE).
     */
    public static void runIgnoringConstraintViolation(Runnable task)
    {
        try {
            task.run();
        }
        catch (RuntimeException e) {
            if (!sqlCodeStartsWith(e, "23")) {
                throw e;
            }
        }
    }

    public static void enableStreamingResults(Statement statement)
            throws SQLException
    {
        if (statement.isWrapperFor(com.mysql.jdbc.Statement.class)) {
            statement.unwrap(com.mysql.jdbc.Statement.class).enableStreamingResults();
        }
    }

    public static OptionalInt getOptionalInt(ResultSet rs, String name)
            throws SQLException
    {
        int value = rs.getInt(name);
        return rs.wasNull() ? OptionalInt.empty() : OptionalInt.of(value);
    }

    public static OptionalLong getOptionalLong(ResultSet rs, String name)
            throws SQLException
    {
        long value = rs.getLong(name);
        return rs.wasNull() ? OptionalLong.empty() : OptionalLong.of(value);
    }

    public static void bindOptionalInt(PreparedStatement statement, int index, OptionalInt value)
            throws SQLException
    {
        if (value.isPresent()) {
            statement.setInt(index, value.getAsInt());
        }
        else {
            statement.setNull(index, INTEGER);
        }
    }

    public static boolean isSyntaxOrAccessError(Exception e)
    {
        return sqlCodeStartsWith(e, "42");
    }

    private static boolean sqlCodeStartsWith(Exception e, String code)
    {
        for (Throwable throwable : Throwables.getCausalChain(e)) {
            if (throwable instanceof SQLException) {
                String state = ((SQLException) throwable).getSQLState();
                if (state != null && state.startsWith(code)) {
                    return true;
                }
            }
        }
        return false;
    }

    public static String utf8String(byte[] bytes)
    {
        return (bytes == null) ? null : new String(bytes, UTF_8);
    }
}
