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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.procedure.Procedure;
import com.facebook.presto.spi.procedure.Procedure.Argument;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.inject.Provider;

import java.util.List;
import java.util.Set;

import static com.facebook.presto.raptorx.util.Reflection.getMethod;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.StandardTypes.BIGINT;
import static com.facebook.presto.spi.type.StandardTypes.INTEGER;
import static com.facebook.presto.spi.type.StandardTypes.VARCHAR;

public class CreateDistributionProcedure
        implements Provider<Procedure>
{
    public static final int MAX_BUCKETS = 100_000;
    private static final Set<String> SUPPORTED_TYPES = ImmutableSet.of(INTEGER, BIGINT, VARCHAR);

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "create_distribution",
                ImmutableList.<Argument>builder()
                        .add(new Argument("name", VARCHAR))
                        .add(new Argument("count", BIGINT))
                        .add(new Argument("types", "array(varchar)"))
                        .build(),
                getMethod(getClass(), "createDistribution"));
    }

    public static void createDistribution(String name, long count, List<String> types)
    {
        validate(!name.isEmpty(), "Distribution name is empty");
        validate(count > 0, "Bucket count must be greater than zero");
        validate(count <= MAX_BUCKETS, "Bucket count must be no more than " + MAX_BUCKETS);
        validate(!types.isEmpty(), "At least one column type must be specified");
        for (String type : types) {
            validate(SUPPORTED_TYPES.contains(type), "Column type not supported for bucketing: " + type);
        }

        // TODO
        throw new PrestoException(NOT_SUPPORTED, "Not yet implemented");
    }

    private static void validate(boolean condition, String message)
    {
        if (!condition) {
            throw new PrestoException(INVALID_PROCEDURE_ARGUMENT, message);
        }
    }
}
