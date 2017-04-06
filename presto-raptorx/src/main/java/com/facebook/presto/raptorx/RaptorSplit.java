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

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class RaptorSplit
        implements ConnectorSplit
{
    private final int bucketNumber;
    private final Set<Long> chunkIds;
    private final TupleDomain<RaptorColumnHandle> predicate;
    private final HostAddress address;

    @JsonCreator
    public RaptorSplit(
            @JsonProperty("bucketNumber") int bucketNumber,
            @JsonProperty("chunkIds") Set<Long> chunkIds,
            @JsonProperty("predicate") TupleDomain<RaptorColumnHandle> predicate,
            @JsonProperty("address") HostAddress address
    )
    {
        this.bucketNumber = bucketNumber;
        this.chunkIds = ImmutableSet.copyOf(requireNonNull(chunkIds, "chunkIds is null"));
        this.predicate = requireNonNull(predicate, "predicate is null");
        this.address = requireNonNull(address, "address is null");
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return false;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of(address);
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @JsonProperty
    public int getBucketNumber()
    {
        return bucketNumber;
    }

    @JsonProperty
    public Set<Long> getChunkIds()
    {
        return chunkIds;
    }

    @JsonProperty
    public TupleDomain<RaptorColumnHandle> getPredicate()
    {
        return predicate;
    }

    @JsonProperty
    public HostAddress getAddress()
    {
        return address;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("bucketNumber", bucketNumber)
                .add("chunkIds", chunkIds)
                .toString();
    }
}
