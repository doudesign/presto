package com.facebook.presto.raptorx.transaction;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class CreateDistributionAction
        implements Action
{
    private final long distributionId;
    private final int bucketCount;
    private final List<Long> nodes;

    public CreateDistributionAction(long distributionId, int bucketCount, List<Long> nodes)
    {

        this.distributionId = requireNonNull(distributionId, "distributionId is null");
        this.bucketCount = requireNonNull(bucketCount, "bucketCount is null");
        this.nodes = ImmutableList.copyOf(requireNonNull(nodes, "nodes is null"));
    }

    public long getDistributionId()
    {
        return distributionId;
    }

    public int getBucketCount()
    {
        return bucketCount;
    }

    public List<Long> getNodes()
    {
        return nodes;
    }

    @Override
    public void accept(ActionVisitor visitor)
    {
        visitor.visitCreateDistribution(this);
    }
}
