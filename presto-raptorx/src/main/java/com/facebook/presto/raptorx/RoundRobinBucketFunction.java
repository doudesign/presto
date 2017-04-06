package com.facebook.presto.raptorx;

import com.facebook.presto.spi.BucketFunction;
import com.facebook.presto.spi.Page;

import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;

public class RoundRobinBucketFunction
        implements BucketFunction
{
    private final int bucketCount;
    private final AtomicInteger counter = new AtomicInteger();

    public RoundRobinBucketFunction(int bucketCount)
    {
        checkArgument(bucketCount > 0, "bucket count must be at least one");
        this.bucketCount = bucketCount;
    }

    @Override
    public int getBucket(Page page, int position)
    {
        return counter.getAndUpdate(n -> (n + 1) & Integer.MAX_VALUE) % bucketCount;
    }
}
