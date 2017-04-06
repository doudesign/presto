package com.facebook.presto.raptorx.util;

import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.succinctDataSize;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class DataRate
{
    private DataRate() {}

    public static DataSize dataRate(DataSize size, Duration duration)
    {
        double rate = size.toBytes() / duration.getValue(SECONDS);
        if (!Double.isFinite(rate)) {
            rate = 0;
        }
        return succinctDataSize(rate, BYTE);
    }
}
