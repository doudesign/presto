package com.facebook.presto.raptor.locks;

import com.facebook.presto.spi.PrestoException;

import static com.facebook.presto.spi.StandardErrorCode.LOCK_TIMEOUT;

public class LockTimeoutException
        extends PrestoException
{
    public LockTimeoutException()
    {
        super(LOCK_TIMEOUT, "Timeout exceeded while waiting for lock");
    }
}
