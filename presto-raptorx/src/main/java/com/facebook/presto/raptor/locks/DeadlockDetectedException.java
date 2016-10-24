package com.facebook.presto.raptor.locks;

import com.facebook.presto.spi.PrestoException;

import static com.facebook.presto.spi.StandardErrorCode.DEADLOCK_DETECTED;

public class DeadlockDetectedException
        extends PrestoException
{
    public DeadlockDetectedException()
    {
        super(DEADLOCK_DETECTED, "Deadlock detected while attempting to acquire lock");
    }
}
