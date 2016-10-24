package com.facebook.presto.raptor.locks;

@SuppressWarnings("ClassMayBeInterface")
abstract class LockResource
{
    @Override
    public abstract boolean equals(Object obj);

    @Override
    public abstract int hashCode();
}
