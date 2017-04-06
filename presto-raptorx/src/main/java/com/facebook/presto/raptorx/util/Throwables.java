package com.facebook.presto.raptorx.util;

public final class Throwables
{
    private Throwables() {}

    @SuppressWarnings("ObjectEquality")
    public static <T extends Throwable> T addSuppressed(T throwable, Throwable suppressed)
    {
        // Self-suppression not permitted
        if (suppressed != throwable) {
            throwable.addSuppressed(suppressed);
        }
        return throwable;
    }
}
