package com.facebook.presto.raptor.locks;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class SchemaNameLockResource
        extends LockResource
{
    private final String schemaName;

    public SchemaNameLockResource(String schemaName)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        SchemaNameLockResource that = (SchemaNameLockResource) obj;
        return Objects.equals(schemaName, that.schemaName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName);
    }
}
