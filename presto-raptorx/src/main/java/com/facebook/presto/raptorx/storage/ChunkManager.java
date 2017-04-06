package com.facebook.presto.raptorx.storage;

import com.facebook.presto.raptorx.metadata.ChunkMetadata;

import java.util.Set;

public interface ChunkManager
{
    default Set<ChunkMetadata> getNodeChunks(String nodeIdentifier)
    {
        throw new UnsupportedOperationException();
    }
}
