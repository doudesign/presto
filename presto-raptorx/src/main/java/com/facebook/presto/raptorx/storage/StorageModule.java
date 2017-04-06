package com.facebook.presto.raptorx.storage;

import com.facebook.presto.raptorx.metadata.SequenceManager;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;

import javax.inject.Singleton;

import static com.google.common.reflect.Reflection.newProxy;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.lang.String.format;

public class StorageModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(StorageManagerConfig.class);

        binder.bind(StorageManager.class).to(OrcStorageManager.class).in(Scopes.SINGLETON);
        binder.bind(StorageService.class).to(FileStorageService.class).in(Scopes.SINGLETON);
        binder.bind(ReaderAttributes.class).in(Scopes.SINGLETON);
        binder.bind(ChunkRecoveryManager.class).in(Scopes.SINGLETON);
        binder.bind(ChunkManager.class).toInstance(newProxy(ChunkManager.class, (proxy, method, args) -> {
            throw new UnsupportedOperationException(method.getName());
        }));
        binder.bind(ChunkRecorder.class).toInstance((transactionId, chunkId) -> System.err.println(format("RECORD CHUNK: tx=%s chunk=%s", transactionId, chunkId)));
    }

    @Provides
    @Singleton
    public ChunkIdSequence createChunkIdSequence(SequenceManager sequenceManager)
    {
        return () -> sequenceManager.nextValue("chunk_id", 100);
    }
}
