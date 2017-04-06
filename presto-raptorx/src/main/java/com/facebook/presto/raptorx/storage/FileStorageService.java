/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.raptorx.storage;

import com.facebook.presto.spi.PrestoException;
import com.google.common.base.CharMatcher;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.airlift.slice.XxHash64;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static com.facebook.presto.raptorx.RaptorErrorCode.RAPTOR_ERROR;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class FileStorageService
        implements StorageService
{
    private static final Logger log = Logger.get(FileStorageService.class);

    private static final Pattern HEX_DIRECTORY = Pattern.compile("[0-9a-f]{2}");
    private static final CharMatcher DIGITS = CharMatcher.singleWidth();

    private final File baseStorageDir;
    private final File baseStagingDir;

    @Inject
    public FileStorageService(StorageManagerConfig config)
    {
        this(config.getDataDirectory());
    }

    public FileStorageService(File dataDirectory)
    {
        File baseDataDir = requireNonNull(dataDirectory, "dataDirectory is null");
        this.baseStorageDir = new File(baseDataDir, "storage");
        this.baseStagingDir = new File(baseDataDir, "staging");
    }

    @Override
    @PostConstruct
    public void start()
            throws IOException
    {
        deleteStagingFilesAsync();
        createParents(new File(baseStagingDir, "."));
        createParents(new File(baseStorageDir, "."));
    }

    @Override
    public long getAvailableBytes()
    {
        return baseStorageDir.getUsableSpace();
    }

    @PreDestroy
    public void stop()
            throws IOException
    {
        deleteDirectory(baseStagingDir);
    }

    @Override
    public File getStorageFile(long chunkId)
    {
        return getFileSystemPath(baseStorageDir, chunkId);
    }

    @Override
    public File getStagingFile(long chunkId)
    {
        String name = getFileSystemPath(new File("/"), chunkId).getName();
        return new File(baseStagingDir, name);
    }

    @Override
    public Set<Long> getStorageChunks()
    {
        ImmutableSet.Builder<Long> chunks = ImmutableSet.builder();
        for (File level1 : listFiles(baseStorageDir, FileStorageService::isHexDirectory)) {
            for (File level2 : listFiles(level1, FileStorageService::isHexDirectory)) {
                for (File file : listFiles(level2, path -> true)) {
                    if (file.isFile()) {
                        chunkIdFromFileName(file.getName()).ifPresent(chunks::add);
                    }
                }
            }
        }
        return chunks.build();
    }

    @Override
    public void createParents(File file)
    {
        File dir = file.getParentFile();
        if (!dir.mkdirs() && !dir.isDirectory()) {
            throw new PrestoException(RAPTOR_ERROR, "Failed creating directories: " + dir);
        }
    }

    /**
     * Generate a file system path for a chunk ID.
     * This creates a three level deep directory structure where the first
     * two levels each contain two hex digits (lowercase) of the xxhash64
     * of the ID in big endian hexadecimal and the final level contains the
     * full ID in decimal. Example:
     * <pre>
     * ID: 12345678
     * Hash: e4fc629b8375452c
     * Path: /base/e4/fc/12345678
     * </pre>
     * This ensures that files are spread out evenly through the tree
     * while a path can still be easily navigated by a human being.
     */
    public static File getFileSystemPath(File base, long chunkId)
    {
        checkArgument(chunkId >= 0, "chunkId is negative");
        String hash = format("%08x", XxHash64.hash(chunkId));
        return base.toPath()
                .resolve(hash.substring(0, 2))
                .resolve(hash.substring(2, 4))
                .resolve(String.valueOf(chunkId))
                .toFile();
    }

    private void deleteStagingFilesAsync()
    {
        List<File> files = listFiles(baseStagingDir, null);
        if (!files.isEmpty()) {
            new Thread(() -> {
                for (File file : files) {
                    try {
                        Files.deleteIfExists(file.toPath());
                    }
                    catch (IOException e) {
                        log.warn(e, "Failed to delete file: %s", file.getAbsolutePath());
                    }
                }
            }, "background-staging-delete").start();
        }
    }

    private static void deleteDirectory(File dir)
            throws IOException
    {
        if (!dir.exists()) {
            return;
        }
        File[] files = dir.listFiles();
        if (files == null) {
            throw new IOException("Failed to list directory: " + dir);
        }
        for (File file : files) {
            Files.deleteIfExists(file.toPath());
        }
        Files.deleteIfExists(dir.toPath());
    }

    private static List<File> listFiles(File dir, FileFilter filter)
    {
        File[] files = dir.listFiles(filter);
        if (files == null) {
            return ImmutableList.of();
        }
        return ImmutableList.copyOf(files);
    }

    private static boolean isHexDirectory(File file)
    {
        return file.isDirectory() && HEX_DIRECTORY.matcher(file.getName()).matches();
    }

    private static Optional<Long> chunkIdFromFileName(String name)
    {
        if (DIGITS.matchesAllOf(name)) {
            return Optional.of(Long.parseLong(name));
        }
        return Optional.empty();
    }
}
