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
package com.facebook.presto.raptorx.backup;

import com.facebook.presto.spi.PrestoException;
import com.google.common.annotations.VisibleForTesting;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import static com.facebook.presto.raptorx.RaptorErrorCode.RAPTOR_BACKUP_ERROR;
import static com.facebook.presto.raptorx.RaptorErrorCode.RAPTOR_BACKUP_NOT_FOUND;
import static com.facebook.presto.raptorx.storage.FileStorageService.getFileSystemPath;
import static java.nio.file.Files.deleteIfExists;
import static java.util.Objects.requireNonNull;

public class FileBackupStore
        implements BackupStore
{
    private final File baseDir;

    @Inject
    public FileBackupStore(FileBackupConfig config)
    {
        this(config.getBackupDirectory());
    }

    public FileBackupStore(File baseDir)
    {
        this.baseDir = requireNonNull(baseDir, "baseDir is null");
    }

    @PostConstruct
    public void start()
    {
        createDirectories(baseDir);
    }

    @Override
    public void backupChunk(long chunkId, File source)
    {
        File backupFile = getBackupFile(chunkId);

        try {
            try {
                // Optimistically assume the file can be created
                copyFile(source, backupFile);
            }
            catch (FileNotFoundException e) {
                createDirectories(backupFile.getParentFile());
                copyFile(source, backupFile);
            }
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_BACKUP_ERROR, "Failed to create backup chunk file", e);
        }
    }

    @Override
    public void restoreChunk(long chunkId, File target)
    {
        try {
            copyFile(getBackupFile(chunkId), target);
        }
        catch (FileNotFoundException e) {
            throw new PrestoException(RAPTOR_BACKUP_NOT_FOUND, "Backup chunk not found: " + chunkId, e);
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_BACKUP_ERROR, "Failed to copy backup chunk: " + chunkId, e);
        }
    }

    @Override
    public boolean deleteChunk(long chunkId)
    {
        try {
            return deleteIfExists(getBackupFile(chunkId).toPath());
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_BACKUP_ERROR, "Failed to delete backup chunk: " + chunkId, e);
        }
    }

    @Override
    public boolean chunkExists(long chunkId)
    {
        return getBackupFile(chunkId).isFile();
    }

    @VisibleForTesting
    public File getBackupFile(long chunkId)
    {
        return getFileSystemPath(baseDir, chunkId);
    }

    private static void createDirectories(File dir)
    {
        if (!dir.mkdirs() && !dir.isDirectory()) {
            throw new PrestoException(RAPTOR_BACKUP_ERROR, "Failed creating directories: " + dir);
        }
    }

    private static void copyFile(File source, File target)
            throws IOException
    {
        try (InputStream in = new FileInputStream(source);
                FileOutputStream out = new FileOutputStream(target)) {
            byte[] buffer = new byte[128 * 1024];
            while (true) {
                int n = in.read(buffer);
                if (n == -1) {
                    break;
                }
                out.write(buffer, 0, n);
            }
            out.flush();
            out.getFD().sync();
        }
    }
}
