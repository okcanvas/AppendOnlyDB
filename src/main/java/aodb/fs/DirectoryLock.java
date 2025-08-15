package aodb.fs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.*;

public final class DirectoryLock implements AutoCloseable {
    private final Path lockPath;
    private final boolean readOnly;
    private FileChannel ch;
    private FileLock lock;

    public DirectoryLock(Path dir, boolean readOnly) throws IOException {
        this.lockPath = dir.resolve(".aodb.lock");
        this.readOnly = readOnly;
        acquire();
    }

    private void acquire() throws IOException {
        // Open READ+WRITE so we can obtain shared or exclusive locks portably
        ch = FileChannel.open(lockPath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        try {
            ch.truncate(0);
            String meta = "pid=" + ProcessHandle.current().pid() + ", mode=" + (readOnly ? "RO" : "RW") + "\n";
            ch.write(ByteBuffer.wrap(meta.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
            ch.force(true);
        } catch (Throwable ignore) {}

        try {
            lock = readOnly ? ch.tryLock(0L, Long.MAX_VALUE, true) : ch.tryLock();
        } catch (OverlappingFileLockException e) {
            throw new IOException("Directory already locked by this JVM", e);
        }
        if (lock == null) throw new IOException("Failed to acquire " + (readOnly ? "shared" : "exclusive") + " lock at " + lockPath);
    }

    @Override public void close() throws IOException {
        try { if (lock != null) lock.release(); } finally { if (ch != null) ch.close(); }
    }
}