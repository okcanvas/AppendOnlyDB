package aodb.storage;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.Set;

public final class Segment implements AutoCloseable {
    public final int id;
    public final Path path;
    public final boolean readOnly;
    public final int capacity;
    public final FileChannel ch;
    public volatile int writePos;
    public volatile MappedByteBuffer roMap;
    public volatile int mappedSize;

    public Segment(int id, Path path, boolean readOnly, int capacity) throws IOException {
        this.id = id; this.path = path; this.readOnly = readOnly; this.capacity = capacity;
        this.ch = FileChannel.open(path,
                readOnly ? Set.of(StandardOpenOption.READ)
                         : Set.of(StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE));
        this.writePos = (int)Math.min(ch.size(), (long)capacity);
        this.roMap = null; this.mappedSize = 0;
    }

    public int remaining(){ return capacity - writePos; }

    public void forceMeta(boolean meta) throws IOException { ch.force(meta); }

    public void ensureMapped() throws IOException {
        if (roMap == null || mappedSize < writePos) {
            roMap = ch.map(FileChannel.MapMode.READ_ONLY, 0, Math.max(1, writePos));
            mappedSize = writePos;
        }
    }

    @Override public void close() throws IOException { ch.close(); }
}