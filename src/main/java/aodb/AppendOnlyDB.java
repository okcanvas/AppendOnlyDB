package aodb;

import aodb.codec.VarInt;
import aodb.codec.VarInt.Read;
import aodb.fs.DirectoryLock;
import aodb.hash.Checksums;
import aodb.storage.MMapUnmapper;
import aodb.storage.Segment;

import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class AppendOnlyDB implements AutoCloseable {

    // ==== Constants ==========================================================
    private static final int MAGIC = 0xA0DBA0DB;
    private static final byte VERSION_LEGACY = 1;
    private static final byte VERSION = 2;
    private static final byte FLAG_TOMBSTONE = 0x1;

    private static final int DEFAULT_SEG_BYTES = 1 << 30; // 1GB
    private static final int ROTATE_SAFETY_BYTES = 128;

    // ==== Durability & Listener ==============================================
    public enum Durability { RELAXED, BATCHED, SYNC_EACH_BATCH, SYNC_EACH_APPEND }
    public enum ListenerMode { SYNC, ASYNC, ASYNC_DROP_OLD }

    private static final int DEFAULT_FSYNC_EVERY_N = 4096;
    private static final long DEFAULT_FSYNC_EVERY_MS = 50;

    // ==== Config =============================================================
    public static final class Config {
        public final Path dir;
        public final int segBytes;
        public final int fsyncEveryN;
        public final long fsyncEveryMs;
        public final boolean fsyncMetadata;
        public final int partitions;
        public final boolean openReadOnly;
        public final Durability durability;
        public final boolean enableAsyncWriter;
        public final int writerQueueCapacity;
        public final int writerBatchMax;
        public final long writerBatchWaitMs;
        public final long offerTimeoutMs;
        public final ListenerMode listenerMode;
        public final int listenerThreads;
        public final int listenerQueueCapacity;
        public final boolean enableUnmap;
        public final boolean enableAsyncFsync;

        public Config(Path dir) {
            this(dir, DEFAULT_SEG_BYTES, DEFAULT_FSYNC_EVERY_N, DEFAULT_FSYNC_EVERY_MS, false, 1, false,
                    Durability.BATCHED, true, 1<<16, 1024, 2, 0, ListenerMode.SYNC, 2, 8192, true, true);
        }
        public Config(Path dir, boolean readOnly) {
            this(dir, DEFAULT_SEG_BYTES, DEFAULT_FSYNC_EVERY_N, DEFAULT_FSYNC_EVERY_MS, false, 1, readOnly,
                    readOnly ? Durability.RELAXED : Durability.BATCHED,
                    !readOnly, 1<<16, 1024, 2, 0, ListenerMode.SYNC, 2, 8192, true, true);
        }
        public Config(Path dir, int segBytes, int fsyncN, long fsyncMs, boolean fsyncMetadata,
                      int partitions, boolean ro, Durability durability,
                      boolean enableAsyncWriter, int writerQueueCapacity, int writerBatchMax,
                      long writerBatchWaitMs, long offerTimeoutMs,
                      ListenerMode listenerMode, int listenerThreads, int listenerQueueCapacity,
                      boolean enableUnmap, boolean enableAsyncFsync) {
            this.dir = dir;
            this.segBytes = segBytes;
            this.fsyncEveryN = fsyncN;
            this.fsyncEveryMs = fsyncMs;
            this.fsyncMetadata = fsyncMetadata;
            this.partitions = Math.max(1, partitions);
            this.openReadOnly = ro;
            this.durability = durability;
            this.enableAsyncWriter = enableAsyncWriter;
            this.writerQueueCapacity = writerQueueCapacity;
            this.writerBatchMax = writerBatchMax;
            this.writerBatchWaitMs = writerBatchWaitMs;
            this.offerTimeoutMs = offerTimeoutMs;
            this.listenerMode = listenerMode;
            this.listenerThreads = Math.max(1, listenerThreads);
            this.listenerQueueCapacity = Math.max(1024, listenerQueueCapacity);
            this.enableUnmap = enableUnmap;
            this.enableAsyncFsync = enableAsyncFsync;
        }
        public Config(Path dir, int segBytes, int fsyncN, long fsyncMs, boolean fsyncMetadata,
                      int partitions, boolean ro, Durability durability,
                      boolean enableAsyncWriter, int writerQueueCapacity, int writerBatchMax,
                      long writerBatchWaitMs, long offerTimeoutMs,
                      ListenerMode listenerMode, int listenerThreads, int listenerQueueCapacity,
                      boolean enableUnmap) {
            this(dir, segBytes, fsyncN, fsyncMs, fsyncMetadata,
                 partitions, ro, durability,
                 enableAsyncWriter, writerQueueCapacity, writerBatchMax,
                 writerBatchWaitMs, offerTimeoutMs,
                 listenerMode, listenerThreads, listenerQueueCapacity,
                 enableUnmap, true);
        }
    }

    // ==== Value / Slice ======================================================
    public static final class Value {
        public final byte[] bytes; public final long tsMs; public final boolean tombstone;
        Value(byte[] b, long ts, boolean tomb) { this.bytes=b; this.tsMs=ts; this.tombstone=tomb; }
    }
    public static final class Slice {
        public final ByteBuffer key, value; public final long tsMs; public final boolean tombstone;
        Slice(ByteBuffer k, ByteBuffer v, long ts, boolean t){ this.key=k.asReadOnlyBuffer(); this.value=v.asReadOnlyBuffer(); this.tsMs=ts; this.tombstone=t; }
    }

    // ==== Internals ==========================================================
    private static final class AppendTask {
        final String key; final byte[] value; final long tsMs; final boolean tombstone;
        final boolean durable;
        final CompletableFuture<Long> cf;
        AppendTask(String k, byte[] v, long ts, boolean t, boolean durable, CompletableFuture<Long> cf){
            this.key=k; this.value=v; this.tsMs=ts; this.tombstone=t; this.durable=durable; this.cf=cf;
        }
    }
    private static final class DurableCompletion {
        final CompletableFuture<Long> cf; final long addr;
        DurableCompletion(CompletableFuture<Long> cf, long addr){ this.cf=cf; this.addr=addr; }
    }

    private static long addr(int segId, int pos){ return ((long)segId << 32) | (pos & 0xFFFF_FFFFL); }
    private static int segId(long addr){ return (int)(addr >>> 32); }
    private static int segPos(long addr){ return (int)(addr & 0xFFFF_FFFFL); }

    private final Config cfg;
    private final Path dir;
    private final Path idxSnapshot;
    private final DirectoryLock dlock;

    private final List<Deque<Segment>> parts;
    private final List<ConcurrentHashMap<String, Long>> latestIdx;
    private final List<CopyOnWriteArrayList<Listener>> listeners;

    private final AtomicLong appendsSinceFsync = new AtomicLong();
    private volatile long lastFsyncMs = System.currentTimeMillis();
    private final Object appendLock = new Object();
    private volatile boolean closed=false;

    private final ArrayBlockingQueue<AppendTask> queue;
    private Thread writerThread;
    private volatile boolean writerRunning=false;

    private ByteBuffer writerBatchBuf = null;
    private FsyncWorker fsyncWorker;

    private ExecutorService listenerExec;
    public interface Listener { void onAppend(String key, Value v); }

    // ==== Construction / Recovery ===========================================
    public AppendOnlyDB(Config cfg) throws IOException {
        this.cfg = cfg;
        this.dir = cfg.dir;
        Files.createDirectories(dir);
        this.idxSnapshot = dir.resolve("index.snapshot");
        this.dlock = new DirectoryLock(dir, cfg.openReadOnly);

        this.parts = new ArrayList<>(cfg.partitions);
        this.latestIdx = new ArrayList<>(cfg.partitions);
        this.listeners = new ArrayList<>(cfg.partitions);
        for (int p=0; p<cfg.partitions; p++) {
            parts.add(new ArrayDeque<>());
            latestIdx.add(new ConcurrentHashMap<>());
            listeners.add(new CopyOnWriteArrayList<>());
        }
        this.queue = new ArrayBlockingQueue<>(Math.max(1024, cfg.writerQueueCapacity));

        if (cfg.listenerMode != ListenerMode.SYNC) {
            BlockingQueue<Runnable> q = new ArrayBlockingQueue<>(cfg.listenerQueueCapacity);
            RejectedExecutionHandler reh = (cfg.listenerMode == ListenerMode.ASYNC_DROP_OLD)
                    ? new ThreadPoolExecutor.DiscardOldestPolicy()
                    : new ThreadPoolExecutor.DiscardPolicy();
            this.listenerExec = new ThreadPoolExecutor(cfg.listenerThreads, cfg.listenerThreads,
                    60L, TimeUnit.SECONDS, q, r -> {
                        Thread t = new Thread(r, "aodb-sub");
                        t.setDaemon(true);
                        return t;
                    }, reh);
        }

        loadOrCreateSegments();
        loadIndex();
        if (cfg.enableAsyncWriter && !cfg.openReadOnly) startWriter();

        if (!cfg.openReadOnly && cfg.enableAsyncFsync && cfg.durability == Durability.BATCHED) {
            this.fsyncWorker = new FsyncWorker();
        }
    }

    private int partOf(String key){ return (key.hashCode() & 0x7fffffff) % cfg.partitions; }

    private void loadOrCreateSegments() throws IOException {
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir, "seg_*.aol")) {
            Map<Integer, TreeMap<Integer, Path>> found = new HashMap<>();
            for (Path p : ds) {
                String name = p.getFileName().toString();
                try {
                    String[] a = name.substring(4, name.length()-4).split("_");
                    int part = Integer.parseInt(a[0]);
                    int id = Integer.parseInt(a[1]);
                    found.computeIfAbsent(part, k -> new TreeMap<>()).put(id, p);
                } catch (Exception ignore) {}
            }
            for (int part=0; part<cfg.partitions; part++) {
                TreeMap<Integer, Path> map = found.getOrDefault(part, new TreeMap<>());
                if (map.isEmpty()) {
                    Path p = dir.resolve(String.format("seg_%d_%06d.aol", part, 0));
                    Segment s = new Segment(0, p, cfg.openReadOnly, cfg.segBytes);
                    parts.get(part).addLast(s);
                } else {
                    for (Map.Entry<Integer, Path> e : map.entrySet()) {
                        Segment s = new Segment(e.getKey(), e.getValue(), cfg.openReadOnly, cfg.segBytes);
                        parts.get(part).addLast(s);
                    }
                }
            }
        }
    }

    private void loadIndex() throws IOException {
        boolean snapshotOk = false;
        if (Files.exists(idxSnapshot)) {
            try (DataInputStream in = new DataInputStream(new BufferedInputStream(Files.newInputStream(idxSnapshot)))) {
                int partsN = in.readInt();
                if (partsN == cfg.partitions) {
                    for (int p=0; p<partsN; p++) {
                        int entries = in.readInt();
                        var map = latestIdx.get(p);
                        for (int i=0;i<entries;i++) {
                            int keyLen = in.readInt();
                            byte[] kb = in.readNBytes(keyLen);
                            long addr = in.readLong();
                            map.put(new String(kb, java.nio.charset.StandardCharsets.UTF_8), addr);
                        }
                    }
                    in.readLong();
                    snapshotOk = true;
                }
            } catch (Exception ignore) {}
        }
        if (!snapshotOk) {
            for (int p=0; p<cfg.partitions; p++) for (Segment s: parts.get(p)) scanSegmentIntoIndex(p, s);
        }
    }

    private void scanSegmentIntoIndex(int part, Segment s) throws IOException {
        s.ensureMapped();
        ByteBuffer mbb = s.roMap.duplicate().order(ByteOrder.BIG_ENDIAN);
        int pos = 0;
        while (pos + 8 <= s.writePos) {
            int magic = mbb.getInt(pos); if (magic != MAGIC) break;
            int totalLen = mbb.getInt(pos + 4);
            if (totalLen <= 0 || totalLen > cfg.segBytes) break;

            int recStart = pos + 8;
            int bodyEnd  = recStart + totalLen;
            if (bodyEnd > s.writePos) break;

            byte ver = mbb.get(recStart);
            int cursor = recStart + 2 + 8;
            int keyLen, valLen, kSize=4, vSize=4;
            if (ver == VERSION_LEGACY) {
                keyLen = mbb.getInt(cursor); cursor += 4;
                valLen = mbb.getInt(cursor); cursor += 4;
            } else if (ver == VERSION) {
                Read k = VarInt.getWithSize(mbb, cursor); cursor += k.size; keyLen = k.value; kSize = k.size;
                Read v = VarInt.getWithSize(mbb, cursor); cursor += v.size; valLen = v.value; vSize = v.size;
            } else break;

            int headerFixed = 1 + 1 + 8;
            int headerLens  = (ver == VERSION_LEGACY) ? (4 + 4) : (kSize + vSize);
            int expectedTotal = headerFixed + headerLens + keyLen + valLen + 4;
            if (expectedTotal != totalLen) break;

            int keyPos = cursor;
            int crcPos = bodyEnd - 4;

            int crcRead = mbb.getInt(crcPos);
            int crcCalc = Checksums.compute(mbb, recStart, crcPos);
            if (crcCalc != crcRead) break;

            ByteBuffer keySlice = mbb.duplicate();
            keySlice.position(keyPos).limit(keyPos + keyLen);
            byte[] keyBytes = new byte[keyLen];
            keySlice.get(keyBytes);
            String keyStr = new String(keyBytes, java.nio.charset.StandardCharsets.UTF_8);

            long address = addr(s.id, pos);
            latestIdx.get(part).put(keyStr, address);

            pos = bodyEnd;
        }
        s.writePos = pos;
        s.ensureMapped();
    }

    // ==== Public API =========================================================
    public CompletableFuture<Long> putAsync(String key, byte[] value, long tsMs){ return enqueue(key, value, tsMs, false, false); }
    public CompletableFuture<Long> deleteAsync(String key, long tsMs){ return enqueue(key, null, tsMs, true, false); }
    public CompletableFuture<Long> putAsyncDurable(String key, byte[] value, long tsMs){ return enqueue(key, value, tsMs, false, true); }
    public CompletableFuture<Long> deleteAsyncDurable(String key, long tsMs){ return enqueue(key, null, tsMs, true, true); }

    public long put(String key, byte[] value, long tsMs) throws IOException { return appendOneSync(key, value, tsMs, false); }
    public long delete(String key, long tsMs) throws IOException { return appendOneSync(key, null, tsMs, true); }

    public Value getLatest(String key) throws IOException {
        int part = partOf(key);
        Long address = latestIdx.get(part).get(key);
        if (address == null) return null;
        return readAt(address);
    }
    public Slice readLatestSlice(String key) throws IOException {
        int part = partOf(key);
        Long address = latestIdx.get(part).get(key);
        if (address == null) return null;
        Segment s = findSegmentById(segId(address));
        if (s == null) return null;
        try {
            s.ensureMapped();
            return sliceAt(address);
        } catch (Throwable t) {
            Value v = readAt(address);
            if (v == null) return null;
            return new Slice(ByteBuffer.wrap(key.getBytes(java.nio.charset.StandardCharsets.UTF_8)), ByteBuffer.wrap(v.bytes == null ? new byte[0] : v.bytes), v.tsMs, v.tombstone);
        }
    }

    public void subscribe(Listener l){ for (var ls: listeners) ls.add(l); }
    public void unsubscribe(Listener l){ for (var ls: listeners) ls.remove(l); }

    public void sync() throws IOException { doFsyncAllTails(cfg.fsyncMetadata); }

    public void refreshMappings() throws IOException {
        for (var dq : parts) {
            Segment tail = dq.getLast();
            if (tail == null) continue;
            long sz = tail.ch.size();
            int newPos = (int)Math.min(sz, (long)tail.capacity);
            if (newPos > tail.writePos) {
                tail.writePos = newPos;
                tail.ensureMapped();
            }
        }
    }

    public long nextAddressAfter(long address) throws IOException {
        if (address < 0) return -1L;
        int sid = segId(address);
        int pos = segPos(address);
        Segment s = findSegmentById(sid);
        if (s == null) return address;
        s.ensureMapped();
        if (pos + 8 > s.writePos) return address;
        ByteBuffer mbb = s.roMap.duplicate().order(ByteOrder.BIG_ENDIAN);
        if (mbb.getInt(pos) != MAGIC) return address;
        int totalLen = mbb.getInt(pos + 4);
        if (totalLen <= 0 || totalLen > cfg.segBytes) return address;
        int nextPos = pos + 8 + totalLen;
        return addr(sid, nextPos);
    }

    /** Keep only latest records by rewriting into a new tail; old segments removed. */
    public void compactPartition(int p) throws IOException {
        if (cfg.openReadOnly) throw new IOException("read-only mode");
        var dq = parts.get(p);
        if (dq.size() <= 1) return;
        Map<String, Long> snap = new HashMap<>(latestIdx.get(p));
        int newId = dq.getLast().id + 1;
        Path target = dir.resolve(String.format("seg_%d_%06d.aol", p, newId));
        Segment tgt = new Segment(newId, target, false, cfg.segBytes);
        for (Map.Entry<String, Long> e : snap.entrySet()) {
            Value v = readAt(e.getValue()); if (v == null) continue;
            int before = tgt.writePos;
            appendRecordToSegment(tgt, e.getKey(), v.bytes, v.tsMs, v.tombstone);
            latestIdx.get(p).put(e.getKey(), addr(tgt.id, before));
        }
        tgt.forceMeta(cfg.fsyncMetadata); dq.addLast(tgt);
        while (dq.size() > 1) {
            Segment old = dq.removeFirst();
            try { old.close(); } catch (Exception ignore) {}
            if (cfg.enableUnmap) MMapUnmapper.unmap(old.roMap);
            try { Files.deleteIfExists(old.path); } catch (IOException ignore) {}
        }
    }

    // ==== NEW: Segment pruning ===============================================
    /**
     * 파티션에서 inclusiveLsn 이 들어 있는 세그먼트 "앞"의 세그먼트들을 삭제합니다.
     * 같은 세그먼트 내부는 잘라내지 않습니다.
     * @return 삭제된 세그먼트 개수
     */
    public int pruneSegmentsUpTo(int partition, long inclusiveLsn) throws IOException {
        if (cfg.openReadOnly) throw new IOException("read-only mode");
        if (partition < 0 || partition >= cfg.partitions) throw new IllegalArgumentException("bad partition");
        if (inclusiveLsn < 0) return 0;

        final int cutoffSegId = segId(inclusiveLsn);
        final Deque<Segment> dq = parts.get(partition);
        if (dq.isEmpty()) return 0;

        int removed = 0;
        synchronized (appendLock) {
            while (dq.size() > 1) { // 최소 1개는 남김
                Segment head = dq.peekFirst();
                if (head == null) break;
                if (head.id >= cutoffSegId) break;

                dq.removeFirst();
                try { head.close(); } catch (Throwable ignore) {}
                if (cfg.enableUnmap) {
                    try { MMapUnmapper.unmap(head.roMap); } catch (Throwable ignore) {}
                }
                try { Files.deleteIfExists(head.path); }
                catch (IOException ioe) { System.err.println("[PRUNE] delete failed: " + head.path + " : " + ioe); }
                removed++;
            }

            if (removed > 0) {
                final ConcurrentHashMap<String, Long> map = latestIdx.get(partition);
                for (var it = map.entrySet().iterator(); it.hasNext(); ) {
                    var e = it.next();
                    long addr = e.getValue();
                    if (segId(addr) < cutoffSegId) it.remove();
                }
            }
        }

        if (removed > 0) {
            System.out.println("[PRUNE] partition=" + partition + " removedSegments=" + removed + " < segId " + cutoffSegId);
        }
        return removed;
    }

    // ==== Tail scan ==========================================================
    public interface RecordHandler { boolean on(String key, Slice slice, long address) throws IOException; }

    public long scanFrom(int partition, long fromAddress, RecordHandler handler) throws IOException {
        if (partition < 0 || partition >= cfg.partitions) throw new IllegalArgumentException("bad partition");
        var dq = parts.get(partition);
        if (dq.isEmpty()) return fromAddress;

        int startSegId, startPos;
        if (fromAddress < 0) {
            Segment first = dq.getFirst();
            startSegId = first.id; startPos = 0;
        } else {
            startSegId = segId(fromAddress);
            startPos   = segPos(fromAddress);
        }

        long lastAddr = fromAddress;
        for (Segment s : dq) {
            if (s.id < startSegId) continue;
            s.ensureMapped();
            ByteBuffer mbb = s.roMap.duplicate().order(ByteOrder.BIG_ENDIAN);

            int pos = (s.id == startSegId) ? startPos : 0;
            while (pos + 8 <= s.writePos) {
                int magic = mbb.getInt(pos); if (magic != MAGIC) break;
                int totalLen = mbb.getInt(pos + 4);
                if (totalLen <= 0 || totalLen > cfg.segBytes) break;
                int recStart = pos + 8;
                int bodyEnd  = recStart + totalLen;
                if (bodyEnd > s.writePos) break;

                byte ver = mbb.get(recStart);
                byte flags = mbb.get(recStart + 1);
                long ts = mbb.getLong(recStart + 2);
                int cursor = recStart + 2 + 8;

                int keyLen, valLen, kSize=4, vSize=4;
                if (ver == VERSION_LEGACY) {
                    keyLen = mbb.getInt(cursor); cursor += 4;
                    valLen = mbb.getInt(cursor); cursor += 4;
                } else if (ver == VERSION) {
                    Read k = VarInt.getWithSize(mbb, cursor); cursor += k.size; keyLen = k.value; kSize = k.size;
                    Read v = VarInt.getWithSize(mbb, cursor); cursor += v.size; valLen = v.value; vSize = v.size;
                } else break;

                int headerFixed = 1 + 1 + 8;
                int headerLens  = (ver == VERSION_LEGACY) ? (4 + 4) : (kSize + vSize);
                int expectedTotal = headerFixed + headerLens + keyLen + valLen + 4;
                if (expectedTotal != totalLen) break;

                int keyPos = cursor;
                int valPos = keyPos + keyLen;
                int crcPos = bodyEnd - 4;
                if (crcPos < valPos) break;

                int crcRead = mbb.getInt(crcPos);
                int crcCalc = Checksums.compute(mbb, recStart, crcPos);
                if (crcCalc != crcRead) break;

                ByteBuffer keySlice = mbb.duplicate(); keySlice.limit(keyPos + keyLen).position(keyPos); keySlice = keySlice.slice();
                ByteBuffer valSlice = mbb.duplicate(); valSlice.limit(valPos + valLen).position(valPos); valSlice = valSlice.slice();
                boolean tomb = (flags & FLAG_TOMBSTONE) != 0;

                byte[] kb = new byte[keyLen];
                keySlice.mark(); keySlice.get(kb); keySlice.reset();
                String kStr = new String(kb, java.nio.charset.StandardCharsets.UTF_8);

                long address = addr(s.id, pos);
                lastAddr = address;
                if (!handler.on(kStr, new Slice(keySlice, valSlice, ts, tomb), address)) {
                    return lastAddr;
                }
                pos = bodyEnd;
            }
        }
        return lastAddr;
    }

    // ==== LSN state helpers ==================================================
    public void saveLsn(int partition, long address) throws IOException {
        Path p = dir.resolve(String.format("state_p%d.lsn", partition));
        ByteBuffer b = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
        b.putLong(address).flip();
        try (FileChannel ch = FileChannel.open(p,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            ch.write(b); ch.force(true);
        }
    }
    public long loadLsn(int partition) throws IOException {
        Path p = dir.resolve(String.format("state_p%d.lsn", partition));
        if (!Files.exists(p)) return -1L;
        ByteBuffer b = ByteBuffer.allocate(8);
        try (FileChannel ch = FileChannel.open(p, StandardOpenOption.READ)) {
            if (ch.read(b) < 8) return -1L;
        }
        b.flip();
        return b.getLong();
    }

    // ==== Async Writer =======================================================
    private CompletableFuture<Long> enqueue(String key, byte[] value, long tsMs, boolean tombstone, boolean durable){
        if (cfg.openReadOnly) throw new RejectedExecutionException("read-only mode");
        if (!cfg.enableAsyncWriter) throw new RejectedExecutionException("async writer disabled");
        CompletableFuture<Long> cf = new CompletableFuture<>();
        AppendTask t = new AppendTask(key, value, tsMs, tombstone, durable, cf);
        try {
            boolean ok = (cfg.offerTimeoutMs <= 0) ? queue.offer(t) : queue.offer(t, cfg.offerTimeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
            if (!ok) throw new RejectedExecutionException("writer queue full");
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            cf.completeExceptionally(ie); return cf;
        }
        return cf;
    }

    private void startWriter() {
        writerRunning = true;
        writerThread = new Thread(this::writerLoop, "aodb-writer");
        writerThread.setDaemon(true);
        writerThread.start();
    }

    private void writerLoop() {
        final int BATCH_MAX = Math.max(1, cfg.writerBatchMax);
        final long WAIT_MS = Math.max(0, cfg.writerBatchWaitMs);
        ArrayList<AppendTask> batch = new ArrayList<>(BATCH_MAX);
        while (writerRunning || !queue.isEmpty()) {
            try {
                AppendTask first = queue.poll(WAIT_MS, java.util.concurrent.TimeUnit.MILLISECONDS);
                if (first == null) continue;
                batch.add(first);
                queue.drainTo(batch, BATCH_MAX - 1);

                Map<Integer, List<AppendTask>> byPart = new HashMap<>();
                for (AppendTask t : batch) byPart.computeIfAbsent(partOf(t.key), k -> new ArrayList<>()).add(t);

                long completed = 0;
                List<DurableCompletion> durList = new ArrayList<>();

                synchronized (appendLock) {
                    for (Map.Entry<Integer, List<AppendTask>> e : byPart.entrySet()) {
                        int p = e.getKey();
                        List<AppendTask> list = e.getValue();
                        if (list.isEmpty()) continue;

                        appendBatchCoalesced(p, list, durList);
                        completed += list.size();

                        if (cfg.durability == Durability.SYNC_EACH_APPEND) {
                            try { parts.get(p).getLast().forceMeta(cfg.fsyncMetadata); } catch (IOException ignore) {}
                        }
                    }

                    boolean needSyncNow = false;

                    switch (cfg.durability) {
                        case RELAXED:
                            needSyncNow = !durList.isEmpty();
                            break;
                        case BATCHED: {
                            long n = appendsSinceFsync.addAndGet(completed);
                            long now = System.currentTimeMillis();
                            if (!durList.isEmpty()) {
                                needSyncNow = true;
                            } else if (n >= cfg.fsyncEveryN || now - lastFsyncMs >= cfg.fsyncEveryMs) {
                                if (fsyncWorker != null) fsyncWorker.request();
                                else needSyncNow = true;
                                appendsSinceFsync.set(0);
                                lastFsyncMs = now;
                            }
                            break;
                        }
                        case SYNC_EACH_BATCH:
                            needSyncNow = true;
                            appendsSinceFsync.set(0);
                            lastFsyncMs = System.currentTimeMillis();
                            break;
                        case SYNC_EACH_APPEND:
                            appendsSinceFsync.set(0);
                            lastFsyncMs = System.currentTimeMillis();
                            break;
                    }

                    if (needSyncNow) {
                        try { doFsyncAllTails(cfg.fsyncMetadata); } catch (IOException ignore) {}
                        appendsSinceFsync.set(0);
                        lastFsyncMs = System.currentTimeMillis();
                    }

                    if (!durList.isEmpty()) {
                        for (DurableCompletion dc : durList) dc.cf.complete(dc.addr);
                    }
                }
            } catch (Throwable t) {
                for (AppendTask at : batch) at.cf.completeExceptionally(t);
            } finally { batch.clear(); }
        }
    }

    private void appendBatchCoalesced(int part, List<AppendTask> list, List<DurableCompletion> durList) throws IOException {
        Segment seg = parts.get(part).getLast();

        final int n = list.size();
        int[] recSizes = new int[n];
        int total = 0;
        for (int i=0; i<n; i++) {
            AppendTask t = list.get(i);
            int size = estimateRecordBytes(t.key, t.value);
            recSizes[i] = size;
            total += size;
        }

        if (seg.remaining() < total + ROTATE_SAFETY_BYTES) {
            int newId = seg.id + 1;
            Segment ns = new Segment(newId, dir.resolve(String.format("seg_%d_%06d.aol", part, newId)), false, cfg.segBytes);
            parts.get(part).addLast(ns);
            seg = ns;
        }

        ensureWriterBuffer(total);
        writerBatchBuf.clear();
        writerBatchBuf.order(ByteOrder.BIG_ENDIAN);

        int basePos = seg.writePos;
        int pos = basePos;

        for (int i=0; i<n; i++) encodeOneInto(writerBatchBuf, list.get(i));

        writerBatchBuf.flip();
        seg.ch.position(seg.writePos);
        while (writerBatchBuf.hasRemaining()) seg.ch.write(writerBatchBuf);
        seg.writePos += total;

        for (int i=0; i<n; i++) {
            long address = addr(seg.id, pos);
            AppendTask t = list.get(i);
            latestIdx.get(part).put(t.key, address);
            Value v = new Value(t.value, t.tsMs, t.tombstone);
            notifyListeners(part, t.key, v);
            if (t.durable) durList.add(new DurableCompletion(t.cf, address));
            else t.cf.complete(address);
            pos += recSizes[i];
        }
    }

    private void ensureWriterBuffer(int minCapacity) {
        if (writerBatchBuf == null || writerBatchBuf.capacity() < minCapacity) {
            int cap = Math.max(minCapacity, (writerBatchBuf == null ? 0 : writerBatchBuf.capacity()) * 2);
            if (cap < 1 << 20) cap = Math.max(cap, 1 << 20);
            writerBatchBuf = ByteBuffer.allocateDirect(cap);
        }
    }

    private void encodeOneInto(ByteBuffer buf, AppendTask t) {
        byte[] kb = t.key.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        int keyLen = kb.length;
        int valLen = (t.value == null) ? 0 : t.value.length;
        int bodyLen = 1 + 1 + 8 + VarInt.len(keyLen) + VarInt.len(valLen) + keyLen + valLen + 4;

        buf.putInt(MAGIC);
        buf.putInt(bodyLen);
        int payloadStart = buf.position();
        buf.put(VERSION);
        buf.put((byte)(t.tombstone ? FLAG_TOMBSTONE : 0));
        buf.putLong(t.tsMs);
        VarInt.put(buf, keyLen);
        VarInt.put(buf, valLen);
        buf.put(kb);
        if (valLen > 0) buf.put(t.value);
        int crc = Checksums.compute(buf, payloadStart, buf.position());
        buf.putInt(crc);
    }

    private long appendOneSync(String key, byte[] value, long tsMs, boolean tombstone) throws IOException {
        if (cfg.openReadOnly) throw new IOException("read-only mode");
        int p = partOf(key);
        synchronized (appendLock) {
            Segment seg = parts.get(p).getLast();
            int need = estimateRecordBytes(key, value);
            if (seg.remaining() < need + ROTATE_SAFETY_BYTES) {
                int newId = seg.id + 1;
                Segment ns = new Segment(newId, dir.resolve(String.format("seg_%d_%06d.aol", p, newId)), false, cfg.segBytes);
                parts.get(p).addLast(ns);
                seg = ns;
            }
            int before = seg.writePos;
            appendRecordToSegment(seg, key, value, tsMs, tombstone);
            long address = addr(seg.id, before);
            latestIdx.get(p).put(key, address);
            Value v = new Value(value, tsMs, tombstone);
            notifyListeners(p, key, v);

            switch (cfg.durability) {
                case RELAXED: break;
                case BATCHED: {
                    long n = appendsSinceFsync.incrementAndGet();
                    long now = System.currentTimeMillis();
                    if (n >= cfg.fsyncEveryN || now - lastFsyncMs >= cfg.fsyncEveryMs) {
                        if (fsyncWorker != null) fsyncWorker.request();
                        else doFsyncAllTails(cfg.fsyncMetadata);
                        appendsSinceFsync.set(0);
                        lastFsyncMs = now;
                    }
                    break;
                }
                case SYNC_EACH_BATCH:
                case SYNC_EACH_APPEND:
                    doFsyncAllTails(cfg.fsyncMetadata);
                    appendsSinceFsync.set(0);
                    lastFsyncMs = System.currentTimeMillis();
                    break;
            }
            return address;
        }
    }

    private void appendRecordToSegment(Segment seg, String key, byte[] val, long tsMs, boolean tombstone) throws IOException {
        byte[] kb = key.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        int keyLen = kb.length;
        int valLen = (val == null) ? 0 : val.length;
        int bodyLen = 1 + 1 + 8 + VarInt.len(keyLen) + VarInt.len(valLen) + keyLen + valLen + 4;
        int recBytes = 4 + 4 + bodyLen;
        ByteBuffer buf = ByteBuffer.allocate(recBytes);
        buf.order(ByteOrder.BIG_ENDIAN);
        buf.putInt(MAGIC);
        buf.putInt(bodyLen);
        int payloadStart = buf.position();
        buf.put(VERSION);
        buf.put((byte)(tombstone ? FLAG_TOMBSTONE : 0));
        buf.putLong(tsMs);
        VarInt.put(buf, keyLen);
        VarInt.put(buf, valLen);
        buf.put(kb);
        if (valLen > 0) buf.put(val);
        int crc = Checksums.compute(buf, payloadStart, buf.position());
        buf.putInt(crc);
        buf.flip();
        seg.ch.write(buf, seg.writePos);
        seg.writePos += recBytes;
    }

    private Value readAt(long address) throws IOException {
        int sid = segId(address);
        int pos = segPos(address);
        Segment s = findSegmentById(sid);
        if (s == null) return null;
        ByteBuffer hdr = ByteBuffer.allocate(8);
        int n = s.ch.read(hdr, pos);
        if (n < 8) return null;
        hdr.flip();
        int magic = hdr.getInt();
        int totalLen = hdr.getInt();
        if (magic != MAGIC || totalLen <= 0 || totalLen > cfg.segBytes) return null;
        ByteBuffer rec = ByteBuffer.allocate(totalLen);
        s.ch.read(rec, pos + 8);
        rec.flip();
        byte ver = rec.get();
        byte flags = rec.get();
        long ts = rec.getLong();
        int keyLen, valLen;
        if (ver == VERSION_LEGACY) {
            keyLen = rec.getInt();
            valLen = rec.getInt();
        } else {
            keyLen = VarInt.readFrom(rec);
            valLen = VarInt.readFrom(rec);
        }
        byte[] key = new byte[keyLen]; rec.get(key);
        byte[] val = new byte[valLen]; if (valLen > 0) rec.get(val);
        int crcRead = rec.getInt();
        int headerBytesBeforeKey = (ver == VERSION_LEGACY) ? (1+1+8+4+4) : (1+1+8+VarInt.len(keyLen)+VarInt.len(valLen));
        ByteBuffer forCrc = ByteBuffer.allocate(headerBytesBeforeKey + keyLen + valLen);
        forCrc.put(ver).put(flags).putLong(ts);
        if (ver == VERSION_LEGACY) { forCrc.putInt(keyLen).putInt(valLen); }
        else { VarInt.put(forCrc, keyLen); VarInt.put(forCrc, valLen); }
        forCrc.put(key); if (valLen > 0) forCrc.put(val); forCrc.flip();
        int crcCalc = Checksums.compute(forCrc, 0, forCrc.limit());
        if (crcCalc != crcRead) throw new IOException("CRC mismatch at address " + address);
        boolean tomb = (flags & FLAG_TOMBSTONE) != 0;
        return new Value(tomb ? null : val, ts, tomb);
    }

    private Slice sliceAt(long address) throws IOException {
        int sid = segId(address); int pos = segPos(address);
        Segment s = findSegmentById(sid);
        if (s == null) return null; s.ensureMapped();
        ByteBuffer mbb = s.roMap.duplicate().order(ByteOrder.BIG_ENDIAN);
        int magic = mbb.getInt(pos); if (magic != MAGIC) return null;
        int totalLen = mbb.getInt(pos + 4); if (totalLen <= 0) return null;
        int recStart = pos + 8;
        byte ver = mbb.get(recStart); if (ver == VERSION_LEGACY) return null;
        byte flags = mbb.get(recStart + 1);
        long ts = mbb.getLong(recStart + 2);
        int cursor = recStart + 2 + 8;
        Read k = VarInt.getWithSize(mbb, cursor); cursor += k.size;
        Read v = VarInt.getWithSize(mbb, cursor); cursor += v.size;
        int keyLen = k.value, valLen = v.value;
        ByteBuffer keySlice = mbb.duplicate(); keySlice.limit(cursor + keyLen).position(cursor); keySlice = keySlice.slice();
        int valPos = cursor + keyLen;
        ByteBuffer valSlice = mbb.duplicate(); valSlice.limit(valPos + valLen).position(valPos); valSlice = valSlice.slice();
        boolean tomb = (flags & FLAG_TOMBSTONE) != 0;
        return new Slice(keySlice, valSlice, ts, tomb);
    }

    private Segment findSegmentById(int id){
        for (var dq : parts) for (Segment s : dq) if (s.id == id) return s;
        return null;
    }

    private void doFsyncAllTails(boolean meta) throws IOException {
        for (var dq : parts) {
            Segment tail = dq.getLast();
            if (tail != null) tail.forceMeta(meta);
        }
    }

    public void saveIndexSnapshot() throws IOException {
        if (cfg.openReadOnly) return;
        doFsyncAllTails(cfg.fsyncMetadata);
        Path tmp = dir.resolve("index.snapshot.tmp");
        try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(tmp, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)))) {
            out.writeInt(cfg.partitions);
            for (int p=0; p<cfg.partitions; p++) {
                var map = latestIdx.get(p);
                out.writeInt(map.size());
                for (var e : map.entrySet()) {
                    byte[] kb = e.getKey().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    out.writeInt(kb.length); out.write(kb); out.writeLong(e.getValue());
                }
            }
            out.writeLong(0L);
            out.flush();
        }
        try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.WRITE)) { ch.force(true); }
        Files.move(tmp, idxSnapshot, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    private void notifyListeners(int p, String key, Value v) {
        List<Listener> ls = listeners.get(p);
        if (ls.isEmpty()) return;
        if (cfg.listenerMode == ListenerMode.SYNC || listenerExec == null) {
            for (Listener l : ls) { try { l.onAppend(key, v); } catch (Throwable ignore) {} }
        } else {
            for (Listener l : ls) {
                Runnable task = () -> { try { l.onAppend(key, v); } catch (Throwable ignore) {} };
                try { listenerExec.execute(task); } catch (Throwable ignore) {}
            }
        }
    }

    private final class FsyncWorker implements AutoCloseable {
        private final AtomicBoolean pending = new AtomicBoolean(false);
        private final Object signal = new Object();
        private volatile boolean running = true;
        private final Thread thread;

        FsyncWorker() {
            thread = new Thread(this::loop, "aodb-fsync");
            thread.setDaemon(true);
            thread.start();
        }

        void request() {
            if (pending.compareAndSet(false, true)) {
                synchronized (signal) { signal.notify(); }
            }
        }

        private void loop() {
            while (running) {
                try {
                    synchronized (signal) {
                        while (running && !pending.get()) signal.wait();
                    }
                    if (!running) break;
                    pending.set(false);
                    doFsyncAllTails(cfg.fsyncMetadata);
                } catch (Throwable ignore) {}
            }
        }

        @Override public void close() {
            running = false;
            synchronized (signal) { signal.notify(); }
            try { thread.join(1000); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
        }
    }

    @Override public void close() throws IOException {
        if (closed) return; closed = true;
        if (writerThread != null) {
            writerRunning=false;
            while (writerThread.isAlive()) {
                try { writerThread.join(100); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
            }
        }
        if (fsyncWorker != null) fsyncWorker.close();
        try { doFsyncAllTails(cfg.fsyncMetadata);} catch (Exception ignore) {}
        saveIndexSnapshot();
        for (var dq : parts) for (var s : dq) try { s.close(); } catch (Exception ignore) {}
        if (listenerExec != null) listenerExec.shutdownNow();
        dlock.close();
    }

    public static void main(String[] args) throws Exception {
        Path dir = Paths.get("./aodb-data");
        Config writerCfg = new Config(dir);
        try (AppendOnlyDB db = new AppendOnlyDB(writerCfg)) {
            List<CompletableFuture<Long>> futures = new ArrayList<>();
            for (int i=0; i<10_000; i++) {
                String key = "room:1/msg:" + i;
                byte[] val = ("v" + i).getBytes();
                futures.add(db.putAsyncDurable(key, val, System.currentTimeMillis()));
            }
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            var v = db.getLatest("room:1/msg:9999");
            if (v != null && v.bytes != null) System.out.println("GET(copy): " + new String(v.bytes));
        }
        try (AppendOnlyDB db = new AppendOnlyDB(new Config(dir, true))) {
            db.refreshMappings();
            var v = db.getLatest("room:1/msg:9999");
            if (v != null && v.bytes != null) System.out.println("READER: " + new String(v.bytes));
        }
    }

    private int estimateRecordBytes(String key, byte[] val){
        int k = key.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        int v = (val == null) ? 0 : val.length;
        int body = 1 + 1 + 8 + VarInt.len(k) + VarInt.len(v) + k + v + 4;
        return 4 + 4 + body;
    }
}
