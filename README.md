# AppendOnlyDB (AODB)
_A fast append-only key–value store with async writer, durable acks, and outbox-tail helpers._

> **Status**: production-leaning prototype used for outbox-style messaging, log capture, and write-heavy workloads.

---

## Highlights

- **Append-only segments** on disk (`seg_<partition>_<id>.aol`) with a compact, checksummed binary format.
- **High write throughput** via a single writer thread using micro-batched, direct `ByteBuffer` I/O.
- **Durability modes**: `RELAXED`, `BATCHED`, `SYNC_EACH_BATCH`, `SYNC_EACH_APPEND`.
- **Durable ACK API** (`putAsyncDurable`) — future completes only after `fsync`.
- **Outbox helpers**: `scanFrom`, `saveLsn`/`loadLsn`, `nextAddressAfter`, `refreshMappings`.
- **Consumer‑driven retention**: `pruneSegmentsUpTo(partition, lsn)` and `compactPartition(partition)`.
- **Low-latency listeners** with `SYNC` / `ASYNC` / `ASYNC_DROP_OLD` modes.
- **Crash‑safe startup** with `DirectoryLock` to prevent concurrent writers.

Test harness (**OutboxSoak**) included for sustained QPS soak, compaction & pruning.

---

## Getting Started

### Build
```bash
mvn -q -DskipTests package
```

### Minimal usage
```java
import aodb.AppendOnlyDB;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;

Path dir = Paths.get("./aodb-data");
AppendOnlyDB.Config cfg = new AppendOnlyDB.Config(dir);  // defaults: BATCHED durability, async writer
try (AppendOnlyDB db = new AppendOnlyDB(cfg)) {
    // async put (non-durable)
    CompletableFuture<Long> f1 = db.putAsync("k1", "v1".getBytes(), System.currentTimeMillis());

    // async put (durable ack → fsync before completion)
    CompletableFuture<Long> f2 = db.putAsyncDurable("k2", "v2".getBytes(), System.currentTimeMillis());

    // sync put (blocking)
    long addr = db.put("k3", "v3".getBytes(), System.currentTimeMillis());

    // lookup latest by key
    AppendOnlyDB.Value v = db.getLatest("k3");
}
```

### Read‑only process
```java
AppendOnlyDB ro = new AppendOnlyDB(new AppendOnlyDB.Config(dir, /*readOnly*/ true));
ro.refreshMappings(); // if the writer is growing tail segments
```

> **Process model**: exactly **one writer process** (exclusive lock). Multiple read‑only processes can open the database directory concurrently.

---

## API Overview

**Async write**  
- `CompletableFuture<Long> putAsync(String key, byte[] value, long tsMs)`  
- `CompletableFuture<Long> deleteAsync(String key, long tsMs)`  
- `CompletableFuture<Long> putAsyncDurable(String key, byte[] value, long tsMs)`  
- `CompletableFuture<Long> deleteAsyncDurable(String key, long tsMs)`

**Sync write**  
- `long put(String key, byte[] value, long tsMs)`  
- `long delete(String key, long tsMs)`

**Lookup / read**  
- `AppendOnlyDB.Value getLatest(String key)` – materializes the latest value for `key` (null if tombstone).  
- `AppendOnlyDB.Slice readLatestSlice(String key)` – zero‑copy slices for key/value (read-only views).

**Tail scanning (dispatcher / consumer)**  
- `long scanFrom(int partition, long fromAddress, RecordHandler handler)`  
  Iterates records sequentially from `fromAddress` (inclusive) to the tail. Returns the last visited address.

**LSN helpers**  
- `void saveLsn(int partition, long address)` → saves consumer cursor (`state_p<partition>.lsn`).  
- `long loadLsn(int partition)` → loads last saved cursor (or `-1` if none).  
- `long nextAddressAfter(long address)` → computes the next record start after `address` (restart w/o duplication).

**Maintenance**  
- `int pruneSegmentsUpTo(int partition, long inclusiveLsn)` – deletes whole segments whose end is ≤ `inclusiveLsn`.  
- `void compactPartition(int partition)` – rewrites only the *latest* version of each key into a fresh tail segment.  
- `void refreshMappings()` – for RO/tailer: detect tail growth and remap mmaps when file expands.  
- `void sync()` – force fsync on all tails.  
- `void saveIndexSnapshot()` – persist in‑memory last‑write index (loaded at startup for faster recovery).

**Listeners**  
- `subscribe(Listener l)` / `unsubscribe(Listener l)` – invoked on each append (see `ListenerMode` below).

---

## Configuration

Use `AppendOnlyDB.Config` to tune durability, batching, partitions, and listener behavior.

```java
public static final class Config {
    public final Path dir;
    public final int  segBytes;                 // per-segment logical capacity (e.g. 1<<30)
    public final int  fsyncEveryN;              // BATCHED: fsync after N appends
    public final long fsyncEveryMs;             // BATCHED: or after this many ms
    public final boolean fsyncMetadata;         // include metadata in fsync
    public final int  partitions;               // # of independent append tails
    public final boolean openReadOnly;
    public final Durability durability;         // RELAXED, BATCHED, SYNC_EACH_BATCH, SYNC_EACH_APPEND
    public final boolean enableAsyncWriter;     // use background writer thread + micro-batching
    public final int  writerQueueCapacity;
    public final int  writerBatchMax;
    public final long writerBatchWaitMs;
    public final long offerTimeoutMs;           // enqueue timeout for async writer
    public final ListenerMode listenerMode;     // SYNC, ASYNC, ASYNC_DROP_OLD
    public final int  listenerThreads;
    public final int  listenerQueueCapacity;
    public final boolean enableUnmap;           // try to unmap old segments on close/rotate
    public final boolean enableAsyncFsync;      // BATCHED: offload fsync to a worker
}
```

**Constructors**
- `Config(Path dir)` – sensible defaults (BATCHED, async writer on).  
- `Config(Path dir, boolean readOnly)` – read‑only view (RELAXED, writer off).  
- Full constructor for fine tuning (see source).

### Durability modes

- `RELAXED` – never fsync on normal appends; **durable requests** still fsync immediately.  
- `BATCHED` – fsync periodically (`fsyncEveryN` or `fsyncEveryMs`), optional async fsync worker; **durable requests** trigger immediate fsync.  
- `SYNC_EACH_BATCH` – fsync after each writer micro‑batch.  
- `SYNC_EACH_APPEND` – fsync on every append (lowest throughput, strongest guarantees).

### Listener modes

- `SYNC` – run listener callbacks inline with the writer.  
- `ASYNC` – thread‑pool offload, backpressure (new events may wait).  
- `ASYNC_DROP_OLD` – thread‑pool offload, drop oldest tasks when saturated.

---

## File & Record Format

- Files are segmented per partition: `seg_<partition>_<id>.aol` where `id` is zero‑padded (e.g., `seg_0_000112.aol`).  
- **Record framing** (big‑endian):
  - `4B` **MAGIC** = `0xA0DBA0DB`
  - `4B` **TOTAL_LEN** (payload bytes below)
  - **Payload**:
    - `1B` **VERSION** (`2` = current, `1` = legacy)
    - `1B` **FLAGS** (`0x1` tombstone)
    - `8B` **tsMs** (app timestamps)
    - `varint` **keyLen**, `varint` **valLen** (VERSION=2); legacy uses `int` lengths
    - `keyLen` bytes **key** (UTF‑8)
    - `valLen` bytes **value**
    - `4B` **checksum** of the payload (32‑bit; see `Checksums.compute`)
- **Address** is a 64‑bit logical pointer: `(segId << 32) | segOffset`.

---

## Outbox Consumption Pattern

A typical consumer loop per partition:

```java
int p = 0;
long saved = db.loadLsn(p);                     // -1 if none
long from  = (saved < 0) ? -1 : db.nextAddressAfter(saved);

db.refreshMappings();
long last = db.scanFrom(p, from, (key, slice, address) -> {
    // deliver slice.value to downstream
    // ...
    return true; // continue
});
db.saveLsn(p, last);
```

**Retention**: After all downstreams have processed up to `inclusiveLsn`, call:
```java
int removed = db.pruneSegmentsUpTo(p, inclusiveLsn);
```
This removes whole segments whose end address ≤ `inclusiveLsn`. For key-level retention (keep only latest versions), run:
```java
db.compactPartition(p);
```

> If you have multiple independent consumers, maintain their **own LSNs** and only prune up to the **minimum** committed LSN across the group.

---

## Benchmark: `OutboxSoak`

Long‑running soak test that shares one `AppendOnlyDB` across a **writer** and a **consumer** thread, with optional background maintenance.

**Common flags**
- `-Ddir=./outbox-soak-data` — data directory
- `-DdurationSec=3600` — total run time
- `-DwarmupSec=10` — warmup (excluded from QPS calc printed by the shutdown hook)
- `-DtargetQps=30000` — target write QPS
- `-Dwindow=4096` — concurrent in‑flight writes (semaphore window)
- `-Dval=128` — payload bytes
- `-DsegBytes=268435456` — segment size (bytes)
- `-Dpartitions=1` — number of partitions
- `-DconsumerPartition=0` — which partition to consume
- `-DdurableAck=true` — use durable ACK writes
- `-DsaveEveryN=50000` — save LSN every N consumed records
- `-DpruneEveryN=200000` — try pruning after N consumed
- `-DpruneSafety=0` — keep extra headroom before the saved LSN (address units)
- `-DcompactEverySec=60` — periodically compact (0 = off)
- `-DlatCap=80000` — latency ring capacity (with down‑sampling to avoid OOM)
- `-DreaderOnly=false` — set `true` to run only the consumer

**Example (Windows)**

```bat
java -Xms2g -Xmx2g -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:+ParallelRefProcEnabled ^
     -XX:MaxDirectMemorySize=1g -XX:+AlwaysPreTouch ^
     -Ddir=./outbox-soak-data -DreaderOnly=false -DdurationSec=3600 -DwarmupSec=10 ^
     -DtargetQps=20000 -Dwindow=4096 -Dval=128 -DsegBytes=268435456 -Dpartitions=1 ^
     -DconsumerPartition=0 -DdurableAck=true -DsaveEveryN=20000 -DpruneEveryN=60000 ^
     -DpruneSafety=0 -DcompactEverySec=20 -DlatCap=80000 ^
     -cp "target/classes;target/test-classes" aodb.OutboxSoak
```

The program logs rolling write/read throughput and latencies (p50/p95/p99 in microseconds). It also prints pruning events:
```
[PRUNE] partition=0 removedSegments=1 < segId 18
```

---

## Tuning Notes

- **Seg size**: 256–1024 MB segments are a good starting point. Larger segments improve write locality; smaller ones make pruning more frequent/cheaper.
- **Batched fsync**: start with `fsyncEveryN=8192`, `fsyncEveryMs=50–100`. Enable `enableAsyncFsync` to decouple fsync from the writer thread.
- **Writer micro‑batch**: `writerBatchMax≈4–8K`, `writerBatchWaitMs=1–2ms` smooths p99 at high QPS.
- **Listener mode**: use `ASYNC_DROP_OLD` for firehose subscriptions (drop oldest on saturation).
- **Partitions**: increase for parallelism if your keys naturally shard. Each partition is an independent append tail.
- **GC**: G1 works well for the default workload; consider `-XX:+AlwaysPreTouch`, `-XX:MaxDirectMemorySize=...` for large direct buffers.
- **Readers**: call `refreshMappings()` before long scans to pick up tail growth.
- **Retention**: prune by LSN frequently to cap disk usage; compact periodically to reduce old versions/tombstones.

---

## Semantics & Limitations

- Single‑writer process (exclusive directory lock). Multiple **read‑only** processes are supported.
- Keys are UTF‑8 strings; values are arbitrary byte arrays; values are immutable (updates append a new version).
- No built‑in TTL: retention/expiry is consumer‑driven via pruning/compaction.
- Addresses are stable within a process lifetime; after compaction/pruning, old addresses may become invalid.
- Crash tolerance: in `BATCHED`, non‑durable writes may be delayed until the next fsync; `putAsyncDurable` gives WAL‑style durability.

---

## License
TBD.
