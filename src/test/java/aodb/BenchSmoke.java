// // FILE: src/test/java/aodb/BenchSmoke.java
// package aodb;

// import java.nio.file.*;
// import java.util.*;
// import java.util.concurrent.*;
// import java.util.concurrent.atomic.*;

// public class BenchSmoke {

//     // ── prop helpers ────────────────────────────────────────────────────────
//     static int    propI(String k, int d){ return Integer.getInteger(k, d); }
//     static long   propL(String k, long d){ return Long.getLong(k, d); }
//     static String propS(String k, String d){ String v=System.getProperty(k); return v==null?d:v; }
//     static boolean propB(String k, boolean d){ String v=System.getProperty(k); return v==null?d:Boolean.parseBoolean(v); }

//     static long pct(List<Long> sorted, double p){
//         if (sorted.isEmpty()) return 0;
//         int idx = (int)Math.floor((sorted.size()-1) * p);
//         if (idx < 0) idx = 0;
//         if (idx >= sorted.size()) idx = sorted.size()-1;
//         return sorted.get(idx);
//     }

//     public static void main(String[] args) throws Exception {
//         Path dir = Paths.get(propS("dir","./bench-data"));

//         var dur   = AppendOnlyDB.Durability.valueOf(propS("dur","BATCHED"));
//         var lmode = AppendOnlyDB.ListenerMode.valueOf(propS("lmode","ASYNC_DROP_OLD"));

//         // Flags (for printing)
//         boolean noCrc   = propB("aodb.noCrc", false);
//         boolean crc32c  = propB("aodb.crc32c", true);
//         int batchBufMB  = propI("aodb.batchBufMB", 4);
//         int writerPrio  = propI("aodb.writerPrio", Thread.NORM_PRIORITY + 1);

//         // AODB Config
//         AppendOnlyDB.Config cfg = new AppendOnlyDB.Config(
//             dir,
//             propI("segBytes", 1<<30),
//             propI("fsyncN", 8192),
//             propL("fsyncMs", 100),
//             false,
//             propI("partitions", 1),
//             false,
//             dur,
//             true,
//             propI("wq", 1<<18),
//             propI("batchMax", 8192),
//             propL("batchWaitMs", 5),
//             propL("offerMs", 2000),
//             lmode,
//             propI("lthreads", 2),
//             propI("lq", 1<<16),
//             true,
//             true    // enableAsyncFsync
//         );

//         final int TOTAL   = propI("total", 200_000);
//         final int WINDOW  = propI("window", 8192);
//         final int VAL_SZ  = propI("val", 128);
//         final boolean CSV = propB("csv", false);
//         final String  CSV_PATH = propS("csvPath", "bench.csv");

//         final byte[] payload = new byte[VAL_SZ];
//         Arrays.fill(payload, (byte)'x');

//         try (AppendOnlyDB db = new AppendOnlyDB(cfg)) {
//             var sem       = new Semaphore(WINDOW);
//             var done      = new AtomicLong();
//             var latencies = new ConcurrentLinkedQueue<Long>();

//             long t0 = System.nanoTime();
//             for (int i = 0; i < TOTAL; i++) {
//                 sem.acquireUninterruptibly();
//                 long start = System.nanoTime();
//                 String key = "k" + i;
//                 db.putAsync(key, payload, System.currentTimeMillis())
//                   .whenComplete((r, ex) -> {
//                       long end = System.nanoTime();
//                       latencies.add(ex == null ? (end - start) : Long.MAX_VALUE/2);
//                       done.incrementAndGet();
//                       sem.release();
//                   });
//             }
//             while (done.get() < TOTAL) Thread.sleep(5);
//             long t1 = System.nanoTime();

//             var list = new ArrayList<Long>(latencies);
//             Collections.sort(list);
//             double secs = Math.max(1e-9, (t1 - t0) / 1e9);
//             long p50 = pct(list, 0.50), p95 = pct(list, 0.95), p99 = pct(list, 0.99);
//             double qps = TOTAL / secs;

//             System.out.printf(Locale.US,
//                 "TOTAL=%d, VAL=%dB, DUR=%s, WINDOW=%d, batchMax=%d, batchWaitMs=%d, fsyncN=%d, fsyncMs=%d%n",
//                 TOTAL, VAL_SZ, dur, WINDOW,
//                 propI("batchMax",8192), propL("batchWaitMs",5), propI("fsyncN",8192), propL("fsyncMs",100));
//             System.out.printf(Locale.US,
//                 "FLAGS: noCrc=%s, crc32c=%s, batchBufMB=%d, writerPrio=%d%n",
//                 String.valueOf(noCrc), String.valueOf(crc32c), batchBufMB, writerPrio);
//             System.out.printf(Locale.US, "THROUGHPUT: %.1f ops/s%n", qps);
//             System.out.printf(Locale.US, "LATENCY (us): p50=%.1f, p95=%.1f, p99=%.1f%n",
//                 p50/1e3, p95/1e3, p99/1e3);

//             if (CSV) {
//                 String header = "ts,total,valB,dur,window,batchMax,batchWaitMs,fsyncN,fsyncMs,qps,p50us,p95us,p99us,noCrc,crc32c,batchBufMB,writerPrio\n";
//                 String row = String.format(Locale.US,
//                     "%d,%d,%d,%s,%d,%d,%d,%d,%d,%.3f,%.1f,%.1f,%.1f,%s,%s,%d,%d\n",
//                     System.currentTimeMillis(), TOTAL, VAL_SZ, dur, WINDOW,
//                     propI("batchMax",8192), propL("batchWaitMs",5), propI("fsyncN",8192), propL("fsyncMs",100),
//                     qps, p50/1e3, p95/1e3, p99/1e3, String.valueOf(noCrc), String.valueOf(crc32c), batchBufMB, writerPrio);
//                 Path p = Paths.get(CSV_PATH);
//                 if (!Files.exists(p)) Files.writeString(p, header);
//                 Files.writeString(p, row, StandardOpenOption.APPEND, StandardOpenOption.CREATE);
//                 System.out.println("CSV >> " + p.toAbsolutePath());
//             }
//         }
//     }
// }
