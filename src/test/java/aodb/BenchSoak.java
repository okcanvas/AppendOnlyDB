// package aodb;

// import java.nio.file.*;
// import java.util.*;
// import java.util.concurrent.*;
// import java.util.concurrent.atomic.*;
// import java.util.concurrent.locks.LockSupport;

// public class BenchSoak {

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
//         // === 실행 파라미터 ===
//         final int durationSec   = propI("durationSec", 300);     // 총 측정 시간(초) ← 5분 기본
//         final int warmupSec     = propI("warmupSec", 10);        // 워밍업(초)
//         final int targetQps     = propI("targetQps", 50000);     // 0 이면 무제한, 기본 50k QPS 권장
//         final int workingSet    = propI("workingSet", 200_000);  // 키 개수(랜덤 업데이트)
//         final boolean csv       = propB("csv", true);
//         final String csvPath    = propS("csvPath", "bench_soak.csv");

//         final Path dir          = Paths.get(propS("dir","./bench-data-soak"));
//         final int segBytes      = propI("segBytes", 1<<30);
//         final int fsyncN        = propI("fsyncN", 8192);
//         final long fsyncMs      = propL("fsyncMs", 100);
//         final int partitions    = propI("partitions", 1);
//         final AppendOnlyDB.Durability dur = AppendOnlyDB.Durability.valueOf(propS("dur", "RELAXED"));
//         final AppendOnlyDB.ListenerMode lmode = AppendOnlyDB.ListenerMode.valueOf(propS("lmode", "ASYNC_DROP_OLD"));

//         final int wq            = propI("wq", 1<<18);
//         final int batchMax      = propI("batchMax", 8192);
//         final long batchWaitMs  = propL("batchWaitMs", 1);
//         final long offerMs      = propL("offerMs", 2000);
//         final int lthreads      = propI("lthreads", 2);
//         final int lq            = propI("lq", 1<<16);
//         final int window        = propI("window", 2048);
//         final int valSz         = propI("val", 64);

//         final byte[] payload = new byte[valSz];
//         Arrays.fill(payload, (byte)'x');

//         // AODB Config (현재 프로젝트의 AppendOnlyDB.Config 시그니처를 그대로 사용)
//         AppendOnlyDB.Config cfg = new AppendOnlyDB.Config(
//             dir,
//             segBytes,
//             fsyncN,
//             fsyncMs,
//             false,
//             partitions,
//             false,
//             dur,
//             true,
//             wq,
//             batchMax,
//             batchWaitMs,
//             offerMs,
//             lmode,
//             lthreads,
//             lq,
//             true,   // enableUnmap
//             true    // enableAsyncFsync (있는 오버로드면 사용, 없으면 컴파일러가 다른 오버로드 선택)
//         );

//         System.out.printf(Locale.US,
//             "== AODB BenchSoak == duration=%ds, warmup=%ds, targetQps=%s, workingSet=%d%n",
//             durationSec, warmupSec, (targetQps==0?"unlimited":(""+targetQps)), workingSet);
//         System.out.printf(Locale.US,
//             "VAL=%dB, DUR=%s, WINDOW=%d, batchMax=%d, batchWaitMs=%d, fsyncN=%d, fsyncMs=%d%n",
//             valSz, dur, window, batchMax, batchWaitMs, fsyncN, fsyncMs);

//         final ThreadLocalRandom rnd = ThreadLocalRandom.current();
//         final Semaphore sem = new Semaphore(window);
//         final AtomicLong issued = new AtomicLong();
//         final AtomicLong done = new AtomicLong();
//         final ConcurrentLinkedQueue<Long> latencies = new ConcurrentLinkedQueue<>();

//         try (AppendOnlyDB db = new AppendOnlyDB(cfg)) {
//             // 워밍업
//             long w0 = System.nanoTime();
//             long wEnd = w0 + warmupSec * 1_000_000_000L;
//             while (System.nanoTime() < wEnd) {
//                 sem.acquireUninterruptibly();
//                 String key = "k" + rnd.nextInt(workingSet);
//                 long s = System.nanoTime();
//                 db.putAsync(key, payload, System.currentTimeMillis())
//                   .whenComplete((r, ex) -> {
//                       long e = System.nanoTime();
//                       latencies.add(ex==null ? (e-s) : Long.MAX_VALUE/2);
//                       done.incrementAndGet();
//                       sem.release();
//                   });
//                 issued.incrementAndGet();
//                 pace(targetQps, w0, issued.get());
//             }
//             // 워밍업 drain
//             while (sem.availablePermits() < window) Thread.sleep(1);
//             latencies.clear();
//             issued.set(0);
//             done.set(0);

//             // 본 측정
//             long t0 = System.nanoTime();
//             long tend = t0 + durationSec * 1_000_000_000L;
//             long lastPrint = t0;
//             while (System.nanoTime() < tend) {
//                 sem.acquireUninterruptibly();
//                 String key = "k" + rnd.nextInt(workingSet);
//                 long s = System.nanoTime();
//                 db.putAsync(key, payload, System.currentTimeMillis())
//                   .whenComplete((r, ex) -> {
//                       long e = System.nanoTime();
//                       latencies.add(ex==null ? (e-s) : Long.MAX_VALUE/2);
//                       done.incrementAndGet();
//                       sem.release();
//                   });
//                 long i = issued.incrementAndGet();
//                 pace(targetQps, t0, i);

//                 long now = System.nanoTime();
//                 if (now - lastPrint >= 30_000_000_000L) { // 30s 마다 진행상황
//                     double secs = (now - t0)/1e9;
//                     double qps = done.get()/secs;
//                     System.out.printf(Locale.US, "[%3.0fs] so far: done=%d, qps=%.0f%n", secs, done.get(), qps);
//                     lastPrint = now;
//                 }
//             }
//             // drain
//             while (sem.availablePermits() < window) Thread.sleep(5);
//             long t1 = System.nanoTime();

//             // 결과
//             var list = new ArrayList<Long>(latencies);
//             Collections.sort(list);
//             double secs = Math.max(1e-9, (t1 - t0)/1e9);
//             long p50 = pct(list, 0.50), p95 = pct(list, 0.95), p99 = pct(list, 0.99);
//             double qps = done.get() / secs;

//             System.out.printf(Locale.US, "== RESULT == duration=%.1fs, ops=%d%n", secs, done.get());
//             System.out.printf(Locale.US, "THROUGHPUT: %.1f ops/s%n", qps);
//             System.out.printf(Locale.US, "LATENCY (us): p50=%.1f, p95=%.1f, p99=%.1f%n",
//                 p50/1e3, p95/1e3, p99/1e3);

//             if (csv) {
//                 String header = "ts,durationSec,warmupSec,targetQps,workingSet,valB,dur,window,batchMax,batchWaitMs,fsyncN,fsyncMs,ops,qps,p50us,p95us,p99us\n";
//                 String row = String.format(Locale.US,
//                     "%d,%d,%d,%d,%d,%d,%s,%d,%d,%d,%d,%d,%d,%.3f,%.1f,%.1f,%.1f\n",
//                     System.currentTimeMillis(), durationSec, warmupSec, targetQps, workingSet, valSz, dur.name(),
//                     window, batchMax, batchWaitMs, fsyncN, fsyncMs, done.get(), qps, p50/1e3, p95/1e3, p99/1e3);
//                 Path p = Paths.get(csvPath);
//                 if (!Files.exists(p)) Files.writeString(p, header);
//                 Files.writeString(p, row, StandardOpenOption.APPEND, StandardOpenOption.CREATE);
//                 System.out.println("CSV >> " + p.toAbsolutePath());
//             }
//         }
//     }

//     // 간단한 발행 속도 제어(목표 QPS)
//     static void pace(int targetQps, long t0, long started){
//         if (targetQps <= 0) return;
//         long perOp = 1_000_000_000L / Math.max(1, targetQps);
//         long target = t0 + started * perOp;
//         long now;
//         while ((now = System.nanoTime()) < target) {
//             long ns = target - now;
//             if (ns > 2_000_000L) { // 2ms 이상이면 park
//                 LockSupport.parkNanos(Math.min(ns, 5_000_000L));
//             } else {
//                 Thread.onSpinWait();
//             }
//         }
//     }
// }
