// package aodb;

// import java.lang.reflect.Method;
// import java.nio.file.*;
// import java.util.Arrays;
// import java.util.Locale;
// import java.util.concurrent.*;
// import java.util.concurrent.atomic.*;
// import java.util.concurrent.locks.LockSupport;

// public class SoakTTL {

//     static int    propI(String k, int d){ return Integer.getInteger(k, d); }
//     static long   propL(String k, long d){ return Long.getLong(k, d); }
//     static String propS(String k, String d){ String v=System.getProperty(k); return v==null?d:v; }
//     static boolean propB(String k, boolean d){ String v=System.getProperty(k); return v==null?d:Boolean.parseBoolean(v); }

//     // 고정 크기 링버퍼(2의 거듭제곱 권장)
//     static final class LongRing {
//         final long[] buf; final int mask; final AtomicLong idx = new AtomicLong();
//         LongRing(int capPow2) {
//             int cap = 1; while (cap < capPow2) cap <<= 1;
//             this.buf = new long[cap]; this.mask = cap - 1;
//         }
//         void add(long v) { long i = idx.getAndIncrement(); buf[(int)(i & mask)] = v; }
//         long size() { long n = idx.get(); return Math.min(n, (long)buf.length); }
//     }

//     static long pctFromSorted(long[] sorted, double p){
//         if (sorted.length == 0) return 0;
//         int idx = (int)Math.floor((sorted.length-1) * p);
//         if (idx < 0) idx = 0; if (idx >= sorted.length) idx = sorted.length-1;
//         return sorted[idx];
//     }

//     static void pace(int targetQps, long t0, long started){
//         if (targetQps <= 0) return;
//         long perOp = 1_000_000_000L / Math.max(1, targetQps);
//         long target = t0 + started * perOp;
//         long now;
//         while ((now = System.nanoTime()) < target) {
//             long ns = target - now;
//             if (ns > 2_000_000L) LockSupport.parkNanos(Math.min(ns, 5_000_000L));
//             else Thread.onSpinWait();
//         }
//     }

//     // AppendOnlyDB.putAsyncTTL 리플렉션 핸들(있으면 사용)
//     static Method findPutAsyncTTL() {
//         try {
//             return AppendOnlyDB.class.getMethod("putAsyncTTL", String.class, byte[].class, long.class, long.class);
//         } catch (NoSuchMethodException e) {
//             System.out.println("[INFO] AppendOnlyDB.putAsyncTTL 미탑재 → tombstone 삭제로 TTL 적용");
//             return null;
//         }
//     }

//     public static void main(String[] args) throws Exception {
//         final int durationSec   = propI("durationSec", 300);
//         final int warmupSec     = propI("warmupSec", 10);
//         final int targetQps     = propI("targetQps", 30000);
//         final int tickMs        = propI("tickMs", 100);
//         final int readsPerTick  = propI("readsPerTick", 2000);
//         final int readers       = Math.max(1, propI("readers", 1));
//         final long ttlMs        = propL("ttlMs", 60_000);
//         final boolean useDbTTL  = propB("useDbTTL", false);
//         final String sweeper    = propS("sweeper", "delete");    // delete|compact|none
//         final long sweepEveryMs = propL("sweepEveryMs", 1000);
//         final int  sweepBatch   = propI("sweepBatch", 30000);
//         final int  safetyWindowSec = propI("safetyWindowSec", 5);
//         final boolean e2e       = propB("e2e", false);
//         final boolean csv       = propB("csv", true);
//         final String  csvPath   = propS("csvPath", "soak_ttl.csv");

//         // ★ 지표 집계 파라미터: 메모리 피크 제한
//         final int latCap        = Math.max(8192, propI("latCap", 200_000));   // 링 버퍼 자체 용량(최대 기록 수)
//         final int latSample     = Math.max(2048, propI("latSample", 32_768)); // 정렬/백분위 계산에 사용할 표본 상한

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
//         final int valSz         = propI("val", 128);

//         final byte[] payload = new byte[valSz];
//         Arrays.fill(payload, (byte)'x');

//         AppendOnlyDB.Config cfg = new AppendOnlyDB.Config(
//             dir, segBytes, fsyncN, fsyncMs, false,
//             partitions, false, dur,
//             true, wq, batchMax, batchWaitMs, offerMs,
//             lmode, lthreads, lq,
//             true,  // enableUnmap
//             true,  // enableAsyncFsync
//             true   // dropIndexOnTombstone (있다면)
//         );

//         System.out.printf(Locale.US,
//             "== SoakTTL == duration=%ds, warmup=%ds, targetQps=%d, tickMs=%d, reads/tick=%d, readers=%d, ttlMs=%d, sweeper=%s, useDbTTL=%s%n",
//             durationSec, warmupSec, targetQps, tickMs, readsPerTick, readers, ttlMs, sweeper, useDbTTL);
//         System.out.printf(Locale.US,
//             "VAL=%dB, DUR=%s, WINDOW=%d, batchMax=%d, batchWaitMs=%d, fsyncN=%d, fsyncMs=%d, latCap=%d, latSample=%d, e2e=%s, durableAck=%s%n",
//             valSz, dur, window, batchMax, batchWaitMs, fsyncN, fsyncMs, latCap, latSample, String.valueOf(e2e), "false");

//         final Method putAsyncTTL = (useDbTTL ? findPutAsyncTTL() : null);

//         final Semaphore inflight = new Semaphore(window);
//         final LongAdder writesDone = new LongAdder();
//         final LongAdder readsDone  = new LongAdder();
//         final LongAdder hits       = new LongAdder();

//         final AtomicLong maxId = new AtomicLong(-1);

//         final LongRing wLat = new LongRing(latCap);
//         final LongRing rLat = new LongRing(latCap);

//         final double alpha = 0.20;
//         final AtomicReference<Double> ewmaQps = new AtomicReference<>(0.0);
//         final AtomicLong lastRateT = new AtomicLong(System.nanoTime());
//         final AtomicLong lastRateId = new AtomicLong(-1);

//         final AtomicLong expireCursor = new AtomicLong(-1);
//         final AtomicLong safetyLagIds = new AtomicLong(0);

//         try (AppendOnlyDB db = new AppendOnlyDB(cfg)) {

//             // --- writer ---
//             Thread writer = new Thread(() -> {
//                 long epoch = System.nanoTime();
//                 final boolean measureE2E = e2e;
//                 while (!Thread.currentThread().isInterrupted()) {
//                     inflight.acquireUninterruptibly();

//                     long id = maxId.incrementAndGet();
//                     String key = "k" + id;
//                     long s = System.nanoTime();
//                     try {
//                         if (putAsyncTTL != null) {
//                             if (measureE2E) {
//                                 @SuppressWarnings("unchecked")
//                                 CompletableFuture<Long> fut =
//                                     (CompletableFuture<Long>) putAsyncTTL.invoke(db, key, payload, System.currentTimeMillis(), ttlMs);
//                                 fut.whenComplete((r, ex) -> {
//                                     long e = System.nanoTime();
//                                     wLat.add(e - s);
//                                     writesDone.increment();
//                                     inflight.release();
//                                 });
//                                 continue;
//                             } else {
//                                 putAsyncTTL.invoke(db, key, payload, System.currentTimeMillis(), ttlMs);
//                             }
//                         } else {
//                             if (measureE2E) {
//                                 db.putAsync(key, payload, System.currentTimeMillis())
//                                   .whenComplete((r, ex) -> {
//                                       long e = System.nanoTime();
//                                       wLat.add(e - s);
//                                       writesDone.increment();
//                                       inflight.release();
//                                   });
//                                 continue;
//                             } else {
//                                 db.putAsync(key, payload, System.currentTimeMillis());
//                             }
//                         }
//                     } catch (Throwable ignore) {
//                         wLat.add(Long.MAX_VALUE/2);
//                         inflight.release();
//                         continue;
//                     }

//                     if (!measureE2E) {
//                         long e = System.nanoTime();
//                         wLat.add(e - s);
//                         writesDone.increment();
//                         inflight.release();
//                     }

//                     long started = id + 1;
//                     pace(targetQps, epoch, started);

//                     long nowN = System.nanoTime();
//                     long prevT = lastRateT.get();
//                     if (nowN - prevT >= 250_000_000L) {
//                         long prevId = lastRateId.getAndSet(id);
//                         lastRateT.set(nowN);
//                         if (prevId >= 0) {
//                             double dt = (nowN - prevT) / 1e9;
//                             double inst = (id - prevId) / Math.max(1e-9, dt);
//                             double cur = ewmaQps.get();
//                             double next = (cur == 0.0) ? inst : cur * (1 - alpha) + inst * alpha;
//                             ewmaQps.set(next);
//                             safetyLagIds.set( (long)(safetyWindowSec * Math.max(1.0, next)) );
//                         }
//                     }
//                 }
//             }, "soak-writer");
//             writer.setDaemon(true);

//             // --- readers ---
//             Thread[] readerTh = new Thread[readers];
//             for (int ri = 0; ri < readers; ri++) {
//                 readerTh[ri] = new Thread(() -> {
//                     final ThreadLocalRandom rnd = ThreadLocalRandom.current();
//                     int perTick = Math.max(1, readsPerTick / readers);
//                     while (!Thread.currentThread().isInterrupted()) {
//                         long curMax = maxId.get();
//                         if (curMax >= 0) {
//                             for (int i = 0; i < perTick; i++) {
//                                 long id = rnd.nextLong(curMax + 1);
//                                 String k = "k" + id;
//                                 long s = System.nanoTime();
//                                 try {
//                                     var v = db.getLatest(k);
//                                     long e = System.nanoTime();
//                                     rLat.add(e - s);
//                                     if (v != null && v.bytes != null) hits.increment();
//                                     readsDone.increment();
//                                 } catch (Throwable t) {
//                                     long e = System.nanoTime();
//                                     rLat.add(e - s);
//                                     readsDone.increment();
//                                 }
//                             }
//                         }
//                         try { Thread.sleep(Math.max(1, tickMs)); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
//                     }
//                 }, "soak-reader-" + ri);
//                 readerTh[ri].setDaemon(true);
//             }

//             // --- sweeper ---
//             Thread sweeperTh = new Thread(() -> {
//                 if ("none".equalsIgnoreCase(sweeper)) return;
//                 long nextAt = System.currentTimeMillis();
//                 int partRoundRobin = 0;
//                 while (!Thread.currentThread().isInterrupted()) {
//                     long now = System.currentTimeMillis();
//                     if (now < nextAt) {
//                         try { Thread.sleep(Math.max(1, nextAt - now)); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
//                         continue;
//                     }
//                     nextAt = now + Math.max(1, sweepEveryMs);

//                     try {
//                         if (putAsyncTTL != null) {
//                             if ("compact".equalsIgnoreCase(sweeper)) {
//                                 db.compactPartition(partRoundRobin % partitions);
//                                 partRoundRobin++;
//                             }
//                             continue;
//                         }

//                         double rate = Math.max(1.0, ewmaQps.get());
//                         long lag = safetyLagIds.get();
//                         long curMax = maxId.get();
//                         long ttlLagIds = (long)Math.floor((ttlMs / 1000.0) * rate);
//                         long expireTarget = curMax - ttlLagIds - lag;
//                         if (expireTarget < 0) expireTarget = -1;

//                         long from = expireCursor.get() + 1;
//                         long to   = expireTarget;

//                         if (to >= from) {
//                             int budget = sweepBatch;
//                             long issued = 0;
//                             for (long id = from; id <= to && issued < budget; id++) {
//                                 try {
//                                     db.deleteAsync("k"+id, now);
//                                     issued++;
//                                 } catch (Throwable ignore) {}
//                             }
//                             if (issued > 0) {
//                                 long newCursor = from + issued - 1;
//                                 expireCursor.set(newCursor);
//                                 System.out.printf("[SWEEP] deleted=%d expireCursor=%d%n", issued, newCursor);
//                             }
//                         }
//                     } catch (Throwable ignore) {}
//                 }
//             }, "soak-sweeper");
//             sweeperTh.setDaemon(true);

//             writer.start();
//             for (Thread t : readerTh) t.start();

//             Thread.sleep(Math.max(0, warmupSec) * 1000L);

//             long t0 = System.nanoTime();
//             long lastLog = t0;

//             while ((System.nanoTime() - t0) < durationSec * 1_000_000_000L) {
//                 if (!sweeperTh.isAlive()) sweeperTh.start();
//                 long now = System.nanoTime();
//                 if (now - lastLog >= 10_000_000_000L) {
//                     log10s(t0, wLat, rLat, writesDone, readsDone, hits, maxId, expireCursor, ewmaQps, latSample);
//                     lastLog = now;
//                 }
//                 Thread.sleep(200);
//             }

//             writer.interrupt();
//             for (Thread t : readerTh) t.interrupt();
//             sweeperTh.interrupt();
//             writer.join(1000);
//             for (Thread t : readerTh) t.join(1000);
//             sweeperTh.join(1000);

//             double[] w = percentilesUs(wLat, latSample);
//             double[] r = percentilesUs(rLat, latSample);
//             double secs = Math.max(1e-9, (System.nanoTime() - t0)/1e9);
//             double wqps = writesDone.sum() / secs;
//             double rqps = readsDone.sum() / secs;
//             double hitPct = (readsDone.sum() == 0) ? 0.0 : (hits.sum() * 100.0 / readsDone.sum());

//             System.out.println("== RESULT ==");
//             System.out.printf(Locale.US, "W-THROUGHPUT: %.1f ops/s, LAT(us) p50=%.1f p95=%.1f p99=%.1f%n",
//                 wqps, w[0], w[1], w[2]);
//             System.out.printf(Locale.US, "R-THROUGHPUT: %.1f ops/s, LAT(us) p50=%.1f p95=%.1f p99=%.1f, HIT=%.1f%%%n",
//                 rqps, r[0], r[1], r[2], hitPct);

//             if (csv) {
//                 String header = "ts,durationSec,warmupSec,targetQps,tickMs,readsPerTick,readers,ttlMs,useDbTTL,sweeper,"
//                               + "sweepEveryMs,sweepBatch,safetyWindowSec,valB,dur,window,batchMax,batchWaitMs,fsyncN,fsyncMs,"
//                               + "wops,rops,wp50us,wp95us,wp99us,rp50us,rp95us,rp99us,hitPct\n";
//                 String row = String.format(Locale.US,
//                     "%d,%d,%d,%d,%d,%d,%d,%d,%s,%s,%d,%d,%d,%d,%s,%d,%d,%d,%d,%d,%.3f,%.3f,%.1f,%.1f,%.1f,%.1f,%.1f,%.1f,%.1f%n",
//                     System.currentTimeMillis(),
//                     durationSec, warmupSec, targetQps, tickMs, readsPerTick, readers,
//                     ttlMs,
//                     String.valueOf(useDbTTL),
//                     sweeper,
//                     sweepEveryMs, sweepBatch, safetyWindowSec,
//                     valSz, dur.name(),
//                     window, batchMax, batchWaitMs, fsyncN, fsyncMs,
//                     wqps, rqps,
//                     w[0], w[1], w[2],
//                     r[0], r[1], r[2],
//                     hitPct
//                 );
//                 try {
//                     Path p = Paths.get(csvPath);
//                     if (!Files.exists(p)) Files.writeString(p, header);
//                     Files.writeString(p, row, StandardOpenOption.APPEND, StandardOpenOption.CREATE);
//                     System.out.println("CSV >> " + p.toAbsolutePath());
//                 } catch (Throwable t) {
//                     System.err.println("[WARN] CSV write failed: " + t);
//                 }
//             }
//         }
//     }

//     static void log10s(long startNanos,
//                        LongRing wLat, LongRing rLat,
//                        LongAdder wDone, LongAdder rDone, LongAdder hits,
//                        AtomicLong maxId, AtomicLong expireCursor,
//                        AtomicReference<Double> ewmaQps,
//                        int latSample) {
//         double[] w = percentilesUs(wLat, latSample);
//         double[] r = percentilesUs(rLat, latSample);
//         long reads = rDone.sum();
//         double hitPct = (reads == 0) ? 0.0 : (hits.sum() * 100.0 / reads);
//         double secs = (System.nanoTime() - startNanos) / 1e9;
//         System.out.printf(Locale.US,
//             "[%3.0fs] writes=%d, reads=%d, hit=%.1f%%, maxId=%d, expireCursor=%d, ewmaQps=%.0f, Wp50=%.1fus Wp99=%.1fus Rp50=%.1fus Rp99=%.1fus%n",
//             secs, wDone.sum(), reads, hitPct, maxId.get(), expireCursor.get(), ewmaQps.get(),
//             w[0], w[2], r[0], r[2]);
//     }

//     // ★ 메모리 안전한 백분위 계산: 표본 제한 + 정렬
//     static double[] percentilesUs(LongRing ring, int maxSamples) {
//         long[] snap = sampleSnapshot(ring, maxSamples);
//         Arrays.sort(snap);
//         long p50 = pctFromSorted(snap, 0.50);
//         long p95 = pctFromSorted(snap, 0.95);
//         long p99 = pctFromSorted(snap, 0.99);
//         return new double[]{ p50/1e3, p95/1e3, p99/1e3 };
//     }

//     // 균일 간격 샘플링(복사 크기를 maxSamples로 제한)
//     static long[] sampleSnapshot(LongRing r, int maxSamples) {
//         long n = r.size();
//         if (n <= 0) return new long[0];
//         int m = (int)Math.min(n, (long)maxSamples);
//         long[] out = new long[m];

//         long end = r.idx.get();
//         long start = Math.max(0, end - n);

//         if (n <= m) {
//             for (int i = 0; i < (int)n; i++) {
//                 out[i] = r.buf[(int)((start + i) & r.mask)];
//             }
//         } else {
//             double stride = (double)n / m;
//             for (int i = 0; i < m; i++) {
//                 long off = (long)Math.floor(i * stride);
//                 out[i] = r.buf[(int)((start + off) & r.mask)];
//             }
//         }
//         return out;
//     }
// }
