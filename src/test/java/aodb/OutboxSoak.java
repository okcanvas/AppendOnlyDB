package aodb;

import java.nio.file.*;
import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.LockSupport;

public class OutboxSoak {

    // ── sysprop helpers ─────────────────────────────────────────────────────
    static int    propI(String k, int d){ return Integer.getInteger(k, d); }
    static long   propL(String k, long d){ return Long.getLong(k, d); }
    static String propS(String k, String d){ String v=System.getProperty(k); return v==null?d:v; }
    static boolean propB(String k, boolean d){ String v=System.getProperty(k); return v==null?d:Boolean.parseBoolean(v); }

    // ── fixed-size ring (latency snapshots) ─────────────────────────────────
    static final class LongRing {
        final long[] buf; final int mask; final AtomicLong idx = new AtomicLong();
        LongRing(int capPow2){
            int cap = 1; while (cap < capPow2) cap <<= 1;
            buf = new long[cap]; mask = cap - 1;
        }
        void add(long v){ long i = idx.getAndIncrement(); buf[(int)(i & mask)] = v; }
        long[] snapshot(){
            long end = idx.get();                 // capture once
            long n = Math.min(end, (long)buf.length);
            long start = Math.max(0, end - n);
            long[] a = new long[(int)n];
            for (long i=0;i<n;i++) a[(int)i] = buf[(int)((start + i) & mask)];
            return a;
        }
    }
    static long pctFromSorted(long[] s, double p){
        if (s.length==0) return 0;
        int i = (int)Math.floor((s.length-1)*p);
        if (i<0) i=0; if (i>=s.length) i=s.length-1;
        return s[i];
    }
    // OOM 방지용 다운샘플링(최대 16k개만 정렬)
    static double[] pctUs(LongRing r){
        long[] a = r.snapshot();
        if (a.length > 16384) {
            long[] sample = new long[16384];
            double step = (double)a.length / sample.length;
            for (int i=0;i<sample.length;i++) sample[i] = a[(int)Math.floor(i*step)];
            a = sample;
        }
        Arrays.sort(a);
        return new double[]{ pctFromSorted(a,0.50)/1e3, pctFromSorted(a,0.95)/1e3, pctFromSorted(a,0.99)/1e3 };
    }
    static void paceQpsBatched(final long perOpNs, final long startEpochNs, final long opsDone, final int batch){
        if (perOpNs <= 0) return;
        if ((opsDone % batch) != 0) return;
        long target = startEpochNs + opsDone * perOpNs;
        long now = System.nanoTime();
        long ns = target - now;
        if (ns > 50_000L) LockSupport.parkNanos(Math.min(ns, 5_000_000L));
    }

    // ── main ────────────────────────────────────────────────────────────────
    public static void main(String[] args) throws Exception {
        // === Parameters ===
        final Path dir         = Paths.get(propS("dir","./outbox-soak-data")).toAbsolutePath().normalize();
        final int durationSec  = propI("durationSec", 3600);
        final int warmupSec    = propI("warmupSec", 10);
        final int targetQps    = propI("targetQps", 30000);
        final int win          = propI("window", 4096);
        final int valSz        = propI("val", 128);
        final int segBytes     = propI("segBytes", 256*1024*1024);
        final int latCap       = propI("latCap", 200_000);
        final boolean durable  = propB("durableAck", true);
        final boolean csv      = propB("csv", true);
        final String csvPath   = propS("csvPath", "outbox_soak.csv");
        final int partitions   = propI("partitions", 1);
        final int consumerP    = propI("consumerPartition", 0);
        final int saveEveryN   = propI("saveEveryN", 50_000);
        final int pruneEveryN  = propI("pruneEveryN", 200_000);
        final int logEverySec  = propI("logEverySec", 10);
        final int killAfterSec = propI("killAfterSec", 0);
        final boolean readerOnly = propB("readerOnly", false);

        // 유지보수 옵션
        final long pruneSafety  = propL("pruneSafety", 0L);     // LSN에서 안전 여유
        final int  compactEvery = propI("compactEverySec", 0);  // 주기적 컴팩션(0=off)

        System.out.printf(Locale.US,
            "== OutboxSoak == dir=%s duration=%ds warmup=%ds qps=%d val=%dB seg=%dMB partitions=%d consumerP=%d durable=%s%n",
            dir, durationSec, warmupSec, targetQps, valSz, segBytes/(1024*1024), partitions, consumerP, String.valueOf(durable));

        // === payload ===
        final byte[] payload = new byte[valSz];
        Arrays.fill(payload, (byte)'x');

        // === Configs === (AppendOnlyDB.Config 시그니처에 맞춤: long 인자에 L)
        final AppendOnlyDB.Config wcfg = new AppendOnlyDB.Config(
            dir, segBytes, 8192, 100L, false,
            partitions, false, AppendOnlyDB.Durability.BATCHED,
            true, 1<<18, 8192, 1L, 2000L,
            AppendOnlyDB.ListenerMode.ASYNC_DROP_OLD, 1, 1<<16,
            true, /*enableAsyncFsync*/ false
        );

        final Semaphore inflight = new Semaphore(win);
        final LongAdder wDone = new LongAdder();
        final LongAdder rDone = new LongAdder();
        final AtomicLong wFail = new AtomicLong();
        final AtomicLong rFail = new AtomicLong();
        final LongRing wLat = new LongRing(latCap);
        final LongRing rLat = new LongRing(latCap);

        final AtomicBoolean running = new AtomicBoolean(true);
        final AtomicLong t0Ref = new AtomicLong(0L); // warmup 이후 측정 시작 시각(ns)

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                long t0 = t0Ref.get();
                long now = System.nanoTime();
                double secs = (t0>0) ? Math.max(1e-9, (now - t0)/1e9) : 0d;
                double wqps = (secs>0)? (wDone.sum() / secs) : 0d;
                double rqps = (secs>0)? (rDone.sum() / secs) : 0d;
                double[] w = pctUs(wLat);
                double[] r = pctUs(rLat);
                System.out.println("\n== SHUTDOWN SUMMARY ==");
                System.out.printf(Locale.US, "W-THROUGHPUT: %.1f ops/s (fail=%d)\n", wqps, wFail.get());
                System.out.printf(Locale.US, "R-THROUGHPUT: %.1f ops/s (fail=%d)\n", rqps, rFail.get());
                System.out.printf(Locale.US, "W LAT(us) p50=%.1f p95=%.1f p99=%.1f; R LAT(us) p50=%.1f p95=%.1f p99=%.1f\n",
                        w[0], w[1], w[2], r[0], r[1], r[2]);
                if (csv && t0>0) {
                    writeCsv(csvPath, System.currentTimeMillis(), durationSec, warmupSec, targetQps, win, valSz,
                            segBytes/(1024*1024), partitions, consumerP, durable, wqps, rqps, w, r);
                }
            } catch (Throwable ignore) {}
        }, "aodb-shutdown"));

        // === AppendOnlyDB 한 번만 오픈 (공유) ===
        final AppendOnlyDB db = new AppendOnlyDB(wcfg);

        // === Writer ===
        Thread writer = null;
        if (!readerOnly) {
            writer = new Thread(() -> {
                try {
                    final long perOpNs = (targetQps>0) ? (1_000_000_000L / Math.max(1, targetQps)) : 0L;
                    final int paceBatch = 1024;
                    long epoch = System.nanoTime();
                    long id = 0;
                    while (running.get()) {
                        inflight.acquireUninterruptibly();
                        final long s = System.nanoTime();
                        try {
                            CompletableFuture<?> fut = (durable)
                                ? db.putAsyncDurable("k"+id, payload, System.currentTimeMillis())
                                : db.putAsync       ("k"+id, payload, System.currentTimeMillis());
                            fut.whenComplete((r, ex) -> {
                                long e = System.nanoTime();
                                if (ex == null) { wLat.add(e - s); wDone.increment(); }
                                else            { wFail.incrementAndGet(); }
                                inflight.release();
                            });
                        } catch (Throwable t) {
                            wFail.incrementAndGet();
                            inflight.release();
                        }
                        id++;
                        paceQpsBatched(perOpNs, epoch, id, paceBatch);
                    }
                } catch (Throwable t) {
                    System.err.println("[Writer] error: " + t);
                }
            }, "outbox-writer");
            writer.setDaemon(true);
        }

        // === Consumer ===
        Thread consumer = new Thread(() -> {
            try {
                long saved0 = db.loadLsn(consumerP);              // -1이면 fresh
                final AtomicLong savedRef = new AtomicLong(saved0);
                long from  = (savedRef.get() < 0) ? -1L : db.nextAddressAfter(savedRef.get());
                long processedSinceSave  = 0;
                long processedSincePrune = 0;

                int idleMs = 1;
                long lastCompact = System.nanoTime();

                while (running.get()) {
                    db.refreshMappings();
                    final AtomicLong lastAddr = new AtomicLong(from);
                    final AtomicLong cnt = new AtomicLong(0);

                    long batchStart = System.nanoTime();
                    try {
                        db.scanFrom(consumerP, from, (key, slice, address) -> {
                            if (savedRef.get() >= 0 && address <= savedRef.get()) return true;
                            cnt.incrementAndGet();
                            lastAddr.set(address);
                            // dispatch 위치
                            return true;
                        });
                    } catch (IndexOutOfBoundsException ioobe) {
                        from = -1L; // 세그먼트 초기화 전 등 상황
                        try { Thread.sleep(idleMs); } catch (InterruptedException ie){ Thread.currentThread().interrupt(); }
                        idleMs = Math.min(8, idleMs << 1);
                        continue;
                    }

                    long got = cnt.get();
                    if (got > 0) {
                        long dt = System.nanoTime() - batchStart;
                        long per = dt / Math.max(1, got);
                        rLat.add(per);

                        rDone.add(got);
                        long newSaved = lastAddr.get();
                        savedRef.set(newSaved);
                        from = newSaved;

                        processedSinceSave  += got;
                        processedSincePrune += got;

                        if (processedSinceSave >= saveEveryN) {
                            db.saveLsn(consumerP, savedRef.get());
                            processedSinceSave = 0;
                        }
                        if (processedSincePrune >= pruneEveryN) {
                            long pruneLsn = Math.max(-1L, savedRef.get() - pruneSafety);
                            try {
                                db.pruneSegmentsUpTo(consumerP, pruneLsn);
                            } catch (Throwable t) {
                                System.out.println("[PRUNE] skip: " + t.getClass().getSimpleName());
                            }
                            processedSincePrune = 0;
                        }

                        idleMs = 1;
                    } else {
                        try { Thread.sleep(idleMs); } catch (InterruptedException ie){ Thread.currentThread().interrupt(); }
                        idleMs = Math.min(8, idleMs << 1);
                    }

                    if (compactEvery > 0) {
                        long nowNs = System.nanoTime();
                        if (nowNs - lastCompact >= compactEvery * 1_000_000_000L) {
                            try { db.compactPartition(consumerP); }
                            catch (Throwable t) { System.err.println("[Compact] failed: " + t); }
                            finally { lastCompact = nowNs; }
                        }
                    }
                }
                if (savedRef.get() >= 0) db.saveLsn(consumerP, savedRef.get());
            } catch (Throwable t) {
                rFail.incrementAndGet();
                System.err.println("[Consumer] error: " + t);
            }
        }, "outbox-consumer");
        consumer.setDaemon(true);

        if (writer != null) writer.start();
        consumer.start();

        // warmup
        Thread.sleep(Math.max(0, warmupSec) * 1000L);
        final long t0 = System.nanoTime();
        t0Ref.set(t0);

        long lastLog = t0;
        long endAt = t0 + (long)durationSec * 1_000_000_000L;
        long killAt = (killAfterSec > 0) ? (t0 + (long)killAfterSec * 1_000_000_000L) : Long.MAX_VALUE;

        while (System.nanoTime() < endAt && running.get()) {
            long now = System.nanoTime();
            if (now - lastLog >= (long)logEverySec * 1_000_000_000L) {
                double secs = (now - t0)/1e9;
                double[] w = pctUs(wLat);
                double[] r = pctUs(rLat);
                double wqps = wDone.sum() / Math.max(1e-9, secs);
                double rqps = rDone.sum() / Math.max(1e-9, secs);
                System.out.printf(Locale.US,
                    "[%3.0fs] writeDone=%d (%.0f/s, fail=%d), readDone=%d (%.0f/s, fail=%d), Wp50=%.1fus Wp95=%.1fus Wp99=%.1fus, Rp50=%.1fus Rp95=%.1fus Rp99=%.1fus%n",
                    secs, wDone.sum(), wqps, wFail.get(), rDone.sum(), rqps, rFail.get(), w[0], w[1], w[2], r[0], r[1], r[2]);
                lastLog = now;
            }
            if (now >= killAt) {
                System.out.println("[KILL] Simulating crash now.");
                System.exit(1);
            }
            Thread.sleep(200);
        }

        // 정상 종료
        running.set(false);
        if (writer != null) writer.join(2000);
        consumer.join(2000);

        // 집계/CSV
        double secs = Math.max(1e-9, (System.nanoTime() - t0)/1e9);
        double wqps = wDone.sum() / secs;
        double rqps = rDone.sum() / secs;
        double[] w = pctUs(wLat);
        double[] r = pctUs(rLat);
        System.out.println("== RESULT ==");
        System.out.printf(Locale.US, "W-THROUGHPUT: %.1f ops/s (fail=%d), LAT(us) p50=%.1f p95=%.1f p99=%.1f%n", wqps, wFail.get(), w[0], w[1], w[2]);
        System.out.printf(Locale.US, "R-THROUGHPUT: %.1f ops/s (fail=%d), LAT(us) p50=%.1f p95=%.1f p99=%.1f%n", rqps, rFail.get(), r[0], r[1], r[2]);

        if (csv) {
            writeCsv(csvPath, System.currentTimeMillis(), durationSec, warmupSec, targetQps, win, valSz,
                     segBytes/(1024*1024), partitions, consumerP, durable, wqps, rqps, w, r);
        }
        db.close();
    }

    private static void writeCsv(String csvPath, long ts, int durationSec, int warmupSec, int targetQps, int win,
                                 int valB, int segMB, int partitions, int consumerP, boolean durable,
                                 double wqps, double rqps, double[] w, double[] r) {
        try {
            Path p = Paths.get(csvPath);
            if (!Files.exists(p) || Files.size(p)==0L) {
                String header = "ts,durationSec,warmupSec,targetQps,window,valB,segMB,partitions,consumerP,durable,wops,rops,wp50us,wp95us,wp99us,rp50us,rp95us,rp99us\n";
                Files.writeString(p, header, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            }
            String row = String.format(Locale.US,
                "%d,%d,%d,%d,%d,%d,%d,%d,%d,%s,%.3f,%.3f,%.1f,%.1f,%.1f,%.1f,%.1f,%.1f%n",
                ts, durationSec, warmupSec, targetQps, win, valB, segMB, partitions, consumerP, String.valueOf(durable),
                wqps, rqps, w[0], w[1], w[2], r[0], r[1], r[2]);
            Files.writeString(p, row, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            System.out.println("CSV >> " + p.toAbsolutePath());
        } catch (Throwable t) {
            System.err.println("[WARN] CSV write failed: " + t);
        }
    }
}
