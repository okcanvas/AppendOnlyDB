// FILE: src/test/java/aodb/OutboxSmoke.java
package aodb;

import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;

public class OutboxSmoke {

    static int    propI(String k, int d){ return Integer.getInteger(k, d); }
    static long   propL(String k, long d){ return Long.getLong(k, d); }
    static String propS(String k, String d){ String v=System.getProperty(k); return v==null?d:v; }
    static boolean propB(String k, boolean d){ String v=System.getProperty(k); return v==null?d:Boolean.parseBoolean(v); }

    static void check(boolean cond, String msg){
        if (!cond) throw new AssertionError(msg);
    }

    public static void main(String[] args) throws Exception {
        Path dir = Paths.get(propS("dir", "./outbox-smoke-data")).toAbsolutePath().normalize();
        int N1 = propI("n1", 100_000);
        int N2 = propI("n2", 25_000);
        int VAL = propI("val", 64);
        int segBytes = propI("segBytes", 256 * 1024 * 1024);
        boolean resetLsn = propB("resetLsn", false);

        System.out.printf(Locale.US, "== OutboxSmoke == dir=%s N1=%d N2=%d VAL=%dB seg=%dMB%n",
                dir, N1, N2, VAL, segBytes / (1024*1024));

        // 디렉터리 보장
        Files.createDirectories(dir);
        if (resetLsn) {
            Files.deleteIfExists(dir.resolve("state_p0.lsn"));
        }

        byte[] payload = new byte[VAL];
        for (int i=0;i<VAL;i++) payload[i] = 'x';

        AppendOnlyDB.Config wcfg = new AppendOnlyDB.Config(
                dir, segBytes, 8192, 100, false,
                1, false, AppendOnlyDB.Durability.BATCHED,
                true, 1<<18, 8192, 1, 2000,
                AppendOnlyDB.ListenerMode.ASYNC_DROP_OLD, 1, 1<<16,
                true, false // async-fsync off → durable 요청 시 즉시 fsync
        );
        AppendOnlyDB.Config roCfg = new AppendOnlyDB.Config(dir, true);

        // ── 0) 초기 상태 판단: 세그먼트 존재 여부 먼저 확인 (빈 디렉터리면 fresh)
        boolean hasSeg = false;
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir, "seg_*.aol")) {
            hasSeg = ds.iterator().hasNext();
        }
        long lsn0 = -1;
        boolean hasAny = false;
        if (hasSeg) {
            try (AppendOnlyDB db = new AppendOnlyDB(roCfg)) {
                lsn0 = db.loadLsn(0);
                final long[] probe = {0};
                db.scanFrom(0, -1L, (key, slice, address) -> { probe[0]++; return false; });
                hasAny = probe[0] > 0;
            }
        }
        boolean fresh = (lsn0 < 0 && !hasAny && !hasSeg);
        System.out.printf("[D] startup lsn=%d, hasAny=%s, hasSeg=%s → mode=%s%n",
                lsn0, String.valueOf(hasAny), String.valueOf(hasSeg), fresh ? "FRESH" : "RESUME");

        // ── 1) (FRESH에서만) N1 durable write
        if (fresh) {
            try (AppendOnlyDB db = new AppendOnlyDB(wcfg)) {
                System.out.println("[W] putAsyncDurable N1...");
                List<CompletableFuture<Long>> futs = new ArrayList<>(N1);
                for (int i=0;i<N1;i++) {
                    futs.add(db.putAsyncDurable("k"+i, payload, System.currentTimeMillis()));
                }
                CompletableFuture.allOf(futs.toArray(new CompletableFuture[0])).join();
                System.out.println("[W] done N1.");
            }
        } else {
            System.out.println("[W] skip N1 phase (resume mode)");
        }

        // ── 2) Reader: fresh면 풀스캔 후 LSN 저장, resume면 기존 LSN 사용
        long lastAddr1;
        try (AppendOnlyDB db = new AppendOnlyDB(roCfg)) {
            if (fresh) {
                final long[] cnt = {0};
                final long[] last = {-1};
                last[0] = db.scanFrom(0, -1L, (key, slice, address) -> { cnt[0]++; last[0]=address; return true; });
                System.out.println("[D] scanned=" + cnt[0] + ", lastAddr=" + last[0]);
                check(cnt[0] == N1, "scanned count mismatch on fresh load");
                db.saveLsn(0, last[0]);
                lastAddr1 = last[0];
            } else {
                long saved = db.loadLsn(0);
                check(saved >= 0, "resume mode requires existing LSN >= 0");
                lastAddr1 = saved;
                System.out.println("[D] resume mode: existing LSN=" + saved);
            }
        }

        // ── 3) 재시작 후: LSN의 '다음'에서 스캔 → 아직 N2 안 썼으니 0건이어야 함
        try (AppendOnlyDB db = new AppendOnlyDB(roCfg)) {
            long saved = db.loadLsn(0);
            long resume = (saved < 0) ? -1L : db.nextAddressAfter(saved);
            final long[] cnt = {0};
            db.scanFrom(0, resume, (key, slice, address) -> { cnt[0]++; return true; });
            System.out.println("[D] after restart (no new writes) more=" + cnt[0]);
            check(cnt[0] == 0, "Should be no new records before N2 phase");
        }

        // ── 4) N2 durable write
        try (AppendOnlyDB db = new AppendOnlyDB(wcfg)) {
            List<CompletableFuture<Long>> futs = new ArrayList<>(N2);
            for (int i=0;i<N2;i++) {
                String k = fresh ? ("k" + (N1 + i)) : ("k:resume:" + i);
                futs.add(db.putAsyncDurable(k, payload, System.currentTimeMillis()));
            }
            CompletableFuture.allOf(futs.toArray(new CompletableFuture[0])).join();
        }

        // ── 5) LSN의 다음부터 재개 → N2건 나와야 함
        try (AppendOnlyDB db = new AppendOnlyDB(roCfg)) {
            long saved = db.loadLsn(0);
            long resume = (saved < 0) ? -1L : db.nextAddressAfter(saved);
            final long[] cnt = {0};
            final long[] last = {saved};
            last[0] = db.scanFrom(0, resume, (key, slice, address) -> { cnt[0]++; last[0]=address; return true; });
            System.out.println("[D] delta scanned=" + cnt[0] + ", lastAddr=" + last[0]);
            check(cnt[0] == N2, "delta scan must equal N2");
            db.saveLsn(0, last[0]);
        }

        System.out.println("== OutboxSmoke OK ==");
    }
}
