// package aodb;

// import java.nio.file.*;
// import java.util.*;
// import java.util.concurrent.*;
// import java.util.concurrent.atomic.*;
// import java.util.stream.Collectors;

// import static java.util.concurrent.TimeUnit.*;

// public class TTLSmoke {

//     // ── tiny assert helpers ─────────────────────────────────────────────────
//     static void check(boolean cond, String msg) {
//         if (!cond) throw new AssertionError(msg);
//     }
//     static void checkEq(Object a, Object b, String msg) {
//         if (!Objects.equals(a, b)) throw new AssertionError(msg + " | got=" + a + " expected=" + b);
//     }

//     // ── cleanup util ────────────────────────────────────────────────────────
//     static void rmTree(Path p) {
//         try {
//             if (!Files.exists(p)) return;
//             try (var s = Files.walk(p)) {
//                 for (Path q : s.sorted(Comparator.reverseOrder()).collect(Collectors.toList())) {
//                     try { Files.deleteIfExists(q); } catch (Exception ignore) {}
//                 }
//             }
//         } catch (Exception ignore) {}
//     }

//     // ── property helpers (optional) ─────────────────────────────────────────
//     static int    propI(String k, int d){ return Integer.getInteger(k, d); }
//     static long   propL(String k, long d){ return Long.getLong(k, d); }
//     static String propS(String k, String d){ String v=System.getProperty(k); return v==null?d:v; }

//     public static void main(String[] args) throws Exception {
//         System.out.println("== AODB TTL Smoke ==");

//         // 개별 실행을 빠르게 하기 위해 서로 다른 디렉터리 사용
//         // Path base = Paths.get(propS("dir", "./ttl-test-" + System.currentTimeMillis()));
//         Path base = Paths.get(propS("dir", "./ttl-test-" + 0));
//         rmTree(base);
//         Files.createDirectories(base);

//         // 공통 파라미터 (필요시 -D로 덮어쓰기 가능)
//         int segBytes   = 1<<28; // 256MB (테스트 충분)
//         int fsyncN     = propI("fsyncN", 8192);
//         long fsyncMs   = propL("fsyncMs", 100);
//         boolean asyncF = true;

//         runPerKeyTTL(base.resolve("perkey"), segBytes, fsyncN, fsyncMs, asyncF);
//         runDefaultTTL(base.resolve("default"), segBytes, fsyncN, fsyncMs, asyncF);
//         runSnapshotReload(base.resolve("reload"), segBytes, fsyncN, fsyncMs, asyncF);
//         runCompactionSkip(base.resolve("compact"), segBytes, fsyncN, fsyncMs, asyncF);

//         System.out.println("== ALL TTL TESTS PASSED ==");
//     }

//     // ── Test 1: per-key TTL 동작 확인 ───────────────────────────────────────
//     static void runPerKeyTTL(Path dir, int segBytes, int fsyncN, long fsyncMs, boolean asyncFsync) throws Exception {
//         rmTree(dir); Files.createDirectories(dir);
//         System.out.println("[T1] Per-key TTL");

//         AppendOnlyDB.Config cfg = new AppendOnlyDB.Config(
//                 dir, segBytes, fsyncN, fsyncMs, false,
//                 1, false, AppendOnlyDB.Durability.RELAXED,
//                 true, 1<<16, 8192, 1, 1000,
//                 AppendOnlyDB.ListenerMode.SYNC, 1, 4096,
//                 true, asyncFsync,
//                 true, 0L // TTL 사용, default TTL 없음
//         );

//         final long TTL_MS = 600; // 짧게
//         try (AppendOnlyDB db = new AppendOnlyDB(cfg)) {
//             long now = System.currentTimeMillis();
//             db.putAsyncTTL("ttl:k1", "v1".getBytes(), now, TTL_MS).join();
//             db.putAsyncTTL("ttl:k2", "v2".getBytes(), now, TTL_MS*3).join();

//             // 만료 전
//             AppendOnlyDB.Value v1a = db.getLatest("ttl:k1");
//             AppendOnlyDB.Value v2a = db.getLatest("ttl:k2");
//             check(v1a != null && !v1a.tombstone, "k1 should exist before TTL");
//             check(v2a != null && !v2a.tombstone, "k2 should exist before TTL");

//             // k1 만료 대기
//             Thread.sleep(TTL_MS + 200);
//             AppendOnlyDB.Value v1b = db.getLatest("ttl:k1");
//             AppendOnlyDB.Value v2b = db.getLatest("ttl:k2");
//             check(v1b == null, "k1 should be expired");
//             check(v2b != null, "k2 should still be alive");

//             // slice 경로도 만료 반영 확인
//             AppendOnlyDB.Slice s1 = db.readLatestSlice("ttl:k1");
//             AppendOnlyDB.Slice s2 = db.readLatestSlice("ttl:k2");
//             check(s1 == null, "slice k1 should be null after expiry");
//             check(s2 != null, "slice k2 should exist");
//         }
//         System.out.println("[T1] OK");
//     }

//     // ── Test 2: Config 기본 TTL 동작 ────────────────────────────────────────
//     static void runDefaultTTL(Path dir, int segBytes, int fsyncN, long fsyncMs, boolean asyncFsync) throws Exception {
//         rmTree(dir); Files.createDirectories(dir);
//         System.out.println("[T2] Default TTL");

//         final long DEFAULT_TTL_MS = 500;

//         AppendOnlyDB.Config cfg = new AppendOnlyDB.Config(
//                 dir, segBytes, fsyncN, fsyncMs, false,
//                 1, false, AppendOnlyDB.Durability.RELAXED,
//                 true, 1<<16, 8192, 1, 1000,
//                 AppendOnlyDB.ListenerMode.SYNC, 1, 4096,
//                 true, asyncFsync,
//                 true, DEFAULT_TTL_MS // 기본 TTL 활성화
//         );

//         try (AppendOnlyDB db = new AppendOnlyDB(cfg)) {
//             long now = System.currentTimeMillis();
//             // 별도 TTL 지정 없이 put -> default TTL 적용
//             db.putAsync("def:k", "v".getBytes(), now).join();

//             AppendOnlyDB.Value v0 = db.getLatest("def:k");
//             check(v0 != null, "key should exist before default TTL expiration");

//             Thread.sleep(DEFAULT_TTL_MS + 200);
//             AppendOnlyDB.Value v1 = db.getLatest("def:k");
//             check(v1 == null, "key should be expired by default TTL");
//         }
//         System.out.println("[T2] OK");
//     }

//     // ── Test 3: 스냅샷/재시작 후 TTL 재구성(만료 반영) ───────────────────────
//     static void runSnapshotReload(Path dir, int segBytes, int fsyncN, long fsyncMs, boolean asyncFsync) throws Exception {
//         rmTree(dir); Files.createDirectories(dir);
//         System.out.println("[T3] Snapshot & Reload TTL rebuild");

//         final long TTL_SHORT = 400;
//         final long TTL_LONG  = 2000;

//         AppendOnlyDB.Config cfg = new AppendOnlyDB.Config(
//                 dir, segBytes, fsyncN, fsyncMs, false,
//                 1, false, AppendOnlyDB.Durability.BATCHED,
//                 true, 1<<16, 8192, 1, 1000,
//                 AppendOnlyDB.ListenerMode.SYNC, 1, 4096,
//                 true, asyncFsync,
//                 true, 0L
//         );

//         // 1) write & close (snapshot 저장됨)
//         try (AppendOnlyDB db = new AppendOnlyDB(cfg)) {
//             long now = System.currentTimeMillis();
//             db.putAsyncTTL("R:kExpire", "x".getBytes(), now, TTL_SHORT).join();
//             db.putAsyncTTL("R:kAlive" , "y".getBytes(), now, TTL_LONG ).join();
//         }

//         // 2) expire 기다린 뒤 재오픈 → 짧은 TTL 키는 rebuild 시 제거되어야 함
//         Thread.sleep(TTL_SHORT + 300);

//         try (AppendOnlyDB db = new AppendOnlyDB(new AppendOnlyDB.Config(dir, true))) { // read-only도 OK
//             AppendOnlyDB.Value ve = db.getLatest("R:kExpire");
//             AppendOnlyDB.Value va = db.getLatest("R:kAlive");
//             check(ve == null, "R:kExpire should be gone after reload");
//             check(va != null, "R:kAlive should survive after reload");
//         }
//         System.out.println("[T3] OK");
//     }

//     // ── Test 4: 컴팩션 시 만료키 제외되는지 확인 ─────────────────────────────
//     static void runCompactionSkip(Path dir, int segBytes, int fsyncN, long fsyncMs, boolean asyncFsync) throws Exception {
//         rmTree(dir); Files.createDirectories(dir);
//         System.out.println("[T4] Compaction skip expired");

//         final long TTL_MS = 600;

//         AppendOnlyDB.Config cfg = new AppendOnlyDB.Config(
//                 dir, segBytes, fsyncN, fsyncMs, false,
//                 1, false, AppendOnlyDB.Durability.RELAXED,
//                 true, 1<<16, 8192, 1, 1000,
//                 AppendOnlyDB.ListenerMode.SYNC, 1, 4096,
//                 true, asyncFsync,
//                 true, 0L
//         );

//         try (AppendOnlyDB db = new AppendOnlyDB(cfg)) {
//             long now = System.currentTimeMillis();
//             db.putAsyncTTL("C:die", "x".getBytes(), now, TTL_MS).join();
//             db.putAsyncTTL("C:live", "y".getBytes(), now, TTL_MS*4).join();

//             Thread.sleep(TTL_MS + 300); // C:die 만료

//             // compaction 수행
//             db.compactPartition(0);

//             // 만료 키는 인덱스에서 제거되어야 함
//             check(db.getLatest("C:die") == null, "C:die should be expired and skipped by compaction");
//             check(db.getLatest("C:live") != null, "C:live should remain after compaction");
//         }
//         System.out.println("[T4] OK");
//     }
// }
