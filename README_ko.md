# AppendOnlyDB (AODB)
_비동기 라이터, 내구성 있는 ACK, 아웃박스 테일 도우미를 갖춘 고성능 Append‑Only 키–값 저장소._

> **상태**: 아웃박스 스타일 메시징, 로그 캡처, 쓰기 집중 워크로드에 사용되는 프로덕션 지향 프로토타입

---

## 핵심 특징

- 디스크에 **Append‑only 세그먼트** 저장 (`seg_<partition>_<id>.aol`), 간결한 바이너리 포맷 + 체크섬.
- **높은 쓰기 처리량**: 단일 라이터 스레드 + 마이크로 배치 + Direct `ByteBuffer` I/O.
- **내구성 모드**: `RELAXED`, `BATCHED`, `SYNC_EACH_BATCH`, `SYNC_EACH_APPEND`.
- **Durable ACK API** (`putAsyncDurable`) — `fsync` 완료 후에만 Future 완료.
- **Outbox 유틸**: `scanFrom`, `saveLsn`/`loadLsn`, `nextAddressAfter`, `refreshMappings`.
- **컨슈머 주도 보존정책**: `pruneSegmentsUpTo(partition, lsn)` / `compactPartition(partition)`.
- **저지연 리스너**: `SYNC` / `ASYNC` / `ASYNC_DROP_OLD` 모드.
- **크래시 안전 시작**: 디렉터리 단일 라이터 보장을 위한 `DirectoryLock`.
- 내장 **소크 테스트**: `OutboxSoak`(QPS 장기 부하, 컴팩션/프루닝 포함).

---

## 시작하기

### 빌드
```bash
mvn -q -DskipTests package
```

### 최소 사용 예시
```java
import aodb.AppendOnlyDB;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;

Path dir = Paths.get("./aodb-data");
AppendOnlyDB.Config cfg = new AppendOnlyDB.Config(dir);  // 기본: BATCHED 내구성, 비동기 라이터
try (AppendOnlyDB db = new AppendOnlyDB(cfg)) {
    // 비내구성 비동기 쓰기
    CompletableFuture<Long> f1 = db.putAsync("k1", "v1".getBytes(), System.currentTimeMillis());

    // 내구성 비동기 쓰기(ACK는 fsync 이후)
    CompletableFuture<Long> f2 = db.putAsyncDurable("k2", "v2".getBytes(), System.currentTimeMillis());

    // 동기 쓰기
    long addr = db.put("k3", "v3".getBytes(), System.currentTimeMillis());

    // 최신 버전 조회
    AppendOnlyDB.Value v = db.getLatest("k3");
}
```

### 읽기 전용 프로세스
```java
AppendOnlyDB ro = new AppendOnlyDB(new AppendOnlyDB.Config(dir, /*readOnly*/ true));
ro.refreshMappings(); // 라이터가 테일을 확장하면 mmap 재매핑
```

> **프로세스 모델**: **하나의 라이터 프로세스**만 허용(디렉터리 배타 락). 여러 **읽기 전용** 프로세스는 동시에 열 수 있음.

---

## API 개요

**비동기 쓰기**  
- `CompletableFuture<Long> putAsync(String key, byte[] value, long tsMs)`  
- `CompletableFuture<Long> deleteAsync(String key, long tsMs)`  
- `CompletableFuture<Long> putAsyncDurable(String key, byte[] value, long tsMs)`  
- `CompletableFuture<Long> deleteAsyncDurable(String key, long tsMs)`

**동기 쓰기**  
- `long put(String key, byte[] value, long tsMs)`  
- `long delete(String key, long tsMs)`

**조회/읽기**  
- `AppendOnlyDB.Value getLatest(String key)` – `key`의 최신 값(또는 톰브스톤이면 null).  
- `AppendOnlyDB.Slice readLatestSlice(String key)` – 0‑copy key/value 슬라이스(읽기 전용 뷰).

**테일 스캔(디스패처/컨슈머)**  
- `long scanFrom(int partition, long fromAddress, RecordHandler handler)`  
  `fromAddress`(포함)부터 테일까지 순차 반복. 마지막 방문 주소 반환.

**LSN 도우미**  
- `void saveLsn(int partition, long address)` → 컨슈머 커서 저장 (`state_p<partition>.lsn`).  
- `long loadLsn(int partition)` → 저장된 커서 로드(없으면 `-1`).  
- `long nextAddressAfter(long address)` → 중복 없이 재시작할 다음 레코드 시작 주소.

**유지보수**  
- `int pruneSegmentsUpTo(int partition, long inclusiveLsn)` – 끝 주소가 `inclusiveLsn` 이하인 **세그먼트 단위** 삭제.  
- `void compactPartition(int partition)` – 각 키의 **최신본만** 새 테일로 재기록(구버전/톰브스톤 제거).  
- `void refreshMappings()` – RO/테일러에서 테일 확장 감지 + mmap 갱신.  
- `void sync()` – 테일 전부 fsync.  
- `void saveIndexSnapshot()` – 인메모리 최신쓰기 인덱스 스냅샷 저장(부팅 가속).

**리스너**  
- `subscribe(Listener l)` / `unsubscribe(Listener l)` – append 시 호출(아래 `ListenerMode` 참조).

---

## 설정 (Config)

`AppendOnlyDB.Config`로 내구성, 배치, 파티션, 리스너 동작을 조정합니다.

```java
public static final class Config {
    public final Path dir;
    public final int  segBytes;                 // 세그먼트 용량(예: 1<<30)
    public final int  fsyncEveryN;              // BATCHED: N건마다 fsync
    public final long fsyncEveryMs;             // BATCHED: 또는 N ms마다 fsync
    public final boolean fsyncMetadata;         // 메타데이터 fsync 포함 여부
    public final int  partitions;               // 독립 테일 수
    public final boolean openReadOnly;
    public final Durability durability;         // RELAXED, BATCHED, SYNC_EACH_BATCH, SYNC_EACH_APPEND
    public final boolean enableAsyncWriter;     // 백그라운드 라이터 + 마이크로 배치
    public final int  writerQueueCapacity;
    public final int  writerBatchMax;
    public final long writerBatchWaitMs;
    public final long offerTimeoutMs;           // 비동기 라이터 enq 타임아웃
    public final ListenerMode listenerMode;     // SYNC, ASYNC, ASYNC_DROP_OLD
    public final int  listenerThreads;
    public final int  listenerQueueCapacity;
    public final boolean enableUnmap;           // 회전/종료 시 구세그먼트 unmap 시도
    public final boolean enableAsyncFsync;      // BATCHED: fsync 워커 오프로딩
}
```

**생성자**
- `Config(Path dir)` – 합리적 기본값(BATCHED, 비동기 라이터).  
- `Config(Path dir, boolean readOnly)` – 읽기 전용(RELAXED, 라이터 off).  
- 전체 튜닝용 생성자는 소스 참고.

### 내구성 모드

- `RELAXED` – 일반 append는 fsync 안함; **durable 요청**은 즉시 fsync.  
- `BATCHED` – `fsyncEveryN`/`fsyncEveryMs` 기준으로 주기적 fsync(+옵션: 비동기 fsync 워커); **durable 요청**은 즉시 fsync.  
- `SYNC_EACH_BATCH` – 각 라이터 마이크로 배치 후 fsync.  
- `SYNC_EACH_APPEND` – 매 append마다 fsync(최강 보장, 최저 처리량).

### 리스너 모드

- `SYNC` – 라이터 스레드 인라인 실행.  
- `ASYNC` – 스레드풀 오프로딩, 백프레셔.  
- `ASYNC_DROP_OLD` – 스레드풀 오프로딩, 포화 시 가장 오래된 작업 드롭.

---

## 파일 & 레코드 포맷

- 파티션별 세그먼트 파일: `seg_<partition>_<id>.aol` (`id`는 0‑패딩, 예: `seg_0_000112.aol`).  
- **레코드 프레이밍**(big‑endian):
  - `4B` **MAGIC** = `0xA0DBA0DB`
  - `4B` **TOTAL_LEN** (아래 페이로드 길이)
  - **Payload**:
    - `1B` **VERSION** (`2`=현재, `1`=레거시)
    - `1B` **FLAGS** (`0x1` 톰브스톤)
    - `8B` **tsMs** (앱 타임스탬프)
    - `varint` **keyLen**, `varint` **valLen** (VERSION=2; 레거시는 `int`)
    - `keyLen` 바이트 **key**(UTF‑8)
    - `valLen` 바이트 **value**
    - `4B` **checksum** (payload 32‑bit 체크섬)
- **주소(address)**: `(segId << 32) | segOffset` 64‑bit 논리 포인터.

---

## Outbox 소비 패턴

파티션당 전형적인 컨슈머 루프:

```java
int p = 0;
long saved = db.loadLsn(p);                     // 없으면 -1
long from  = (saved < 0) ? -1 : db.nextAddressAfter(saved);

db.refreshMappings();
long last = db.scanFrom(p, from, (key, slice, address) -> {
    // slice.value 를 다운스트림으로 전달
    return true;
});
db.saveLsn(p, last);
```

**보존/삭제**: 모든 다운스트림이 `inclusiveLsn`까지 처리했다면,
```java
int removed = db.pruneSegmentsUpTo(p, inclusiveLsn);
```
로 해당 지점까지의 **세그먼트**를 제거합니다. 키 레벨로 구버전/톰브스톤을 제거하려면:
```java
db.compactPartition(p);
```

> 여러 독립 컨슈머가 있다면 각자 **LSN**을 관리하고, 프루닝은 그룹의 **최소 커밋 LSN**까지만 수행하세요.

---

## 벤치마크: `OutboxSoak`

단일 `AppendOnlyDB` 인스턴스를 **라이터**와 **컨슈머** 스레드가 공유하며, 선택적으로 백그라운드 유지보수를 수행하는 장기 소크 테스트입니다.

**주요 플래그**
- `-Ddir=./outbox-soak-data` — 데이터 디렉터리
- `-DdurationSec=3600` — 전체 실행 시간(초)
- `-DwarmupSec=10` — 워밍업(종료 요약 QPS 계산에서 제외)
- `-DtargetQps=30000` — 목표 쓰기 QPS
- `-Dwindow=4096` — 동시 인플라이트 쓰기 수(세마포어 창)
- `-Dval=128` — 페이로드 크기(바이트)
- `-DsegBytes=268435456` — 세그먼트 크기(바이트)
- `-Dpartitions=1` — 파티션 수
- `-DconsumerPartition=0` — 소비할 파티션 인덱스
- `-DdurableAck=true` — 내구성 ACK 쓰기 사용
- `-DsaveEveryN=50000` — N개 소비마다 LSN 저장
- `-DpruneEveryN=200000` — N개 소비마다 프루닝 시도
- `-DpruneSafety=0` — 저장된 LSN 앞쪽으로 남겨둘 안전 여유(주소 단위)
- `-DcompactEverySec=60` — 주기적 컴팩션(0 = 비활성화)
- `-DlatCap=80000` — 레이턴시 링 용량(다운샘플링으로 OOM 방지)
- `-DreaderOnly=false` — `true`면 컨슈머만 실행

**예시(Windows)**

```bat
java -Xms2g -Xmx2g -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:+ParallelRefProcEnabled ^
     -XX:MaxDirectMemorySize=1g -XX:+AlwaysPreTouch ^
     -Ddir=./outbox-soak-data -DreaderOnly=false -DdurationSec=3600 -DwarmupSec=10 ^
     -DtargetQps=20000 -Dwindow=4096 -Dval=128 -DsegBytes=268435456 -Dpartitions=1 ^
     -DconsumerPartition=0 -DdurableAck=true -DsaveEveryN=20000 -DpruneEveryN=60000 ^
     -DpruneSafety=0 -DcompactEverySec=20 -DlatCap=80000 ^
     -cp "target/classes;target/test-classes" aodb.OutboxSoak
```

프로그램은 쓰기/읽기 처리량과 지연(p50/p95/p99, μs)을 주기적으로 출력하고, 프루닝 이벤트도 기록합니다:
```
[PRUNE] partition=0 removedSegments=1 < segId 18
```

---

## 튜닝 팁

- **세그먼트 크기**: 256–1024MB가 무난합니다. 크면 쓰기 지역성이 좋아지고, 작으면 프루닝이 잦아져 빠른 회수가 가능합니다.
- **배치 fsync**: `fsyncEveryN=8192`, `fsyncEveryMs=50–100`으로 시작. `enableAsyncFsync`로 라이터와 fsync 분리.
- **라이터 마이크로 배치**: `writerBatchMax≈4–8K`, `writerBatchWaitMs=1–2ms`가 고 QPS에서 p99 안정화에 도움.
- **리스너 모드**: 파이프가 가득 찰 수 있는 구독에는 `ASYNC_DROP_OLD` 권장.
- **파티션**: 키가 자연스럽게 샤딩된다면 파티션 수를 늘려 병렬성 향상.
- **GC**: 기본 워크로드에 G1이 적합. 큰 Direct 버퍼에는 `-XX:+AlwaysPreTouch`, `-XX:MaxDirectMemorySize=...` 고려.
- **리더**: 긴 스캔 전에 `refreshMappings()` 호출로 테일 확장 반영.
- **보존정책**: 디스크 사용량을 제한하려면 LSN 기준 **프루닝**을 자주, 구버전/톰브스톤 정리는 **컴팩션**을 주기적으로.

---

## 의미와 제한

- **단일 라이터 프로세스**(디렉터리 배타 락). 여러 **읽기 전용** 프로세스는 동시 접근 가능.
- 키는 UTF‑8 문자열, 값은 임의의 바이트 배열. 값 업데이트는 새 버전 append.
- **TTL 내장 아님**: 만료/삭제는 컨슈머 주도(프루닝/컴팩션)로 수행.
- 주소는 프로세스 수명 내에서만 안정적. 프루닝/컴팩션 후 옛 주소는 무효가 될 수 있음.
- 크래시 허용성: `BATCHED`에서는 비내구성 쓰기가 다음 fsync까지 지연될 수 있음. 강한 보장이 필요하면 `putAsyncDurable` 사용.

---

## 라이선스
TBD.
