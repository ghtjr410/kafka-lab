---
volume: I
chapter: 8
title: "저장 엔진 — 디스크인데 왜 빠른가"
prose: done
proof:
  mode: self
  status: 부분
  method: "3-broker docker exec — .log 세그먼트 · kafka-dump-log · segment rolling · compaction"
  pending: ["segment rolling 관측", "compaction 키별 최신·tombstone"]
  done: [".log 파일 열기 [code @3.7]", "kafka-dump-log [code @3.7]"]
upstream: ["07-transactions.md"]
forward: ["09-client-runtime.md"]
baseline: { broker: "Kafka 3.9 (MSK)", client: "kafka-clients 3.7", ref: "../../CHARTER.md" }
conventions: ../README.md
---

# 8장. 저장 엔진 — 디스크인데 왜 빠른가

> 앞 장: [7장 트랜잭션·EOS](./07-transactions.md) · 다음 장: [9장 클라이언트 런타임](./09-client-runtime.md)
>
> **이 장의 보장(한 문장)**: *Kafka는 디스크 기반인데도 순차 I/O와 OS 수준 최적화(page cache·zero-copy)로 메모리 큐에 준하는 처리량을 낸다.*

2장에서 로그는 추상이었다. 이 장은 그 로그가 **디스크에 실제로 어떻게 놓이는가**, 그리고 "디스크는 느리다"는 통념을 Kafka가 어떻게 뒤집는가다.

---

## 8.1 통념 깨기 — "디스크 = 느리다"가 틀리는 지점

디스크가 느린 건 **랜덤 접근**일 때다. **순차 접근**은 디스크도 충분히 빠르다(때로 메모리 랜덤 접근과 비슷). Kafka는 로그가 append-only(2장)라 **쓰기는 항상 순차**이고, 읽기도 대부분 순차다(sparse index로 근처까지 점프한 뒤 짧게 순차 스캔 — 8.5) — 이게 첫 번째 비결이다.

```mermaid
graph TB
    subgraph WRITE["쓰기 — 위치 찾기 0 (100% 순차)"]
        direction LR
        W["새 레코드"] --> WT["active segment 끝에 append"]
    end
    subgraph READ["읽기 — 점프 1회 + 순차 (분할상환)"]
        direction LR
        R["offset N부터"] --> RI["sparse index로 근처 점프<br/>(랜덤 1회)"] --> RS[".log 짧게 순차 스캔"] --> RC["이후 계속 순차"]
    end
```

> 쓰기는 끝에만 붙이니 100% 순차, 읽기는 offset을 찾을 때 index로 점프(랜덤 1회) + 그 뒤 순차다 — 이어 읽으면 그 점프가 *분할상환*되어, 이게 위 문단의 "읽기도 **대부분** 순차"의 뜻이다(점프 메커니즘 → 8.5).

<details>
<summary><b>왜 순차는 빠르고 랜덤은 느린가? — 한 층 아래 (HDD·SSD)</b></summary>

"순차라서 빠르다"는 결론만으론 동어반복이다. 한 층 아래 *왜*를 보면:

**HDD** — 데이터 위치는 (트랙, 섹터) 2차원이다. 한 위치를 잡으려면 헤드를 그 트랙으로 옮기고([seek](../GLOSSARY.md#seek-time)) 그 섹터가 헤드 밑으로 돌아올 때까지 기다려야 한다([rotational latency](../GLOSSARY.md#rotational-latency)). [랜덤 I/O](../GLOSSARY.md#random-sequential-io)는 흩어진 위치라 접근마다 이 둘을 물고, [순차 I/O](../GLOSSARY.md#random-sequential-io)는 이어진 섹터라 **첫 1회**만 내고 그 뒤는 옆 칸이라 ≈0으로 **분할상환**된다.

```mermaid
graph LR
    H["헤드 시작"] -. "매번 seek+회전" .-> A["섹터 800"] -.-> B["섹터 12"] -.-> C["섹터 450"]
    S0["순차: 첫 seek 1회"] --> S1["섹터 N"] --> S2["N+1"] --> S3["N+2"]
```

**SSD** — 회전 플래터가 없어 *기계적* seek는 ≈0인데도 순차가 빠르다: (읽기) OS readahead가 요청을 크게·깊게 만들어 디바이스 병렬성을 끌어올리고, (쓰기) 작은 랜덤 쓰기는 [write amplification](../GLOSSARY.md#write-amplification)으로 실제 쓰는 양이 부풀려진다.

→ "위치 찾는 비용"은 HDD에서 가장 직관적이고, SSD에선 *I/O 모양·write amp*로 형태만 바뀐다. 더 깊은 물리(FTL·NCQ)는 → 『데이터 중심 애플리케이션 설계』(DDIA) 3장. `[docs @3.9 Persistence · DDIA 3장 Tier 3]`

<details>
<summary>↳ 더 깊이: seek을 자주 헷갈리는 5가지 + 논리/물리 층위</summary>

대화에서 실제로 막힌 지점들이다:

1. **seek = 논리 커서 ≠ 물리 이동** — `lseek()`/`file.seek()`의 seek은 메모리 속 *논리* 포인터(파일 offset)를 옮긴다. *디스크 헤드* seek은 모터가 arm을 **물리로** 움직인다. 같은 단어, 다른 층.
2. **seek = 주소로 직행 ≠ 탐색(search)** — 컨트롤러가 **LBA**(선형 블록 주소)를 트랙/헤드/섹터로 변환해 헤드를 *그 트랙으로 바로* 보낸다. "있나? 옆 트랙?" 뒤지는 게 아니라 *이동*이다(도서관 청구기호처럼).
3. **트랙 ≠ 페이지** — 트랙=물리 동심원(섹터 수백) / 페이지=OS 가상메모리 논리 단위(4KB). 섹터(4KB)가 페이지와 크기 비슷해 헷갈린다 — 잇는 다리가 page cache(8.2).
4. **seek ≠ rotational latency** — seek=어느 *트랙*이냐(arm 반지름 이동) / rotational latency=그 트랙의 어느 *섹터*냐(스핀들 회전, 평균 ½). 위치=(트랙,섹터) 2축이라 따로 맞춘다.
5. **물리 seek ≠ 논리 index 점프** — sparse index 점프는 *논리*(offset→.log byte, 8.5) 1회. 물리 seek은 그 byte가 **page cache에 없을 때만** 일어난다 — tail 읽기는 보통 캐시 hit라 seek ≈0, 과거를 거슬러 읽을 때 디스크 seek.

**층위로 보면**(위=논리, 아래=물리): 앱(파일 offset=논리 커서) → OS·page cache(파일↔LBA, 4KB 페이지) → 블록 레이어(LBA 선형 번호) → 펌웨어(LBA→트랙/헤드/섹터) → 물리 HW(arm 이동=seek + 회전=rotational latency).

```
[head]헤드 ─ arm ─● pivot      seek ↕ = arm이 헤드를 안/밖 트랙으로
╭─────────────╮
│ ╭─────────╮ │ ← track(동심원)
│ │  ╭───╮  │ │
│ │  │ ⊙ │  │ │   ⊙ = 스핀들(회전축)
│ │  ╰───╯  │ │
│ ╰─────────╯ │
╰─────────────╯  ↻ 회전 → 섹터가 헤드 밑으로 = rotational latency
```

→ 여기까지가 Kafka "왜 빠른가"의 바닥. 그 아래(펌웨어 내부·NAND)는 → DDIA 3장.

</details>

</details>

---

## 8.2 page cache와 zero-copy

두 번째·세 번째 비결은 OS를 적극 활용하는 것이다.

- **OS page cache**: Kafka는 데이터를 JVM 힙에 캐싱하지 않고 **OS 페이지 캐시**에 맡긴다. → GC 압력이 없고, 브로커가 재시작해도 캐시(페이지)가 살아 있으며, 쓰기는 캐시에 했다가 OS가 디스크로 flush한다.
  - 즉 Kafka는 **매 쓰기마다 fsync를 강제하지 않는다**(그래서 page cache 위임이 빠르다). 아직 flush 안 된 데이터의 내구성은 디스크 fsync가 아니라 **복제**로 보장한다 — 여러 브로커의 page cache + `acks=all`·`min.insync.replicas`(3장). 한 브로커의 디스크 손실은 복구(8.9)+복제로 메운다.
- **zero-copy (`sendfile`)**: consumer에게 보낼 때, 디스크→커널→유저공간→커널→소켓의 복사를 거치지 않고 **커널 안에서 디스크→소켓으로 바로** 보낸다(`sendfile` 시스템콜). 유저공간 복사가 사라져 CPU·메모리 대역폭을 아낀다.
  - 단, **TLS 암호화** · **브로커측 재압축** · **구버전 클라이언트용 다운컨버전(메시지 포맷 변환)** 이 끼면 데이터가 user-space를 거쳐야 해서 `sendfile` 경로를 **우회**한다 — zero-copy는 평문·무변환 전송에서만 성립하는 원리다.

```mermaid
graph LR
    subgraph "일반 전송 (복사 多)"
        D1["디스크"] --> K1["커널"] --> U["유저공간"] --> K2["커널"] --> S1["소켓"]
    end
    subgraph "zero-copy (sendfile)"
        D2["디스크"] --> K3["커널"] --> S2["소켓"]
    end
```

> 이 둘이 "메시지 큐인데 왜 파일에 쌓나"의 답이다 — 파일에 쌓되, OS의 가장 빠른 경로를 탄다.

---

## 8.3 Log Segment — 로그의 물리적 실체

파티션 로그는 하나의 거대한 파일이 아니라 **세그먼트(segment)** 들로 쪼개진다.

```
order-events-0/                         (토픽-파티션 디렉터리)
├── 00000000000000000000.log            (레코드 배치 — 실제 데이터)
├── 00000000000000000000.index          (offset → 파일 물리위치, sparse)
├── 00000000000000000000.timeindex      (timestamp → offset)
├── 00000000000000000000.snapshot       (프로듀서 상태 PID→seq — 재시작 복원, 8.4)
├── 00000000000000170245.log            (다음 세그먼트, base offset=170245)
├── leader-epoch-checkpoint             (리더 epoch — 3장·8.9 truncation)
├── partition.metadata                  (파티션 메타: topic id 등)
└── ...
```

- 파일명 = 그 세그먼트의 **base offset**.
- 가장 최근 세그먼트가 **active segment**이고, 쓰기는 여기에만 append된다.
- `segment.bytes`(크기)나 `segment.ms`(시간)에 도달하면 **롤링** — 새 세그먼트를 연다.

**왜 크기·시간 *둘 다* 트리거인가.** retention·compaction은 **닫힌(old) 세그먼트에만** 적용된다(active segment는 안 건드린다). 그래서 크기 트리거(`segment.bytes`)만 있으면 **저트래픽 토픽**은 세그먼트가 좀처럼 안 차 active가 무한정 열려 있고 → 오래된 데이터가 active에 갇혀 retention·compaction이 **굶는다**(예: retention이 1일인데 세그먼트가 일주일째 안 닫혀 삭제가 안 됨). `segment.ms`가 저트래픽에서도 **주기적 강제 롤링**을 보장해 이 구멍을 막는다. `[docs @3.9]`

한 축만 보면 반대 상황에서 구멍이 나므로, 저장 계층의 임계는 **크기+시간 이중 트리거**로 짝지어진다:

| 무엇 | 크기 축 | 시간 축 |
|------|---------|---------|
| 세그먼트 롤링 | `segment.bytes` | `segment.ms` |
| retention 삭제 | `retention.bytes` | `retention.ms` |

(프로듀서 배치도 같은 철학의 `batch.size`+`linger.ms` 짝이다 → [클라이언트 런타임](./09-client-runtime.md).)

> `.index`/`.timeindex`는 **memory-mapped 파일(mmap)** 로 다뤄진다 — OS가 파일을 메모리에 매핑해 빠르게 접근한다. 대신 크래시 시 아직 디스크에 안 내려간 부분이 손상될 수 있어, 재시작 때 복구·재구성이 필요하다(→ 8.9). `[code @3.7]`

---

## 8.4 Record Batch (v2) — 멱등·트랜잭션이 박히는 곳

레코드는 하나씩이 아니라 **배치(RecordBatch)** 단위로 저장·전송된다. v2 배치 헤더에는 `baseOffset`, `producerId`, `producerEpoch`, `baseSequence`, 압축 타입 등이 들어간다.

→ 6장의 멱등(PID/epoch/sequence)과 7장의 트랜잭션 정보가 **바로 이 배치 헤더에 박힌다.** 즉 6·7장의 추상이 8장의 물리 포맷에서 만난다.

---

## 8.5 조회 — sparse index

`.index`는 모든 offset이 아니라 **드문드문(sparse)** 만 기록한다(`index.interval.bytes` 간격). offset을 찾을 때 index로 **근처 위치까지 점프**한 뒤 `.log`를 순차 스캔한다 — 즉 **논리 offset**('몇 번째 레코드')을 **`.log` 안의 물리 byte 위치**('파일 몇 바이트')로 바꾸는 게 `.index`의 일이다(둘은 다르다). → 인덱스 메모리를 아끼면서 조회도 빠르다(순차 스캔은 짧으니까).

---

## 8.6 압축(compression)

프로듀서가 **배치 단위로 압축**(lz4/zstd/snappy/gzip)해서 보내면, **기본값(`compression.type=producer`)에서는** 브로커가 그대로 저장·전송하고 consumer가 푼다. 배치 단위라 압축률이 좋고, 브로커가 풀었다 다시 압축하지 않아 CPU도 아낀다(그래서 zero-copy도 성립 — 8.2). 단 토픽 `compression.type`을 특정 코덱으로 지정하면 브로커가 **재압축**하고, compaction(8.7)·다운컨버전에서도 재인코딩이 일어난다 — 이때는 8.2의 zero-copy 우회 케이스다. (압축 알고리즘 선택은 CPU↔대역폭 트레이드오프 → III권.)

consumer는 코덱 설정이 따로 필요 없다 — 배치 헤더(8.4)의 **압축 타입 필드**로 어떤 코덱인지 읽어 **스스로 해제**한다(self-describing). 브로커가 재압축하면 그 헤더의 코덱 표시도 함께 갱신된다.

---

## 8.7 Log Compaction의 "메커니즘"

2장에서 compaction의 *의미*(키별 최신 = 상태 스냅샷)를 봤다. 여기선 *메커니즘*이다:

- **log cleaner 스레드**가 백그라운드로 세그먼트를 돌며, 같은 key의 옛 레코드를 제거하고 최신만 남긴다.
- cleaner는 **즉시·완전 압축을 보장하지 않는다** — active segment는 제외되고, dirty(미압축) 비율이 임계(`min.cleanable.dirty.ratio`, 기본 0.5)를 넘어야 돌며, `min.compaction.lag.ms`가 지난 레코드만 대상이다. 그래서 같은 key가 한동안 중복으로 남을 수 있고, **소비 측은 offset 순서로 마지막 값을 취하는(last-write-wins) 식으로 읽어야 한다**(의미는 → [로그 추상](./02-log-abstraction.md)). `[docs @3.9]`
- `value=null` 레코드(**tombstone**)는 "이 key를 삭제하라"는 표식이다(key는 살아 있어 *어느* key를 지울지 가리킨다). tombstone **레코드 자체**는 `delete.retention.ms`(기본 1일) 동안 보존됐다가 제거된다 — 소비자가 삭제를 놓치지 않게 한 grace period다. `[docs @3.9]`
- `cleanup.policy=compact`(또는 `compact,delete`)로 켠다.

→ `__consumer_offsets`가 무한히 안 자라는 이유가 이것(compaction)이다. (`__cluster_metadata`는 compaction이 아니라 **KRaft 스냅샷**으로 잘라낸다 — 메커니즘이 다르다, 4장.)

---

## 8.8 Retention — 세그먼트 단위 삭제

`cleanup.policy=delete`(기본)에서는 `retention.ms`(시간)나 `retention.bytes`(크기)를 넘긴 데이터를 지운다. 단 **레코드 하나씩이 아니라 세그먼트 통째로** 삭제한다(그래서 active segment는 안 지워지고, 경계가 세그먼트 단위로 움직인다).

**retention vs compaction — `cleanup.policy` 하나로 갈리는 두 전략.**

| | retention (`delete`) | compaction (`compact`) |
|---|---|---|
| 자르는 기준 | 시간·크기(오래된 것) | key(key별 최신 1개) |
| 무엇이 남나 | 최근 구간의 전체 이력 | 각 key의 최신값(시간 무관) |
| 삭제 단위 | 세그먼트 통째 | 같은 key의 옛 레코드 |
| 데이터 성격 | **이벤트**(흐름) | **상태**(현재 값) |

결정 규칙은 단순하다: **데이터가 이벤트면 `delete`, 상태면 `compact`** — 데이터의 성격이 정책을 정한다. 둘을 함께 켜는 `compact,delete`는 *key별 최신값은 남기되, 그마저 너무 오래되면 삭제*한다. (replay 가능 범위 관점의 retention↔compaction 대비는 → [로그 추상](./02-log-abstraction.md). retention 기간 등 *어떤 값을 고를지*는 운영 판단 → [III권 운영](../3-operations/README.md).) `[docs @3.9]`

---

## 8.9 로그 복구 — 재시작 시 어디부터 믿나

브로커가 재시작하면 **마지막으로 안전하게 디스크에 내려간 지점**을 알아야 한다. 그래서 **로그 디렉터리(`log.dir`)마다** 체크포인트 파일을 두고, 한 파일 안에 **파티션별 행**으로 적는다:

- `recovery-point-offset-checkpoint` — 어디까지 디스크로 flush됐나
- `replication-offset-checkpoint` — HW(3장)
- `log-start-offset-checkpoint` — 로그 시작 offset

(이 셋은 `log.dir` 단위 파일이고, `leader-epoch-checkpoint`는 파티션 디렉터리마다 따로 둔다 — 8.3.)

**clean shutdown**이면 이 체크포인트를 믿고 즉시 시작한다. **unclean shutdown**(크래시·`kill -9`)이면 마지막 세그먼트가 온전하다는 보장이 없으므로, **마지막 세그먼트를 스캔·재검증해 반쯤 쓰인 손상 배치를 잘라낸다.** mmap 인덱스(8.3)도 이때 재구성된다.

→ 3.1의 "커밋했는데 사라짐"을 막는 것이 복제(3장)라면, 그 약속을 **한 브로커의 디스크 레벨에서** 지키는 것이 이 복구 절차다. `[code @3.7]`

---

## 8.10 시간의 의미 — timestamp.type과 retention

레코드의 timestamp는 둘 중 하나다(`message.timestamp.type`):

- **CreateTime**(기본) — 프로듀서가 레코드를 만든 시각
- **LogAppendTime** — 브로커가 받아 로그에 쓴 시각

`.timeindex`(8.3)는 이 timestamp로 "시각 T의 메시지는 어느 offset인가"를 인덱싱해 **시간 기반 seek**을 지원한다.

★ 함정: **retention(시간 기반 삭제)도 이 timestamp를 본다** — 정확히는 한 세그먼트의 **최대 timestamp**가 기준이라 세그먼트 단위로 판정한다(과거 레코드 하나로 즉시 삭제되진 않는다). 그래서 프로듀서가 잘못된 CreateTime(예: 과거 시각)을 넣으면 데이터가 의도보다 **너무 일찍 삭제**되거나, 미래 시각이면 안 지워질 수 있다. (어느 타입을 쓸지는 운영 판단 → III권.) `[docs @3.9]`

---

## 8.11 Tiered Storage — 무한 보존 (KIP-405)

로그가 무한히 쌓이면 로컬 디스크가 한계다. **Tiered Storage**는 오래된 세그먼트를 **원격 스토리지(S3 등)로 내리고**, 로컬엔 최근 것만 둔다.

```mermaid
graph LR
    P[Producer] --> L["로컬 디스크<br/>(최근 세그먼트)"]
    L -->|"오래되면 업로드"| R["원격 스토리지<br/>(S3 등)"]
    C[Consumer] -->|"최근 읽기"| L
    C -.->|"오래된 offset 읽기"| R
```

- **RemoteLogManager**가 원격 업로드·조회를 담당한다.
- consumer가 오래된 offset을 읽으면 **원격에서 fallback**해 가져온다(느리지만 가능).
- **local retention**과 **remote retention**을 따로 설정 — 로컬은 짧게, 원격은 길게(사실상 무한).

→ 보존을 로컬 디스크 용량에서 분리한다. 운영·용량 측면은 → III권. (KIP-405, 3.9부터 production-ready) `[KIP-405]`

---

## 8.12 증명 (executable — docker exec · 부분 2/4)

| 실험 | 관측/단언 | 라벨 |
|------|----------|------|
| 브로커 컨테이너의 `.log` 파일 열기 | 파티션 디렉터리·세그먼트 파일명(base offset) 확인 | `[code @3.7]` |
| `kafka-dump-log --print-data-log` | 배치 헤더(producerId 등)·control record 덤프 | `[code @3.7]` |
| `segment.bytes` 작게 + 다량 produce | 세그먼트 rolling(파일 여러 개) 관측 | `[테스트 예정]` |
| compaction 토픽 cleaner 후 | 키별 최신만, tombstone로 삭제 | `[테스트 예정]` |

---

## 참조

- Kafka 공식 문서 — *Persistence*, *Efficiency*(zero-copy·page cache), *Log Compaction* `[docs @3.9]`
- Linux `sendfile(2)` — zero-copy 근거 `[Tier 0]`
- `[KIP-405]` Tiered Storage (보존 확장, 3.9 production-ready) — 운영 측면은 III권 `[Tier 1]`
- *Designing Data-Intensive Applications* 3장(로그 구조 저장소·SSTable/LSM 대비) `[Tier 3]`

← [7장 트랜잭션·EOS](./07-transactions.md) · [I권 목차](./README.md) · 다음: [9장 클라이언트 런타임](./09-client-runtime.md)
