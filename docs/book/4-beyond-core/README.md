# 📕 IV권 — Beyond Core (데이터 플랫폼으로서의 Kafka)

> Core(I~III권)가 "메시지를 안전하게 주고받기"라면, IV권은 **그 위에 얹는 엔터프라이즈 스택**이다.
> 큰 회사가 Kafka를 "데이터 플랫폼"으로 쓸 때 등장하는 것들 — 스트림 처리·데이터 통합·멀티클러스터.

> ⚠️ **이 README가 IV권의 중심 작업판이자 단일 인덱스다.**
>
> **📏 작성 규칙:** 각 불릿은 **"한 주제를 한 문장으로"**. 상세는 개별 `NN-*.md`로. README에 본문을 쏟지 말 것. (규칙 → [전체 표지](../README.md))

---

## 이 권의 역할 · Scope · 경계

**목적**: Core 위에 얹는 **데이터 플랫폼/생태계**를 다룬다. Streams·Connect/CDC·Schema Registry·멀티클러스터.

**한 문장**: *"메시지를 주고받는 것을 넘어, Kafka를 데이터 플랫폼으로 운용하는 법."*

### 다룬다 (Scope)
- **A. 스트림 처리** — Kafka Streams, ksqlDB
- **B. 데이터 통합** — Kafka Connect(심화), **CDC/Debezium**, Schema Registry(Avro/Protobuf)
- **C. 멀티클러스터·대규모** — MirrorMaker 2, Tiered Storage, Cruise Control

### 다루지 않는다 (Out of Scope)
| 주제 | 가야 할 곳 |
|------|-----------|
| Core 원리(복제·합의·트랜잭션…) | **I권 Internals** |
| Spring Kafka 애플리케이션 코드 | **II권 Spring** |
| Core 운영(사이징·모니터링·장애) | **III권 Operations** |
| 이벤트 설계 · **Outbox 패턴 구현** · Saga | messaging-lab / saga-lab |

> 🤖 **경계 주의**: "Outbox/CDC 릴레이를 애플리케이션에서 어떻게 설계하나"는 **messaging-lab**.
> IV권은 "**Kafka 인프라로서** Debezium 커넥터·Schema Registry·MirrorMaker를 어떻게 구성·운용하나".

> 멀티브로커가 기본 전제이며, 멀티클러스터(C파트)는 클러스터 2개 이상을 가정한다.

---

## 목차 (대략 — 깎는 중)

### A. 스트림 처리

#### 1장 — Kafka Streams   🚧
- KStream / KTable, **stream-table duality** (I권 2장에서 씨앗)
- 상태 저장 — RocksDB state store, changelog 토픽
- 윈도우 / 조인 / 집계
- Streams **EOS** (`processing.guarantee=exactly_once_v2`) — Core 트랜잭션(I권 7장) 위에서
- 토폴로지 · 스레드 모델 · 스케일링

#### 2장 — ksqlDB   🚧
- 스트림을 SQL로 (`CREATE STREAM ... SELECT`)
- Streams와의 관계 (ksqlDB = Streams 위의 SQL 엔진)

### B. 데이터 통합

#### 3장 — Kafka Connect (심화)   🚧
- Source / Sink, 분산 워커 · 태스크
- **SMT**(Single Message Transform)
- (기존 `s08_connect` 기본 사용 흡수 + 심화)

#### 4장 — CDC / Debezium   🚧
- DB binlog/WAL → Kafka (MySQL / Postgres 변경분 수집)
- 초기 스냅샷 · 스키마 변경 처리
- "binlog도 로그 → Kafka 로그" 개념 연결 (I권 2장)
- (경계: 애플리케이션 Outbox 설계는 messaging-lab)

#### 5장 — Schema Registry   🚧
- Avro / Protobuf, 데이터 계약(contract)
- 호환성 모드(backward/forward/full) 강제
- (II권 7장 "JSON 직렬화"의 다음 단계 — 중앙 스키마 관리)

### C. 멀티클러스터·대규모

#### 6장 — MirrorMaker 2   🚧
- 클러스터 간 복제, DR, geo-replication
- offset 변환 · active-active / active-passive

#### 7장 — Tiered Storage (KIP-405)   🚧
- 오래된 로그를 원격 스토리지(S3 등)로, 무한 보존

#### 8장 — Cruise Control   🚧
- 자동 리밸런싱 · 용량 관리 · 자가 치유

---

← [전체 표지](../README.md) · [I권](../1-internals/README.md) · [II권](../2-spring/README.md) · [III권](../3-operations/README.md)
