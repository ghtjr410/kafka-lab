# II권 10장. 설정 레퍼런스 (인덱스)

> 앞: [코드 구조·순서의 함정](./09-code-order-traps.md) · [II권 목차](./README.md)
>
> **이 장의 성격**: *새 설명이 아니라 인덱스다.* 각 설정의 *의미*는 본편·8·9장이 단일 진실(SSOT)이고, 여기서는 "어디서 다루나 + 검증된 기본값"만 모은다. 같은 설명을 두 번 적지 않는다(드리프트 방지).

> ⚠️ **기본값 검증 정책**: 아래 기본값 중 **`✓` 표시만 1차 소스(KIP/공식 docs/`*Config.java`)로 확인됨.** 나머지(`?`)는 **검증 대기**다 — 추정값을 사실처럼 쓰지 않기 위해 별도 라운드에서 확정한다. (이 책 [SOURCES](./../SOURCES.md) 규율)
>
> 🧭 **이 표는 "무엇이 기본값인가"의 인덱스다. "언제 어느 값으로 돌리는 게 유리한가(trade-off)"는** → [III권 의사결정 트리(CAP·PACELC)](../3-operations/10-config-decision-tree.md). 특히 `acks` · `linger.ms` · `max.block.ms`.

---

## 설정은 어떻게 주입되나 (표를 읽기 전에)

아래 표의 키들은 *"무엇이 기본값인가"* 이전에 **"어디에 어떻게 넣어야 먹는가"** 가 먼저다 — 값만 알아선 안 먹을 수 있다.

- `spring.kafka.*`는 Kafka 설정을 **전부 매핑하지 않는다.** Spring Boot가 아는 일부 키만 전용 프로퍼티(`spring.kafka.consumer.group-id`·`isolation-level` 등)로 노출되고, **전용 키가 없는 설정은 `spring.kafka.consumer.properties.*`(또는 `producer.properties.*`) 맵**으로 넘겨야 한다(예: `partition.assignment.strategy`).
- 전용 키가 없는 설정을 `spring.kafka.consumer.무엇`처럼 적으면 (relaxed binding이 모르는 키라) **조용히 무시**된다 — 에러도 없다. *"설정했는데 왜 안 먹지?"* 의 정체.
- **7장의 `spring.jackson.*`가 `JsonDeserializer`에 미적용**인 것도 같은 결의 함정.
- precedence: 커스텀 `ConsumerFactory`/`@Bean`을 정의하면 yml 기반 자동구성을 **대체**한다(병합 아님 — yml 일부만 반영되는 게 아니라 아예 안 탄다). 둘을 섞지 말고 **한 곳에서** 설정하라.

---

## Producer

| 설정 | 기본값 | 검증 | 다루는 곳 |
|------|--------|------|----------|
| `acks` | `all` (3.0+ idempotence=true가 강제) | ✓ | [본편 1장](../../../src/test/java/com/example/kafka/s01_producer/README.md) · [8장](./08-config-combination-traps.md) |
| `enable.idempotence` | `true` (3.0+) | ✓ `[KIP-679]` | [8장](./08-config-combination-traps.md) · [I권 멱등](../1-internals/06-ordering-atomicity.md) |
| `max.in.flight.requests.per.connection` | 5 | ✓ | [8장](./08-config-combination-traps.md) |
| `linger.ms` | 0 | ✓ `[code @3.7]` | [본편 1장](../../../src/test/java/com/example/kafka/s01_producer/README.md) |
| `batch.size` | 16384 | ✓ `[code @3.7]` | 본편 1장 |
| `buffer.memory` | 33554432 (32MB) | ✓ `[code @3.7]` | 본편 1장 · [I권 클라이언트 런타임](../1-internals/09-client-runtime.md) |
| `max.block.ms` | 60000 | ✓ `[code @3.7]` | 본편 1장 · [I권 클라이언트 런타임](../1-internals/09-client-runtime.md) |
| `delivery.timeout.ms` | 120000 | ✓ `[KIP-91]` | 본편 1장 · [I권 클라이언트 런타임](../1-internals/09-client-runtime.md) |
| `transaction.timeout.ms` | 60000 | ✓ | [I권 트랜잭션](../1-internals/07-transactions.md) |
| `transactional.id` / Spring `transaction-id-prefix` | (없음, 인스턴스별 고유 필수) | — | [본편 6장](../../../src/test/java/com/example/kafka/s06_eos/README.md) |

> `min.insync.replicas`는 **프로듀서 설정이 아니다**(토픽·브로커 레벨). → [I권 복제](../1-internals/03-replication.md) / 운영 기준 → III권.

---

## Consumer

| 설정 | 기본값 | 검증 | 다루는 곳 |
|------|--------|------|----------|
| `enable.auto.commit` | (Spring이 `false`로 강제) | ✓ | [본편 2장](../../../src/test/java/com/example/kafka/s02_consumer/README.md) |
| `auto.offset.reset` | `latest` | ✓ `[code @3.7]` | 본편 2장 |
| `isolation.level` | `read_uncommitted` | ✓ | [본편 6장](../../../src/test/java/com/example/kafka/s06_eos/README.md) · [8장](./08-config-combination-traps.md) · [I권 트랜잭션](../1-internals/07-transactions.md) |
| `heartbeat.interval.ms` | 3000 | ✓ `[code @3.7]` | [8장](./08-config-combination-traps.md) · [I권 조정](../1-internals/05-coordination.md) |
| `session.timeout.ms` | 45000 | ✓ `[code @3.7]` | 8장 · [I권 조정](../1-internals/05-coordination.md) |
| `max.poll.interval.ms` | 300000 | ✓ `[code @3.7]` | 8장 · [9장](./09-code-order-traps.md) |
| `max.poll.records` | 500 | ✓ `[code @3.7]` | 8장 · 9장 |
| `fetch.min.bytes` | 1 | ✓ `[code @3.7]` | [I권 클라이언트 런타임](../1-internals/09-client-runtime.md) (9.7 fetch) |
| `fetch.max.wait.ms` | 500 | ✓ `[code @3.7]` | [I권 클라이언트 런타임](../1-internals/09-client-runtime.md) |
| `group.instance.id` (static membership) | (없음) | — | [본편 4장](../../../src/test/java/com/example/kafka/s04_rebalancing/README.md) · [I권 조정](../1-internals/05-coordination.md) |
| `partition.assignment.strategy` | `[RangeAssignor, CooperativeStickyAssignor]` (3.x) | ✓ `[code @3.7]` | 본편 4장 · [I권 조정](../1-internals/05-coordination.md) |

---

## Listener (Spring `ContainerProperties`)

| 설정 | 의미 | 다루는 곳 |
|------|------|----------|
| `AckMode` | 커밋 시점(BATCH·RECORD·MANUAL·MANUAL_IMMEDIATE) | [본편 2장](../../../src/test/java/com/example/kafka/s02_consumer/README.md) · [9장 commit 위치](./09-code-order-traps.md) |
| `concurrency` | 컨테이너 스레드 수 (`concurrency × 인스턴스 ≤ 파티션`) | [본편 3장](../../../src/test/java/com/example/kafka/s03_partition/README.md) |
| `CommonErrorHandler` / `DefaultErrorHandler` | retry(BackOff) + DLQ(recoverer) | [본편 5장](../../../src/test/java/com/example/kafka/s05_dlq/README.md) · [9장](./09-code-order-traps.md) |
| `@RetryableTopic` | non-blocking retry(retry 토픽) | [9장](./09-code-order-traps.md) |

---

## 기본값 검증 — 완료 ✅

표의 모든 기본값을 **kafka-clients 3.7.0 `ProducerConfig`/`ConsumerConfig`로 verbatim 확인**했다(`✓ [code @3.7]`). 특이:
- `session.timeout.ms=45000` — `[KIP-735]`로 `10000`→`45000`(3.0+).
- `partition.assignment.strategy` = `[RangeAssignor, CooperativeStickyAssignor]`(Range 우선 · cooperative 자동 전환은 → [리밸런싱 & 배포](./04-rebalancing.md)).
- 라벨 `[code @3.7]`은 **클라이언트(kafka-clients 3.7) 기준**이다. 브로커 baseline은 MSK 3.9 → 버전 매트릭스는 [CHARTER](../../CHARTER.md).

---

## 증상 → 함정 역인덱스 (장애 중 진단용)

위 표·본편 아웃라인이 *설정 → 장* 방향이라면, 이건 **증상 → 원인** 역방향이다. 장애 한가운데서 "무슨 증상 → 어디를 펴라"로 쓴다.

> 🔒 각 행은 본편 *보장/착각* 줄과 8·9장에서 **도출**된 것이다 — 새 사실을 정의하지 않는 순수 네비게이션. 본문이 바뀌면 이 표가 아니라 본문을 따른다.

| 증상 | 가장 흔한 원인 → 어디 |
|------|----------------------|
| 메시지가 **유실**됐다 | 처리 전 커밋 / 예외 삼킴 → [2장](#)·[9.2](./09-code-order-traps.md) · skip(DLQ 아님) → [5장](#) · `send()` async 실패 무시 → [1장](#) |
| **중복** 처리된다 | at-least-once 미멱등 → [9.2](./09-code-order-traps.md) · 배치 전체 재시도 → [9.6](./09-code-order-traps.md) · 멱등 꺼짐 → [8.1](./08-config-combination-traps.md) |
| **순서**가 꼬였다 | `max.in.flight`>1 + retry + 멱등 off → [8.2](./08-config-combination-traps.md) |
| 롤링 배포 때 **멈춘다** | 리밸런싱 / 타이밍 → [4장](#)·[8.3](./08-config-combination-traps.md) |
| 역직렬화에서 **죽는다**(무한 재폴링) | `ErrorHandlingDeserializer` 부재(poison-pill) → [7장](#)·[9.1](./09-code-order-traps.md) |
| 멱등이 **조용히** 꺼졌다 | acks / max.in.flight 충돌 → [8.1](./08-config-combination-traps.md) |
| 설정이 **안 먹는다** | `properties.*` 맵 우회 / `spring.jackson` 미적용 → 위 *설정 주입* · [7장](#) |
| 컨슈머가 **퇴출**된다 | 리스너 blocking / poll 초과 → [9.3](./09-code-order-traps.md)·[8.3](./08-config-combination-traps.md) |
| 새 그룹인데 **메시지가 안 온다** | `auto.offset.reset=latest`(기본) → [2장](#) |

> `#` 링크(본편 1·2·4·5·7장)는 정본 본문에 정확한 named anchor를 박아 채울 수 있다(본편 재산문화 완료 ✅) — 장 단위로도 읽힌다.

---

← [코드 구조·순서의 함정](./09-code-order-traps.md) · [II권 목차](./README.md) · 버전 SSOT: [CHARTER](../../CHARTER.md)
