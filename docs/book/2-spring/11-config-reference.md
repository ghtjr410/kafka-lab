# II권 11장. 설정 레퍼런스 (인덱스)

> 앞: [10장 코드 구조·순서의 함정](./10-code-order-traps.md) · [II권 목차](./README.md)
>
> **이 장의 성격**: *새 설명이 아니라 인덱스다.* 각 설정의 *의미*는 본편·9·10장이 단일 진실(SSOT)이고, 여기서는 "어디서 다루나 + 검증된 기본값"만 모은다. 같은 설명을 두 번 적지 않는다(드리프트 방지).

> ⚠️ **기본값 검증 정책**: 아래 기본값 중 **`✓` 표시만 1차 소스(KIP/공식 docs/`*Config.java`)로 확인됨.** 나머지(`?`)는 **검증 대기**다 — 추정값을 사실처럼 쓰지 않기 위해 별도 라운드에서 확정한다. (이 책 [SOURCES](./../SOURCES.md) 규율)

---

## Producer

| 설정 | 기본값 | 검증 | 다루는 곳 |
|------|--------|------|----------|
| `acks` | `all` (3.0+ idempotence=true가 강제) | ✓ | [본편 1장](../../../src/test/java/com/example/kafka/s01_producer/README.md) · [9장](./09-config-combination-traps.md) |
| `enable.idempotence` | `true` (3.0+) | ✓ `[KIP-679]` | [9장](./09-config-combination-traps.md) · [I권 멱등](../1-internals/06-ordering-atomicity.md) |
| `max.in.flight.requests.per.connection` | 5 | ✓ | [9장](./09-config-combination-traps.md) |
| `linger.ms` | 0 | ? | [본편 1장](../../../src/test/java/com/example/kafka/s01_producer/README.md) |
| `batch.size` | 16384 | ? | 본편 1장 |
| `buffer.memory` | 33554432 (32MB) | ? | 본편 1장 · [I권 클라이언트 런타임](../1-internals/09-client-runtime.md) |
| `max.block.ms` | 60000 | ? | 본편 1장 · I권 9장 |
| `delivery.timeout.ms` | 120000 | ✓ `[KIP-91]` | 본편 1장 · I권 9장 |
| `transaction.timeout.ms` | 60000 | ✓ | [I권 트랜잭션](../1-internals/07-transactions.md) |
| `transactional.id` / Spring `transaction-id-prefix` | (없음, 인스턴스별 고유 필수) | — | [본편 6장](../../../src/test/java/com/example/kafka/s06_eos/README.md) |

> `min.insync.replicas`는 **프로듀서 설정이 아니다**(토픽·브로커 레벨). → [I권 복제](../1-internals/03-replication.md) / 운영 기준 → III권.

---

## Consumer

| 설정 | 기본값 | 검증 | 다루는 곳 |
|------|--------|------|----------|
| `enable.auto.commit` | (Spring이 `false`로 강제) | ✓ | [본편 2장](../../../src/test/java/com/example/kafka/s02_consumer/README.md) |
| `auto.offset.reset` | `latest` | ? | 본편 2장 |
| `isolation.level` | `read_uncommitted` | ✓ | [9장](./09-config-combination-traps.md) · [I권 트랜잭션](../1-internals/07-transactions.md) |
| `heartbeat.interval.ms` | 3000 | ? | [9장](./09-config-combination-traps.md) · [I권 조정](../1-internals/05-coordination.md) |
| `session.timeout.ms` | 45000 | ? | 9장 · I권 5장 |
| `max.poll.interval.ms` | 300000 | ? | 9장 · [10장](./10-code-order-traps.md) |
| `max.poll.records` | 500 | ? | 9장 · 10장 |
| `fetch.min.bytes` | 1 | ? | [I권 클라이언트 런타임](../1-internals/09-client-runtime.md) (9.7 fetch) |
| `fetch.max.wait.ms` | 500 | ? | I권 9장 |
| `group.instance.id` (static membership) | (없음) | — | [본편 4장](../../../src/test/java/com/example/kafka/s04_rebalancing/README.md) · I권 5장 |
| `partition.assignment.strategy` | `[RangeAssignor, CooperativeStickyAssignor]` (3.x) | ? | 본편 4장 · I권 5장 |

---

## Listener (Spring `ContainerProperties`)

| 설정 | 의미 | 다루는 곳 |
|------|------|----------|
| `AckMode` | 커밋 시점(BATCH·RECORD·MANUAL·MANUAL_IMMEDIATE) | [본편 2장](../../../src/test/java/com/example/kafka/s02_consumer/README.md) · [10장 commit 위치](./10-code-order-traps.md) |
| `concurrency` | 컨테이너 스레드 수 (`concurrency × 인스턴스 ≤ 파티션`) | [본편 3장](../../../src/test/java/com/example/kafka/s03_partition/README.md) |
| `CommonErrorHandler` / `DefaultErrorHandler` | retry(BackOff) + DLQ(recoverer) | [본편 5장](../../../src/test/java/com/example/kafka/s05_dlq/README.md) · [10장](./10-code-order-traps.md) |
| `@RetryableTopic` | non-blocking retry(retry 토픽) | [10장](./10-code-order-traps.md) |

---

## 검증 대기 목록 (다음 라운드)

위 표의 `?` 기본값은 **Kafka 3.7 / spring-kafka 3.3.x 기준으로 1차 소스 확정**해야 한다 — 특히:
- `partition.assignment.strategy` 3.x 기본값 (sticky 관련, 9.2·본편 3장에서도 미검증 지적됨)
- `auto.offset.reset`·`fetch.*`·타이밍 3박자 기본값

확정 후 `?` → `✓`로 갱신하고, 버전 라벨(`[docs @3.7]`)을 단다.

---

← [10장 코드 구조·순서의 함정](./10-code-order-traps.md) · [II권 목차](./README.md) · 버전 SSOT: [CHARTER](../../CHARTER.md)
