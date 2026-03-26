# kafka-lab

**Kafka의 동작 원리와 함정을 테스트 코드로 증명하는 학습 lab.**

> Every test name is a provable proposition.
> Don't just read about Kafka — prove it, break it, and fix it.

---

## 목차

- [프로젝트 소개](#프로젝트-소개)
- [왜 이 lab이 필요한가](#왜-이-lab이-필요한가)
- [시작하기](#시작하기)
- [학습 구조](#학습-구조)
- [레벨별 학습 경로](#레벨별-학습-경로)
- [학습 순서 가이드](#학습-순서-가이드)
- [테스트 목록](#테스트-목록)
- [이 lab이 다루지 않는 것](#이-lab이-다루지-않는-것)
- [Kafka Internals — 클러스터, 컨트롤러, 코디네이터, 리플리케이션](KAFKA-ARCHITECTURE.md)

---

## 프로젝트 소개

Kafka의 **"잘 되는 것"보다 "안 되는 것"**을 먼저 체험합니다.
각 Step은 함정을 먼저 밟아보고, 설정 하나로 어떻게 해결되는지 직접 증명합니다.
모든 테스트 이름이 곧 증명 명제입니다.

> messaging-lab에서 "왜 Kafka가 필요한가"를 체험했다면,
> 이 lab에서는 "Kafka를 어떻게 제대로 쓰는가"를 증명합니다.

---

## 왜 이 lab이 필요한가

모놀리식 아키텍처에서 하나의 트랜잭션으로 주문·결제·재고·쿠폰을 묶으면 처음엔 단순하지만,
규모가 커지면 **긴 트랜잭션 → DB 커넥션 점유 → 락 → 전체 서비스 부하**로 이어집니다.

서비스를 도메인 단위로 분리(MSA)하면 각 팀이 독립 배포할 수 있지만, **분산 트랜잭션**이라는 새 문제가 생깁니다.
서비스별 DB가 달라 하나의 롤백으로 정합성을 맞출 수 없고, **결과적 일관성(Eventual Consistency)**을 설계해야 합니다.

이때 Kafka는 **메시지의 영속성, 재처리(DLQ), 수평 확장, 감사 추적**을 제공하는 도구입니다.

| 판단 기준 | 동기 (한 트랜잭션) | 비동기 (이벤트/Kafka) |
|-----------|-------------------|---------------------|
| 실패 시 도메인 불변식이 깨지는가? | YES → 동기 | — |
| 나중에 보정해도 되는가? | — | YES → 비동기 |
| UX 상 즉시 응답이 필요한가? | YES → 동기처럼 보이게 (Polling 등) | NO → 비동기 |

> **핵심: Kafka는 목적이 아니라 도구다.**
> 이벤트는 "멋진 구조"가 아니라, 분산 환경에서 결과적 일관성을 설계하기 위한 수단이다.

> 이벤트 네이밍 원칙, Outbox 패턴(Polling vs CDC), Saga 패턴 등 이벤트 설계와 구현에 대해서는 [messaging-lab](../messaging-lab/)에서 다룬다.

---

## 시작하기

### 기술 스택

- **Java 21** / **Spring Boot 3.4.4** / **spring-kafka 3.3.4** (Kafka 클라이언트 3.7.x)
- **Docker** — apache/kafka:3.7.0 (KRaft 단일 브로커)
- **Kafka Connect** — FileStream Source/Sink (Step 8, 개발/테스트 전용)
- **JUnit 5** + **AssertJ** — 학습 테스트

### 필요 환경

- **Java 21** 이상
- **Docker Desktop** 실행 중

### 실행

```bash
# Kafka 브로커 기동
docker-compose up -d kafka

# Kafka Connect도 필요한 경우 (Step 8)
docker-compose up -d

# 전체 테스트 실행
./gradlew test

# 특정 Step만 실행
./gradlew test --tests "com.example.kafka.s01_producer.*"
./gradlew test --tests "com.example.kafka.s04_rebalancing.*"

# 특정 테스트 클래스만 실행
./gradlew test --tests "ProducerAcksTest"
```

---

## 학습 구조

```
Part 1 — 메시지를 안전하게 보내고, 안전하게 받고, 장애에 대응하기

Step 1  "acks=all이면 안전한 거 아닌가?"
        → RF=1이면 ISR 축소 시 acks=1로 퇴화. 배치와 백프레셔까지.
Step 2  "예외를 try-catch로 삼키면 안전한 거 아닌가?"
        → 기본 AckMode(BATCH)에서 offset이 커밋되어 메시지가 영원히 유실된다.
Step 3  "파티션 수를 늘리면 처리량이 올라가니까 좋은 거 아닌가?"
        → 기존 key의 파티션 매핑이 깨진다. concurrency와 파티션의 관계까지.
Step 4  "Consumer를 롤링 배포하면 왜 순간적으로 처리가 멈추는가?"
        → Eager 리밸런싱이 모든 파티션을 회수한다.
Step 5  "Spring Kafka의 기본 에러 핸들러가 DLQ로 보내주는 거 아닌가?"
        → 아니다. 기본 동작은 최대 10회 시도 후 skip이다.
Step 6  "Kafka가 Exactly-Once를 지원하니까 중복 걱정 없는 거 아닌가?"
        → EOS는 Kafka 내부에서만 보장된다.

Part 2 — 직렬화, 파이프라인, 모니터링, 브로커 내부

Step 7  "Producer가 필드를 하나 추가했는데 왜 Consumer가 죽는가?"
        → ObjectMapper 기본값이 알 수 없는 필드를 거부한다.
Step 8  "DB 변경사항을 Kafka로 보내려면 Producer를 직접 짜야 하나?"
        → Kafka Connect로 설정만으로 파이프라인을 만든다.
Step 9  "Consumer가 처리를 못 따라가는 걸 어떻게 알 수 있는가?"
        → Consumer Lag = LEO - Committed Offset.
Step 10 "retention.ms를 실수로 잘못 설정하면 어떻게 되는가?"
        → AdminClient로 브로커 재시작 없이 동적으로 변경한다.
```

---

## 레벨별 학습 경로

| 레벨 | 범위 | 목표 | 선행 지식 |
|------|------|------|----------|
| **초급** | Step 1~3 | 메시지를 안전하게 보내고 받는 것 | Java, Spring Boot 기본 |
| **중급** | Step 4~7 | 리밸런싱, DLQ, EOS, 스키마 진화 | 초급 완료 |
| **고급** | Step 8~10 + [KAFKA-INTERNALS.md](KAFKA-ARCHITECTURE.md) | 운영, 모니터링, 브로커 내부 구조 | 중급 완료 |

- **초급**은 Kafka를 처음 접하는 개발자를 위한 경로. Producer/Consumer/Partition의 기본 동작과 함정을 체험한다.
- **중급**은 실무에서 Kafka를 운영하기 시작한 개발자를 위한 경로. 장애 상황과 트레이드오프를 다룬다.
- **고급**은 [KAFKA-INTERNALS.md](KAFKA-ARCHITECTURE.md)를 먼저 읽고, 클러스터/컨트롤러/코디네이터/리플리케이션의 내부 동작을 이해한 상태에서 Step 8~10을 진행한다. 이 개념들은 Kafka뿐 아니라 **모든 분산 시스템의 기본 패턴**이다.

---

## 학습 순서 가이드

> 총 **72개 학습 테스트**. 각 테스트 이름이 곧 증명 명제입니다.

### 이렇게 읽으세요

1. **각 디렉터리의 README를 먼저 읽기** — 스토리를 따라가며 "왜 이것이 문제인지" 맥락을 잡습니다
2. **테스트를 실행** — 함정을 직접 체험하고, 해결 방법을 확인합니다
3. **"직접 답해보자"에 답하기** — README 끝에 있는 질문에 먼저 답해보고, 막히면 테스트로 검증합니다
4. **yml 대응 주석 확인** — 실무에서 어떤 설정에 매핑되는지 확인합니다

### Step 1 — Producer Guarantee + Advanced

"acks=all이면 안전한 거 아닌가?" — **ISR이 리더 1대로 줄어들면 acks=1로 퇴화한다.** Kafka 3.0+ 기본값(`enable.idempotence=true` → `acks=all` 강제)을 먼저 이해하고, min.insync.replicas 함정, 배치(linger.ms + batch.size), 백프레셔(max.block.ms), delivery.timeout.ms까지 다룬다.

### Step 2 — Consumer Offset + Advanced

"예외를 try-catch로 삼키면 안전한 거 아닌가?" — **기본 AckMode(BATCH)에서 offset이 커밋되어 메시지가 영원히 유실된다.** auto-commit 함정(`auto.commit.interval.ms` 간격), AckMode 제어(BATCH/RECORD/MANUAL/MANUAL_IMMEDIATE), Consumer Lag 개념, seek/AdminClient를 이용한 장애 복구까지.

### Step 3 — Partition & Ordering + Advanced

"파티션 수를 늘리면 처리량이 올라가니까 좋은 거 아닌가?" — **기존 key의 파티션 매핑이 깨진다.** 스티키 파티셔닝(Kafka 2.4+ 기본), murmur2 해시, key 기반 순서 보장, Consumer > 파티션 시 IDLE 문제, **Spring Kafka concurrency와 파티션의 관계**(`concurrency × 인스턴스 수 ≤ 파티션 수`). 파티션 키를 "공유 자원" 기준으로 잡는 실무 패턴까지.

### Step 4 — Rebalancing

"Consumer를 롤링 배포하면 왜 순간적으로 처리가 멈추는가?" — **Eager 리밸런싱이 모든 파티션을 회수한다.** Eager vs Cooperative(2라운드 동작) vs KIP-848(3세대), Kafka 3.0+ 기본 assignor(`[RangeAssignor, CooperativeStickyAssignor]`), Static Membership, 퇴출 메커니즘 두 가지(`session.timeout.ms` vs `max.poll.interval.ms`). 리밸런싱 중 중복 발생과 멱등 처리의 관계까지.

### Step 5 — DLQ & Error Handling

"Spring Kafka의 기본 에러 핸들러가 DLQ로 보내주는 거 아닌가?" — **아니다. 최대 10회 시도 후 조용히 skip한다.** DLQ는 DeadLetterPublishingRecoverer를 명시적으로 설정해야 동작한다. Non-retryable 예외(`DeserializationException` 등)는 재시도 없이 즉시 skip되는 함정까지.

### Step 6 — Exactly-Once Semantics

"Kafka가 Exactly-Once를 지원하니까 중복 걱정 없는 거 아닌가?" — **아니다. EOS는 Kafka 내부에서만 보장된다.** 멱등 프로듀서의 세션 한계, 트랜잭셔널 프로듀서(`transaction-id-prefix` 인스턴스별 고유 필수), read_committed 함정(기본값은 read_uncommitted!), Consumer 멱등키가 최종 방어선인 이유까지.

### Step 7 — Serialization & Schema Evolution

"Producer가 필드를 하나 추가했는데 왜 Consumer가 죽는가?" — **ObjectMapper 기본값이 알 수 없는 필드를 거부한다.** trusted.packages 함정, `__TypeId__` 패키지 불일치, Forward/Backward Compatibility(`@JsonIgnoreProperties(ignoreUnknown = true)`가 가장 확실), 토픽 버저닝은 마지막 수단.

### Step 8 — Kafka Connect

"DB 변경사항을 Kafka로 보내려면 Producer를 직접 짜야 하나?" — **설정만으로 데이터 파이프라인을 만든다.** Standalone vs Distributed 모드, Source/Sink Connector, `tasks.max`, REST API 운영(status 확인 필수), pause/resume.

### Step 9 — Monitoring & Observability

"Consumer가 처리를 못 따라가는 걸 어떻게 알 수 있는가?" — **Consumer Lag = LEO - Committed Offset.** `records-lag-max`(Consumer 자체 보고) vs AdminClient Lag(Consumer 죽어도 감지 가능), Lag 폭증 체험, 클러스터 정보 조회, ISR 확인(under-replicated 감지), Producer/Consumer 클라이언트 메트릭.

### Step 10 — Broker Internals & KRaft

"retention.ms를 실수로 잘못 설정하면 어떻게 되는가?" — **AdminClient로 브로커 재시작 없이 동적으로 변경할 수 있다.** 단, 이미 삭제된 데이터는 복구 불가. `incrementalAlterConfigs`(`alterConfigs`는 전체 교체 사고 위험), KRaft 모드, Earliest/Latest Offset, 토픽 관리(삭제 시 영구 삭제).

---

## 테스트 목록

> 각 Step의 README에 상세 설명이 있습니다.

<details>
<summary><b>Step 1 — Producer Guarantee + Advanced (12개)</b></summary>

| 테스트 | 증명하는 것 |
|--------|-----------|
| acks_0이면_브로커_응답을_기다리지_않는다 | 유실 가능, 가장 빠름 |
| acks_1이면_leader만_확인한다 | 리더 장애 시 유실 가능 |
| acks_all이면_모든_ISR이_확인한다 | 가장 안전 |
| acks_all에_min_insync_replicas_1이면_사실상_acks_1이다 | **함정: ISR 축소 시 퇴화** |
| send의_반환값을_확인해야_발행_성공을_보장할_수_있다 | Future 확인 필수 |
| send의_반환값_RecordMetadata에서_메시지_구조를_확인할_수_있다 | offset, partition, topic |
| Header에_correlation_id를_추가하면_Consumer에서_확인할_수_있다 | 메시지 추적 |
| linger_ms가_0이면_메시지를_즉시_전송한다 | 배치 효과 없음 |
| linger_ms를_설정하면_배치로_묶어_전송한다 | 처리량 ↑ 지연 ↑ |
| flush를_호출하면_linger_ms를_무시하고_즉시_전송한다 | 강제 전송 |
| max_block_ms가_만료되면_send에서_TimeoutException이_발생한다 | 백프레셔 |
| buffer_memory가_충분하면_send는_즉시_반환된다 | 비동기 동작 |

</details>

<details>
<summary><b>Step 2 — Consumer Offset + Advanced (14개)</b></summary>

| 테스트 | 증명하는 것 |
|--------|-----------|
| AckMode_BATCH_기본값은_poll_반환_레코드_전부_처리_후_커밋한다 | BATCH 동작 |
| AckMode_RECORD는_레코드_하나_처리할_때마다_커밋한다 | at-least-once |
| AckMode_MANUAL_IMMEDIATE는_명시적_호출_시_즉시_커밋한다 | 세밀한 제어 |
| 예외를_삼키면_offset이_커밋되어_메시지가_유실된다 | **핵심 함정** |
| auto_commit이면_처리_실패해도_offset이_넘어간다 | 유실 위험 |
| manual_commit에서_커밋_전에_죽으면_중복_소비된다 | at-least-once |
| Spring_Kafka가_auto_commit을_false로_강제하는_이유를_이해한다 | 프레임워크 보호 |
| auto_offset_reset_earliest이면_처음부터_모든_메시지를_소비한다 | 새 그룹 전용 |
| auto_offset_reset_latest이면_기존_메시지를_무시한다 | 과거 무시 |
| LEO에서_Committed_Offset을_빼면_Consumer_Lag이다 | Lag 개념 |
| producer_속도가_consumer보다_빠르면_lag이_누적된다 | Lag 폭증 체험 |
| seekToBeginning으로_처음부터_재소비할_수_있다 | offset 리셋 |
| seek으로_특정_offset부터_재소비할_수_있다 | 부분 재처리 |
| AdminClient로_Consumer_Group의_offset을_수동_변경할_수_있다 | 장애 복구 |

</details>

<details>
<summary><b>Step 3 — Partition & Ordering + Advanced (6개)</b></summary>

| 테스트 | 증명하는 것 |
|--------|-----------|
| key_없이_발행하면_파티션이_분산된다 | 스티키 파티셔닝 기본 |
| 같은_key로_발행하면_같은_파티션에_들어가_순서가_보장된다 | 순서 보장 |
| 서로_다른_key는_서로_다른_파티션에_배정될_수_있다 | 병렬 처리 |
| 파티션_수_변경_후_같은_key가_다른_파티션에_배정된다 | **함정: rekey** |
| 실제_브로커에서_파티션_수_변경_전후_key_배정이_달라진다 | 순서 보장 파괴 |
| Consumer_수가_파티션_수보다_많으면_놀리는_Consumer가_발생한다 | **함정: 초과분 IDLE** |

</details>

<details>
<summary><b>Step 4 — Rebalancing (6개)</b></summary>

| 테스트 | 증명하는 것 |
|--------|-----------|
| Eager_리밸런싱에서는_새_Consumer_합류_시_모든_파티션이_revoke된다 | stop-the-world |
| Cooperative_리밸런싱에서는_이동이_필요한_파티션만_revoke된다 | 점진적 리밸런싱 (2라운드) |
| Static_Membership_없이_재접속하면_리밸런싱이_발생한다 | 롤링 배포 불안정 |
| Static_Membership으로_재접속_시_같은_파티션을_유지한다 | group.instance.id |
| max_poll_interval을_초과하면_Consumer가_강제_퇴출된다 | **함정: 퇴출 + 중복** |
| max_poll_records를_줄이면_퇴출을_방지할_수_있다 | 처리량/시간 밸런스 |

</details>

<details>
<summary><b>Step 5 — DLQ & Error Handling (2개)</b></summary>

| 테스트 | 증명하는 것 |
|--------|-----------|
| DefaultErrorHandler_기본_동작은_재시도_후_skip이다_DLQ가_아니다 | **함정: 최대 10회 시도 후 skip** |
| DLQ를_설정하면_실패한_메시지가_DLT_토픽으로_이동한다 | DLQ 설정 |

</details>

<details>
<summary><b>Step 6 — Exactly-Once Semantics (8개)</b></summary>

| 테스트 | 증명하는 것 |
|--------|-----------|
| 멱등_프로듀서는_같은_세션_내에서_재시도_중복을_방지한다 | PID + sequence |
| Producer_재시작_후_같은_메시지_발행하면_중복_저장된다 | **함정: 세션 한계** |
| 트랜잭셔널_프로듀서로_원자적_발행을_할_수_있다 | 전부 or 전무 |
| 트랜잭션_abort_시_read_committed_Consumer는_메시지를_볼_수_없다 | abort 필터링 |
| read_committed와_read_uncommitted의_차이를_확인한다 | **함정: 기본값은 read_uncommitted** |
| Kafka_내부_produce에서_consume_offset까지는_exactly_once가_보장된다 | EOS 범위 |
| Consumer에서_DB_insert_후_장애나면_DB에_중복이_발생한다_EOS_범위_밖 | **함정: 외부 시스템** |
| Consumer_측_멱등키로_외부_시스템_중복을_방어한다 | 실무 해법 |

</details>

<details>
<summary><b>Step 7 — Serialization & Schema (7개)</b></summary>

| 테스트 | 증명하는 것 |
|--------|-----------|
| String_직렬화로_JSON을_보내면_Consumer는_수동_파싱이_필요하다 | 타입 안전성 없음 |
| JsonSerializer로_객체를_직접_보내고_JsonDeserializer로_받을_수_있다 | 자동 변환 |
| trusted_packages를_설정하지_않으면_역직렬화가_거부된다 | **함정: 보안 차단** |
| Producer가_필드를_추가하면_기존_Consumer는_역직렬화에_실패한다 | **함정: 스키마 깨짐** |
| FAIL_ON_UNKNOWN_PROPERTIES를_끄면_Forward_Compatibility를_확보한다 | Forward 호환 |
| 필수_필드가_없으면_기본값으로_채워진다 | Backward 호환 |
| 메시지_헤더에_스키마_버전을_넣어_호환성을_관리할_수_있다 | 간이 스키마 관리 |

</details>

<details>
<summary><b>Step 8 — Kafka Connect (4개)</b></summary>

| 테스트 | 증명하는 것 |
|--------|-----------|
| Connect_REST_API로_클러스터_정보를_조회할_수_있다 | GET /, GET /connector-plugins |
| Source_Connector로_파일에서_Kafka_토픽으로_데이터를_보낼_수_있다 | Source 동작 |
| Sink_Connector로_Kafka_토픽에서_파일로_데이터를_내보낼_수_있다 | Sink 동작 |
| Connector를_일시정지하고_재개할_수_있다 | pause/resume |

</details>

<details>
<summary><b>Step 9 — Monitoring & Observability (7개)</b></summary>

| 테스트 | 증명하는 것 |
|--------|-----------|
| AdminClient로_Consumer_Lag을_계산할_수_있다 | Lag = LEO - Committed |
| Consumer가_중단되면_lag이_계속_증가한다 | Lag 폭증 재현 |
| 여러_파티션의_lag을_합산하여_전체_그룹_lag을_계산한다 | 멀티파티션 Lag |
| AdminClient로_클러스터_정보를_조회할_수_있다 | describeCluster |
| AdminClient로_토픽의_파티션_리더와_ISR을_확인할_수_있다 | under-replicated 감지 |
| Producer_클라이언트_메트릭으로_발행_성능을_확인할_수_있다 | record-send-rate, latency |
| Consumer_클라이언트_메트릭으로_소비_성능을_확인할_수_있다 | fetch-rate, records-lag-max |

</details>

<details>
<summary><b>Step 10 — Broker Internals & KRaft (5개)</b></summary>

| 테스트 | 증명하는 것 |
|--------|-----------|
| AdminClient로_토픽_설정을_조회할_수_있다 | describeConfigs |
| AdminClient로_토픽_설정을_동적으로_변경할_수_있다 | incrementalAlterConfigs |
| KRaft_모드에서_컨트롤러_노드를_확인할_수_있다 | ZooKeeper 없는 운영 |
| 토픽의_Earliest와_Latest_Offset으로_데이터_범위를_확인한다 | 보존 범위 |
| AdminClient로_토픽_목록을_조회하고_삭제할_수_있다 | 토픽 관리 |

</details>

---

## 프로젝트 구조

```
src/test/java/com/example/kafka/
├── KafkaTestBase.java
├── s01_producer/                    <- Step 1: Producer Guarantee + Advanced
│   ├── ProducerAcksTest.java
│   ├── ProducerRecordStructureTest.java
│   ├── ProducerBatchingTest.java
│   └── ProducerBackpressureTest.java
├── s02_consumer/                    <- Step 2: Consumer Offset + Advanced
│   ├── ConsumerAckModeTest.java
│   ├── ConsumerAutoCommitTrapTest.java
│   ├── ConsumerOffsetResetTest.java
│   ├── ConsumerLagBasicTest.java
│   └── ConsumerOffsetResetToolTest.java
├── s03_partition/                   <- Step 3: Partition & Ordering + Advanced
│   ├── PartitionKeyTest.java
│   ├── PartitionRekeyTest.java
│   └── PartitionConsumerTest.java
├── s04_rebalancing/                 <- Step 4: Rebalancing
│   ├── RebalancingEagerVsCooperativeTest.java
│   ├── StaticMembershipTest.java
│   └── MaxPollIntervalTest.java
├── s05_dlq/                         <- Step 5: DLQ & Error Handling
│   └── DefaultErrorHandlerTrapTest.java
├── s06_eos/                         <- Step 6: Exactly-Once Semantics
│   ├── IdempotentProducerTest.java
│   ├── TransactionalProducerTest.java
│   └── EOSBoundaryTest.java
├── s07_serialization/               <- Step 7: Serialization & Schema
│   ├── JsonSerializerTest.java
│   └── SchemaEvolutionTest.java
├── s08_connect/                     <- Step 8: Kafka Connect
│   └── KafkaConnectTest.java
├── s09_monitoring/                  <- Step 9: Monitoring & Observability
│   ├── ConsumerLagMonitoringTest.java
│   └── BrokerMetricsTest.java
└── s10_broker/                      <- Step 10: Broker Internals & KRaft
    └── BrokerInternalsTest.java
```

---

## 이 lab이 다루지 않는 것

| 주제 | 다루는 곳 |
|------|----------|
| 왜 Kafka가 필요한가 (이벤트 진화 과정) | messaging-lab |
| ApplicationEvent → Redis → Kafka 전환 체험 | messaging-lab |
| Transactional Outbox Pattern 구현 | messaging-lab |
| 이벤트 네이밍 원칙 (Event vs Command) | messaging-lab |
| Outbox 릴레이 (Polling vs CDC) | messaging-lab |
| Saga Pattern (Choreography/Orchestration) | saga-lab (예정) |
| 멀티 브로커 클러스터 운영 (리더 선출, ISR 복구) | 별도 주제 |
| 보안 (SASL/SSL, ACL) | 별도 주제 |
| Schema Registry (Avro 기반) | 별도 주제 |
| Kafka Streams | 별도 주제 |