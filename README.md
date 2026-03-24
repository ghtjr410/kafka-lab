# kafka-lab

**Kafka의 동작 원리와 함정을 테스트 코드로 증명하는 학습 lab.**

> Every test name is a provable proposition.
> Don't just read about Kafka — prove it, break it, and fix it.

---

## 목차

- [프로젝트 소개](#프로젝트-소개)
- [시작하기](#시작하기)
- [학습 구조](#학습-구조)
- [테스트 목록](#테스트-목록)
- [설계 원칙](#설계-원칙)
- [이 lab이 다루지 않는 것](#이-lab이-다루지-않는-것)

---

## 프로젝트 소개

Kafka의 **"잘 되는 것"보다 "안 되는 것"**을 먼저 체험합니다.
각 Step은 Before(함정)와 After(해결)를 한 클래스에 담아, 설정 하나 차이로 장애가 나는 이유를 직접 증명합니다.

- 모든 **테스트 이름이 곧 증명 명제**입니다
- **yml 대응** 주석으로 Spring Boot 설정과의 매핑을 명시합니다
- 테스트가 **자기완결** — 토픽 생성, 발행, 검증까지 한 메서드에서 완결됩니다

> messaging-lab에서 "왜 Kafka가 필요한가"를 체험했다면,
> 이 lab에서는 "Kafka를 어떻게 제대로 쓰는가"를 증명합니다.

---

## 시작하기

### 기술 스택

- **Java 21** / **Spring Boot 3.4.4** / **spring-kafka 3.3.4**
- **Docker** — apache/kafka:3.7.0 (KRaft 단일 브로커)
- **Kafka Connect** — FileStream Source/Sink (Step 8)
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
Part 1 — Producer, Consumer, 파티션, 리밸런싱, DLQ, EOS

Step 1   Producer Guarantee     acks=0/1/all 차이, min.insync.replicas 함정
Step 1+  Producer Advanced      linger.ms 배치, buffer.memory 백프레셔, flush()
Step 2   Consumer Offset        AckMode(BATCH/RECORD/MANUAL), auto-commit 함정
Step 2+  Consumer Advanced      seekToBeginning, seek, alterConsumerGroupOffsets
Step 3   Partition & Ordering   key 기반 파티션 분배, 파티션 수 변경 시 매핑 깨짐
Step 3+  Partition Advanced     Consumer 수 > 파티션 수 → 놀리는 Consumer
Step 4   Rebalancing            Eager vs Cooperative, Static Membership, max.poll.interval
Step 5   DLQ & Error Handling   기본 동작은 skip(DLQ 아님!), DLQ 설정
Step 6   Exactly-Once Semantics 멱등 프로듀서, 트랜잭셔널 프로듀서, EOS 경계

Part 2 — 직렬화, Connect, 모니터링, 브로커 내부

Step 7   Serialization & Schema JSON 직렬화, trusted.packages 함정, 스키마 진화
Step 8   Kafka Connect          Source/Sink Connector, REST API, 일시정지/재개
Step 9   Monitoring             Consumer Lag 계산, 클러스터 정보, 클라이언트 메트릭
Step 10  Broker Internals       토픽 설정 조회/변경, KRaft 컨트롤러, Earliest/Latest Offset
```

---

## 이렇게 읽으세요

1. **각 디렉터리의 README를 먼저 읽기** — 그 Step이 증명하는 핵심 함정을 파악합니다
2. **Before(함정) 테스트를 먼저 실행** — "왜 이것이 문제인지" 체험합니다
3. **After(해결) 테스트로 넘어가기** — 설정 하나로 어떻게 해결되는지 확인합니다
4. **yml 대응 주석 확인** — 실무에서 어떤 설정에 매핑되는지 확인합니다

---

## 테스트 목록

> 총 **72개 학습 테스트**. 각 테스트 이름이 곧 증명 명제입니다.

<details>
<summary><b>Step 1 — Producer Guarantee (7개)</b></summary>

| 테스트 | 증명하는 것 |
|--------|-----------|
| acks_0은_브로커_응답을_기다리지_않는다 | 유실 가능, 가장 빠름 |
| acks_1은_리더_기록_후_응답한다 | 리더 장애 시 유실 가능 |
| acks_all은_ISR_전체_기록_후_응답한다 | 가장 안전 |
| 단일_브로커에서_acks_all과_min_insync_replicas_1은_acks_1과_같다 | **함정: RF=1이면 all도 무의미** |
| send_반환값으로_파티션과_offset을_확인할_수_있다 | RecordMetadata 구조 |
| RecordMetadata로_발행된_메시지의_위치를_확인할_수_있다 | offset, partition, topic |
| Header에_correlation_id를_추가할_수_있다 | 메시지 추적 |

</details>

<details>
<summary><b>Step 1+ — Producer Advanced (5개)</b></summary>

| 테스트 | 증명하는 것 |
|--------|-----------|
| linger_ms가_0이면_메시지를_즉시_전송한다 | 배치 효과 없음 |
| linger_ms를_설정하면_배치로_묶어_전송한다 | 처리량 ↑ 지연 ↑ |
| flush를_호출하면_linger_ms를_무시하고_즉시_전송한다 | 강제 전송 |
| max_block_ms가_만료되면_send에서_TimeoutException이_발생한다 | 백프레셔 |
| buffer_memory가_충분하면_send는_즉시_반환된다 | 비동기 동작 |

</details>

<details>
<summary><b>Step 2 — Consumer Offset (11개)</b></summary>

| 테스트 | 증명하는 것 |
|--------|-----------|
| AckMode_BATCH_기본값은_poll_반환_레코드_전부_처리_후_커밋한다 | BATCH 동작 |
| AckMode_RECORD는_레코드_하나_처리할_때마다_커밋한다 | at-least-once |
| AckMode_MANUAL_IMMEDIATE는_명시적_호출_시_즉시_커밋한다 | 세밀한 제어 |
| 예외를_삼키면_offset이_커밋되어_메시지가_유실된다 | **핵심 함정** |
| auto_commit은_처리_완료와_무관하게_주기적으로_커밋한다 | 유실 위험 |
| manual_commit은_처리_완료_후_명시적으로_커밋한다 | 안전한 방식 |
| Spring_Kafka는_enable_auto_commit을_false로_강제한다 | 프레임워크 보호 |
| earliest는_처음부터_소비한다 | 새 그룹 전용 |
| latest는_구독_이후_메시지만_소비한다 | 과거 무시 |
| LEO와_Committed_Offset의_차이가_Consumer_Lag이다 | Lag 개념 |
| Consumer가_느리면_lag이_누적된다 | Lag 폭증 체험 |

</details>

<details>
<summary><b>Step 2+ — Consumer Advanced (3개)</b></summary>

| 테스트 | 증명하는 것 |
|--------|-----------|
| seekToBeginning으로_처음부터_재소비할_수_있다 | offset 리셋 |
| seek으로_특정_offset부터_재소비할_수_있다 | 부분 재처리 |
| AdminClient로_Consumer_Group의_offset을_수동_변경할_수_있다 | 장애 복구 |

</details>

<details>
<summary><b>Step 3 — Partition & Ordering (5개)</b></summary>

| 테스트 | 증명하는 것 |
|--------|-----------|
| null_key는_라운드로빈으로_분산된다 | 기본 분배 |
| 같은_key는_항상_같은_파티션에_할당된다 | 순서 보장 |
| 다른_key는_다른_파티션에_할당될_수_있다 | 병렬 처리 |
| 파티션_수가_변경되면_기존_key의_파티션_매핑이_깨진다 | **함정: rekey** |
| 매핑이_깨지면_같은_key의_메시지가_다른_파티션으로_간다 | 순서 보장 파괴 |

</details>

<details>
<summary><b>Step 3+ — Partition Advanced (2개)</b></summary>

| 테스트 | 증명하는 것 |
|--------|-----------|
| Consumer_수가_파티션_수보다_많으면_놀리는_Consumer가_발생한다 | **함정: 초과분 IDLE** |
| Consumer_수가_파티션_수와_같으면_1대_1로_할당된다 | 이상적 분배 |

</details>

<details>
<summary><b>Step 4 — Rebalancing (6개)</b></summary>

| 테스트 | 증명하는 것 |
|--------|-----------|
| Eager_리밸런싱에서는_새_Consumer_합류_시_모든_파티션이_revoke된다 | stop-the-world |
| Cooperative_리밸런싱에서는_이동이_필요한_파티션만_revoke된다 | 점진적 리밸런싱 |
| Static_Membership_없이_재접속하면_리밸런싱이_발생한다 | 롤링 배포 불안정 |
| Static_Membership으로_재접속_시_같은_파티션을_유지한다 | group.instance.id |
| max_poll_interval을_초과하면_Consumer가_강제_퇴출된다 | **함정: 퇴출 + 중복** |
| max_poll_records를_줄이면_퇴출을_방지할_수_있다 | 처리량/시간 밸런스 |

</details>

<details>
<summary><b>Step 5 — DLQ & Error Handling (2개)</b></summary>

| 테스트 | 증명하는 것 |
|--------|-----------|
| DefaultErrorHandler의_기본_동작은_10회_재시도_후_skip이다 | **함정: DLQ 아님!** |
| DeadLetterPublishingRecoverer를_설정하면_DLT_토픽으로_이동한다 | DLQ 설정 |

</details>

<details>
<summary><b>Step 6 — Exactly-Once Semantics (8개)</b></summary>

| 테스트 | 증명하는 것 |
|--------|-----------|
| 멱등_프로듀서는_세션_내_재시도에서_중복을_방지한다 | PID + sequence |
| 멱등_프로듀서도_재시작하면_새_세션이라_중복이_발생한다 | **함정: 세션 한계** |
| 트랜잭셔널_프로듀서로_원자적_발행을_할_수_있다 | 전부 or 전무 |
| 트랜잭션_abort_시_read_committed_Consumer는_메시지를_볼_수_없다 | abort 필터링 |
| read_committed와_read_uncommitted의_차이를_확인한다 | **함정: 기본값은 read_uncommitted** |
| Kafka_내부_EOS는_Kafka_안에서만_보장된다 | EOS 범위 |
| DB_쓰기는_EOS_범위_밖이라_중복이_발생할_수_있다 | **함정: 외부 시스템** |
| Consumer_멱등키로_외부_시스템_중복을_방어할_수_있다 | 실무 해법 |

</details>

<details>
<summary><b>Step 7 — Serialization & Schema (7개)</b></summary>

| 테스트 | 증명하는 것 |
|--------|-----------|
| String_직렬화로_JSON을_보내면_Consumer는_수동_파싱이_필요하다 | 타입 안전성 없음 |
| JsonSerializer로_객체를_직접_보내고_JsonDeserializer로_받을_수_있다 | 자동 변환 |
| trusted_packages를_설정하지_않으면_역직렬화가_거부된다 | **함정: 보안 차단** |
| Producer가_필드를_추가하면_기존_Consumer는_역직렬화에_실패한다 | **함정: 스키마 깨짐** |
| FAIL_ON_UNKNOWN_PROPERTIES를_끄면_하위_호환성을_유지할_수_있다 | 하위 호환 |
| 필수_필드가_없으면_기본값으로_채워진다 | 상위 호환 |
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
| Consumer_클라이언트_메트릭으로_소비_성능을_확인할_수_있다 | fetch-rate, lag-max |

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

## 설계 원칙

1. **테스트 이름 = 증명 명제** — `@DisplayNameGeneration(ReplaceUnderscores.class)` + 한글
2. **yml이 보여야 한다** — JavaDoc에 `yml 대응:` 명시
3. **assertion이 명제를 직접 증명** — `assertThat(lag).isEqualTo(5)`
4. **테스트가 자기완결** — 토픽 생성, 발행, 검증까지 한 메서드에서 완결
5. **Before/After는 같은 클래스 안에** — 함정(Before)과 해결(After) 비교
6. **KafkaTestBase로 격리** — UUID 기반 고유 토픽/그룹ID

---

## 이 lab이 다루지 않는 것

| 주제 | 다루는 곳 |
|------|----------|
| 왜 Kafka가 필요한가 (이벤트 진화 과정) | messaging-lab |
| ApplicationEvent → Redis → Kafka 전환 체험 | messaging-lab |
| Transactional Outbox Pattern 구현 | messaging-lab |
| Saga Pattern (Choreography/Orchestration) | saga-lab (예정) |
| CDC (Debezium) 기반 Outbox 릴레이 | 별도 주제 |
