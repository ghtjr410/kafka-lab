# kafka-lab 작업 현황

## 환경
- Spring Boot 3.4.4, spring-kafka 3.3.4, Java 21
- Docker: apache/kafka:3.7.0 (KRaft 단일 브로커) — `docker-compose up -d kafka`
- Gradle 8.10, 테스트 stdout 출력 활성화 (`showStandardStreams = true`)

## 커밋 컨벤션
- `feat:`, `chore:`, `test:`, `docs:`, `refactor:`, `fix:`
- Co-Authored-By 제외

---

## 완료된 작업 (9 commits, 33개 테스트 전부 PASSED)

### 인프라
| 커밋 | 내용 |
|------|------|
| `chore:` 9fdeec1 | Spring Boot 3.4.4 + spring-kafka 프로젝트 뼈대 (build.gradle.kts, docker-compose.yml, application.yml) |
| `test:` b8028c5 | KafkaTestBase — 고유 토픽/그룹ID, AdminClient 토픽 생성, producerProps/consumerProps 팩토리, sendSync/pollValues |

### Step 1. Producer Guarantee (7개 테스트)
| 커밋 | 테스트 클래스 | 내용 |
|------|--------------|------|
| `test:` 9f6fac5 | ProducerAcksTest (5개) | acks=0/1/all 차이, min.insync.replicas=1 함정, send() 반환값 확인 |
| | ProducerRecordStructureTest (2개) | RecordMetadata 구조, Header correlation-id |

### Step 2. Consumer Offset (11개 테스트)
| 커밋 | 테스트 클래스 | 내용 |
|------|--------------|------|
| `test:` 206bd5e | ConsumerAckModeTest (4개) | BATCH/RECORD/MANUAL_IMMEDIATE 동작, 예외 삼키기 함정 |
| `test:` ccc347c | ConsumerAutoCommitTrapTest (3개) | auto-commit 유실, manual commit 중복, Spring Kafka가 false 강제하는 이유 |
| `test:` 1d60e6a | ConsumerOffsetResetTest (2개) | earliest vs latest |
| | ConsumerLagBasicTest (2개) | LEO - Committed = Lag, lag 누적 재현 |

### Step 3. Partition & Ordering (5개 테스트)
| 커밋 | 테스트 클래스 | 내용 |
|------|--------------|------|
| `test:` 10582cc | PartitionKeyTest (3개) | null key 분산, 같은 key 같은 파티션, 다른 key 다른 파티션 |
| | PartitionRekeyTest (2개) | 파티션 수 변경 시 murmur2 매핑 깨짐 |

### Step 5. DLQ & Error Handling (2개 테스트)
| 커밋 | 테스트 클래스 | 내용 |
|------|--------------|------|
| `test:` 1885624 | DefaultErrorHandlerTrapTest (2개) | 기본 동작은 skip(DLQ 아님!), DLQ 설정 시 .DLT 토픽 이동 |

### Step 6. Exactly-Once Semantics (8개 테스트)
| 커밋 | 테스트 클래스 | 내용 |
|------|--------------|------|
| `test:` 0ebecb2 | IdempotentProducerTest (2개) | 멱등 프로듀서 세션 내 재시도 중복 방지, 재시작 시 중복 |
| | TransactionalProducerTest (3개) | 트랜잭셔널 원자적 발행, abort 필터링, read_committed vs read_uncommitted |
| | EOSBoundaryTest (3개) | Kafka 내부 EOS 보장, DB 중복(EOS 범위 밖), Consumer 멱등키 방어 |

---

## 남은 작업 (Part 1 나머지 + Part 2)

### Part 1 — 아직 안 한 Step

#### Step 4. Rebalancing (난이도 높음 — 멀티 Consumer 동시성 필요)
- RebalancingEagerVsCooperativeTest: Eager(stop-the-world) vs Cooperative(점진적) 리밸런싱
- StaticMembershipTest: group.instance.id 설정 시 불필요한 리밸런싱 방지
- MaxPollIntervalTest: 처리 시간 초과 시 강제 퇴출 + lag 폭증
- 구현 난점: ExecutorService + CountDownLatch로 여러 Consumer 동시 기동 필요

#### Step 1 Advanced (선택)
- ProducerBatchingTest: linger.ms, batch.size에 따른 배치 동작
- ProducerBackpressureTest: buffer.memory 초과 시 max.block.ms 블로킹

#### Step 2 Advanced (선택)
- ConsumerOffsetResetToolTest: AdminClient로 offset 수동 조작 (seekToBeginning 등)

#### Step 3 Advanced (선택)
- PartitionConsumerTest: 파티션 수와 Consumer 수 관계 (파티션 < Consumer → 놀림)

### Part 2 — 클러스터 운영과 생태계

#### Step 7. Serialization & Schema
- JSON/Avro 직렬화, 스키마 호환성

#### Step 8. Kafka Connect
- Source/Sink Connector 기본 동작

#### Step 9. Monitoring & Observability
- JMX 메트릭, Consumer Lag 모니터링

#### Step 10. Broker Internals & KRaft
- 리더 선출, ISR, KRaft 메타데이터

---

## 프로젝트 구조
```
src/test/java/com/example/kafka/
├── KafkaTestBase.java
├── producer/
│   ├── ProducerAcksTest.java
│   └── ProducerRecordStructureTest.java
├── consumer/
│   ├── ConsumerAckModeTest.java
│   ├── ConsumerAutoCommitTrapTest.java
│   ├── ConsumerOffsetResetTest.java
│   └── ConsumerLagBasicTest.java
├── partition/
│   ├── PartitionKeyTest.java
│   └── PartitionRekeyTest.java
├── dlq/
│   └── DefaultErrorHandlerTrapTest.java
└── eos/
    ├── IdempotentProducerTest.java
    ├── TransactionalProducerTest.java
    └── EOSBoundaryTest.java
```

## 설계 원칙
1. 테스트 이름 = 증명 명제 (`@DisplayNameGeneration(ReplaceUnderscores.class)` + 한글)
2. yml이 보여야 한다 — JavaDoc에 `yml 대응:` 명시
3. assertion이 명제를 직접 증명 — `assertThat(offset).isEqualTo(-1)`
4. 테스트가 자기완결 — 토픽 생성, 발행, 검증까지 한 메서드에서 완결
5. Before/After는 같은 클래스 안에 — 함정(Before)과 해결(After) 비교
6. KafkaTestBase로 격리 — UUID 기반 고유 토픽/그룹ID
