# CHARTER — kafka-lab 헌법

> 이 문서는 kafka-lab의 **변하지 않는 뼈대**다.
> 목적, 원칙, 버전, 스코프 경계를 정의한다.
> 다른 모든 문서(README, ROADMAP, 각 Step README)는 이 문서를 **참조**하며, 여기 정의된 내용을 재정의하지 않는다.
>
> **변경 빈도: 매우 낮음.** 이 문서가 자주 바뀐다면 설계가 흔들리고 있다는 신호다.

---

## 0. 한 줄 정의

**kafka-lab은 Kafka의 동작 원리와 함정을 "실행 가능한 테스트"로 증명하는, 영구 보존을 목표로 하는 학습 저장소다.**

읽고 끝나는 문서가 아니라, 함정을 직접 밟고(break) 설정으로 해결(fix)하는 과정을 코드로 남긴다.

---

## 1. 목적 (Why)

- Kafka를 "쓸 줄 안다"에서 "왜 그렇게 동작하는지 설명할 수 있다"로 끌어올린다.
- 공식 문서의 서술이 아니라, **내 환경에서 직접 재현한 증거**를 남긴다.
- 시간이 지나도(=Kafka 버전이 올라가도) 다시 돌려 검증할 수 있는 **재현 가능한 기억**을 만든다.

### 대상 독자

- Kafka를 실무에 쓰기 시작했거나 쓸 예정인 백엔드 개발자
- "되는 것"보다 "안 되는 것"을 미리 알고 싶은 사람
- 전제 지식: Java, Spring Boot 기본, JUnit 기반 테스트 읽기

---

## 2. 불변 원칙 (Invariants)

이 lab의 모든 코드/문서는 아래 원칙을 따른다. 위반하면 그것은 버그다.

1. **모든 명제는 테스트로 증명한다.** 산문으로만 설명하고 끝내지 않는다.
2. **테스트 이름이 곧 증명 명제다.** (`예외를_삼키면_offset이_커밋되어_메시지가_유실된다`)
   - 한글 서술형, "무엇이 일어나는가"를 문장으로 적는다.
3. **함정을 먼저 보여준다.** "잘 되는 것"이 아니라 "안 되는 것"을 먼저 재현하고, 그 다음 해결한다.
4. **각 Step은 하나의 잘못된 통념(착각 질문)에서 출발한다.** ("~하면 안전한 거 아닌가?")
5. **해결은 항상 설정(yml/properties)으로 매핑한다.** 실무 코드와 연결되지 않는 증명은 반쪽이다.
6. **테스트는 서로 격리된다.** 토픽명·Consumer Group ID는 테스트마다 고유 생성한다 (`KafkaTestBase`).
7. **SSOT를 지킨다.** 같은 사실(특히 버전)을 두 곳에 적지 않는다. → [§3 버전 매트릭스](#3-버전-매트릭스-single-source-of-truth)

---

## 3. 버전 매트릭스 (Single Source of Truth)

> ⚠️ **버전은 오직 이 표에서만 관리한다.**
> Step README, 본문 어디에도 버전 숫자를 적지 않고 이 표를 링크한다.
> 이 표를 바꾸면 아래 "동기화 대상 파일"도 함께 갱신한다.

### 현재 기준 (Baseline)

| 구성요소 | 버전 | 비고 |
|----------|------|------|
| Java | **21** | LTS, `build.gradle.kts` toolchain |
| Gradle 빌드 | Kotlin DSL | — |
| Spring Boot | **3.4.4** | `org.springframework.boot` 플러그인 |
| Spring dependency-management | **1.1.7** | — |
| spring-kafka | **3.3.x** | Spring Boot BOM 관리 (명시 버전 미고정) |
| Kafka client | **3.7.x** | spring-kafka가 가져오는 버전 |
| Apache Kafka (broker) | **3.7.0** | Docker 이미지 `apache/kafka:3.7.0` |
| 실행 모드 | **KRaft** | ZooKeeper 없음 |
| 테스트 | JUnit 5 + AssertJ | spring-boot-starter-test |
| JSON | Jackson (jackson-databind) | Step 7 |

### 브로커 기본 설정 (docker-compose 기준)

| 설정 | 값 | 영향받는 Step |
|------|-----|--------------|
| `num.partitions` (기본) | 3 | Step 3 |
| `default.replication.factor` | **1** | Step 1 (RF=1 함정의 근원) |
| `offsets.topic.replication.factor` | 1 | Step 2 |
| `transaction.state.log.replication.factor` | 1 | Step 6 |
| `transaction.state.log.min.isr` | 1 | Step 6 |
| `log.retention.hours` | 1 | Step 10 |
| `log.segment.bytes` | 104857600 (100MB) | Step 10 |
| `auto.create.topics.enable` | true | 전체 |
| Cluster ID | `kafka-lab-cluster-id-01` | Step 10 (KRaft 확인) |

### 동기화 대상 파일

버전/브로커 설정을 바꿀 때 반드시 함께 확인:

- `build.gradle.kts` — Java toolchain, Spring Boot 플러그인 버전
- `docker-compose.yml` — Kafka 이미지 태그, 브로커 환경변수
- `connect-distributed.properties` — Kafka Connect 설정 (Step 8)

### 버전 업그레이드 정책 (영구 저장소이기에 중요)

- 이 lab의 모든 증명은 **위 Baseline 버전 기준**이다.
- 버전을 올릴 때는:
  1. 이 표를 먼저 갱신하고 동기화 대상 파일을 맞춘다.
  2. 전체 테스트를 돌려 **깨지는 명제**를 찾는다. (깨짐 = Kafka 동작이 바뀌었다는 증거 → 그 자체가 학습 자산)
  3. 바뀐 동작은 해당 Step README의 "버전 노트"에 기록한다. 과거 동작을 지우지 않는다.
- 예: 기본 partitioner(Sticky), 기본 assignor 목록, `enable.idempotence` 기본값 등은 버전에 따라 변한다.

---

## 4. 스코프 경계 (다루는 것 / 다루지 않는 것)

### 다룬다 (In Scope)

애플리케이션 개발자가 Kafka를 **안전하게 쓰고 운영하는 관점**의 함정과 해결.
→ 구체 목록은 [ROADMAP.md](./ROADMAP.md) 참조.

### 다루지 않는다 (Out of Scope)

| 주제 | 이유 / 다루는 곳 |
|------|----------------|
| 왜 Kafka가 필요한가, 이벤트 설계, Outbox 구현, 이벤트 네이밍 | messaging-lab |
| Saga 패턴 | saga-lab (예정) |
| 처리량 벤치마크/튜닝 가이드 | throughput-lab (예정) |

### 보류 — ROADMAP에서 편입 검토 중 (Candidate)

> 아래는 "원래 안 다룸"이었으나, 영구 저장소로 디벨롭하며 **편입을 검토하는** 항목이다.
> 확정 시 [ROADMAP.md](./ROADMAP.md)의 정식 Step으로 옮기고 이 표에서 제거한다.

| 주제 | 현재 갭 | 비고 |
|------|---------|------|
| 멀티 브로커 클러스터 (리더 선출, ISR 축소/복구, reassignment) | 단일 브로커라 재현 불가 | 가장 큰 구조적 갭 |
| 로그 엔진 내부 (segment 파일, compaction, 압축, control batch) | 산문만 존재 | low↔high 연결 지점 |
| Schema Registry + Avro/Protobuf | JSON만 다룸 | Step 7 확장 또는 신규 |
| Kafka Streams / ksqlDB | 전무 | 신규 lab 가능성 |
| 보안 (SASL/SSL, ACL, mTLS) | 전무 | 별도 주제 |
| 고급 모니터링 (JMX→Prometheus/Grafana, 분산 트레이싱) | 메트릭 정의만 | Step 9 확장 |

---

## 5. 문서 거버넌스 (이 저장소의 문서 계층)

```
README.md            [입구]   무엇이고 어디로 가라. 네비게이션. 얇게 유지.
docs/
  CHARTER.md         [헌법]   ← 이 문서. 목적·원칙·버전·스코프. 거의 안 변함.
  ROADMAP.md         [지도]   Step 전체 흐름 + 의존 그래프 + 상태.
  CONVENTIONS.md     [규약]   Step README 템플릿, 네이밍, 커밋 규칙.
  KAFKA-ARCHITECTURE.md       분산 시스템/브로커 내부 개념.
  GLOSSARY.md                 용어 사전.
src/test/.../sNN_*/README.md  [로컬 명세]  상위 문서를 참조, 재정의 금지.
```

**원칙**: 위에서 아래로 참조한다. 아래 문서가 위 문서의 사실(버전·원칙·스코프)을 다시 적지 않는다.

---

## 6. 변경 이력 (Changelog)

> 헌법이 바뀌면 한 줄 남긴다. (날짜는 절대표기)

- 2026-06-01 — 최초 초안 작성. Baseline: Java 21 / Spring Boot 3.4.4 / Kafka 3.7.0 (KRaft).
