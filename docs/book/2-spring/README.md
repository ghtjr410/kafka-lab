# 📙 II권 — Spring Kafka (코드로 어떻게 쓰는가)

> ⚠️ **러프 초안 — II권 목차 골격.**
> *"Spring Kafka로 실제 코드를 어떻게 짜는가, 그리고 어디서 데이는가."*
> 기존 함정 Step(s01~s07)이 이 권의 주 재료다. 각 설정이 무엇을 보장하는지는 **I권으로 거슬러** 올라간다.
>
> 📐 집필 공통 규칙은 [전체 표지](../README.md)를 따른다. 특히 🔒 **다른 장·권은 named link로만 — 장/권 번호를 본문에 박지 말 것.**

---

## 이 권의 역할 · Scope · 경계

**목적**: Spring Kafka로 **코드를 어떻게 짜고 어디서 데이나**. 개별 설정 → 설정 조합 → 코드 구조·순서 함정.

### 다룬다 (Scope)
- Producer/Consumer/Listener 코드·설정 · 설정 조합 함정 · 코드 구조·순서 함정 · 직렬화 (기존 Step s01~s07)

### 다루지 않는다 (Out of Scope)
- 내부 원리(보장·알고리즘) → [I권](../1-internals/README.md) · 운영 절차·모니터링·사이징·**보안(SASL/SSL/ACL)** → [III권](../3-operations/README.md)
- **Kafka Connect / CDC / Schema Registry / Streams** → [IV권](../4-beyond-core/README.md) (인프라·플랫폼 영역)

> 🤖 "왜 그런가"는 [I권](../1-internals/README.md), "운영 기준"은 [III권](../3-operations/README.md)에 **링크만**. 여기선 코드 관점만 깎는다.

---

## 이 권이 다루는 함정의 두 차원

함정은 "설정 하나 잘못 넣는 것"만이 아니다. 더 무서운 건 **개별로는 맞는데 조합·순서가 틀린 것**이다.

```mermaid
graph TB
    subgraph C1["① 설정 조합 함정 (개별 유효, 조합이 위험)"]
        A["enable.idempotence=true"] -.요구.-> B["acks=all"]
        A -.요구.-> M["max.in.flight ≤ 5"]
        EX["explicit true + acks=1<br/>→ ConfigException (생성 실패=시끄러워서 안전)"]
        SD["default 의존 + acks=1<br/>→ 침묵 disable (경고 없음=진짜 함정 ⚠️)"]
    end
    subgraph C2["② 코드 구조·순서 함정 (레이어 순서)"]
        R["retry"] --> D["DLQ/외부호출"]
        W2["순서·분류가 틀리면<br/>1번 실패를 N번 재시도·집계 ⚠️"]
    end
```

> **resilience4j 비유**: `retry`가 앞단, `circuitbreaker`가 뒷단이면 1번 실패할 것을 N번 시도해 N번이 다 서킷에 집계된다.
> Kafka에도 똑같은 형태가 있다 — non-retryable 예외(역직렬화 실패=poison-pill)를 분류하지 않으면 **절대 성공 못 할 메시지를 끝까지 헛재시도**한다(정확한 횟수·원리는 [10장](./10-code-order-traps.md)). (형제 [resilience4j-lab](../../../../resilience4j-lab/)과 연결)

**위 두 차원(① 조합 · ② 순서)에 그 베이스인 `개별 설정의 의미`를 더하면, II권의 각 장은 3관점으로 깎인다:**
`개별 설정의 의미`(베이스) → `설정 조합`(차원① = 위 C1) → `코드 구조·순서`(차원② = 위 C2)

---

## 이 권의 성격

함정은 버리지 않는다 — **원리를 알고 나서 보는 "현실에서 깨지는 증거"** 로 재배치된다. 같은 사실의 세 얼굴이다: I권 `커밋의 정의 = HW`(원리) → II권 `acks=all` + `ProducerFactory`(코드) → 기본 `AckMode(BATCH)`에서 예외를 삼키면 offset이 커밋되어 **유실**(함정).

---

## 증명 모델 · 상태

- **🧪 증명 모델**: II권의 증명은 **Spring Kafka 통합 테스트**(기존 Step 테스트)다. I권의 `docker`/CLI 자체증명과 달리 **프레임워크에 의존**한다 — 그래서 이 권에 속한다. (표지 "증명 모델 명시" 규칙)
- **🚦 상태 (이 섹션이 II권 상태 SSOT — 2축)**:
  - **[산문]** 본편(1~7장)=기존 Step README(✅ 확정) · 횡단편 9·10장=✅ 작성 · 11장=인덱스 ✅
  - **[증명]** 본편=기존 Step 테스트(🧪 있음) · 횡단편=⬜ 미구현(본편 Step 테스트를 링크로 참조)

---

## 목차 (Spring 적용 축)

### 본편 — 기존 Step 재사용(📄)

| 장 | 제목 | 착각 질문 | 본문 위치 | 테스트 |
|----|------|----------|----------|--------|
| 1장 | Producer 보장 | "acks=all이면 안전한가?" | 📄 `s01_producer/README.md` | 🧪 12 |
| 2장 | Consumer & Offset | "예외를 삼키면 안전한가?" | 📄 `s02_consumer/README.md` | 🧪 14 |
| 3장 | Partition & concurrency | "파티션 늘리면 좋은가?" | 📄 `s03_partition/README.md` | 🧪 6 |
| 4장 | Rebalancing & 배포 | "롤링 배포 시 왜 멈추나?" | 📄 `s04_rebalancing/README.md` | 🧪 6 |
| 5장 | 에러 처리 & DLQ | "기본 핸들러가 DLQ로 보내나?" | 📄 `s05_dlq/README.md` | 🧪 2 |
| 6장 | EOS & 트랜잭션 | "EOS면 중복 없나?" | 📄 `s06_eos/README.md` | 🧪 8 |
| 7장 | 직렬화 & 스키마 진화<br/>*(중앙 스키마 Registry는 [IV권](../4-beyond-core/README.md))* | "필드 추가했는데 왜 죽나?" | 📄 `s07_serialization/README.md` | 🧪 7 |

> **8장은 결번.** `s08_connect`(Kafka Connect)는 인프라/통합 주제라 → [IV권](../4-beyond-core/README.md)으로 이동했다. `s09_monitoring`·`s10_broker`(운영 성격)는 → [III권](../3-operations/README.md)으로 분류.
> *(챕터 번호와 Step 번호 `sNN`은 별개 축이다 — Step 이동으로 8장이 비고, 횡단편은 9장부터 잇는다.)*

### 횡단편 — 설정·코드 차원의 함정 (신규, 9~11장)

| 장 | 제목 | 다루는 핵심 | 산문 상태 |
|----|------|------------|----------|
| 9장 | **[설정 조합의 함정](./09-config-combination-traps.md)** | idempotence ↔ acks ↔ max.in.flight / order ↔ retry / `session·heartbeat·max.poll` 3박자 타이밍 / transactional ↔ isolation.level | ✅ 산문 |
| 10장 | **[코드 구조·순서의 함정](./10-code-order-traps.md)** | ErrorHandler retry↔DLQ 순서·non-retryable 분류 / commit 위치(처리 전 vs 후) / `@RetryableTopic` non-blocking retry / `@Transactional`+Kafka 트랜잭션 경계 / 리스너 내 blocking 호출 → poll 초과 | ✅ 산문 |
| 11장 | **[설정 레퍼런스(인덱스)](./11-config-reference.md)** | Producer/Consumer/Listener 설정을 한 곳에서 인덱싱 (의미는 본편·9·10장 SSOT, 기본값은 검증된 것만 ✓) | ✅ 인덱스 |

---

← [전체 표지](../README.md) · [I권](../1-internals/README.md) · [III권](../3-operations/README.md)
