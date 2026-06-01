# Kafka, 직접 증명하며 읽는 책

> ⚠️ **러프 초안 — 전체 표지.**
> Kafka를 **개념으로 서술하고, 모든 핵심 주장을 돌아가는 테스트로 증명**한다. (executable book)
> 용어를 외우지 않고 **"왜 이렇게 설계할 수밖에 없었나"** 를 밑바닥부터 깎아 올라간다.

---

## 집필 원칙

- **개념이 먼저, 테스트는 증명이다.** 현상을 외우지 않고 제1원리에서 출발한다.
- **함정은 목적이 아니라 검증 도구다.** "안 되는 것"을 밟아보며 이해가 진짜인지 확인한다.
- **Mermaid 다이어그램을 적극 활용한다.** 구조·흐름·상태 전이는 글보다 그림으로 먼저 보여준다.
- 집필 규약·버전·스코프 → [CHARTER](../CHARTER.md) · [CONVENTIONS](../CONVENTIONS.md) · 진행 상태 → [ROADMAP](../ROADMAP.md)

### 📏 README 작성 규칙 (모든 권 공통)

각 권의 `README.md`는 **본문이 아니라 인덱스(지도)** 다.

- README의 각 불릿은 **"한 주제를 한 문장으로"** 표현한다. 상세 서술·다이어그램·코드·증명은 **개별 `NN-*.md`(산문화)** 에만 둔다.
- 워크플로우: README에서 무엇을 적을지 불릿로 정함 → 확정 장만 `NN-*.md`로 옮겨 산문화 → 산문에서 새 포인트가 생기면 **README 불릿(-D)에 한 문장으로 반영**(양방향 동기화).
- 각 권은 자기 **Scope/경계**를 README 상단에 명시한다. 다른 권 주제로 넘어가면 **링크만** 남기고 그 권에서 다룬다.

### 권의 경계 (한눈에 — AI/작업자용)

| 이 주제는… | …이 권에서 |
|------------|-----------|
| 왜 그렇게 동작하나 · 보장 · 구조 · 합의 알고리즘 | 📘 **I권 Internals** |
| 어떤 숫자로 운영 · 모니터링 · 사이징 · 장애 대응 | 📗 **II권 Operations** |
| Spring Kafka 코드 · 설정 조합/순서 함정 | 📙 **III권 Spring** |
| 이벤트 설계 · Outbox · Saga | (다른 lab: messaging-lab / saga-lab) |

---

## 세 권으로 나눈 이유

같은 주제라도 **독자의 목적**에 따라 알아야 할 깊이가 다르다. 그래서 한 권에 욱여넣지 않고 셋으로 나눈다.

```mermaid
graph LR
    subgraph 한 주제 예: 복제/ISR
        I["📘 I권 Internals<br/>왜 ISR인가<br/>무엇을 보장하나<br/>HW·LEO·leader epoch<br/>ISR ≠ 합의"]
        II["📗 II권 Operations<br/>RF/min.insync 기준<br/>ISR 축소 알람<br/>under-replicated 대응"]
        III["📙 III권 Spring<br/>acks 설정<br/>ProducerFactory<br/>@KafkaListener"]
    end
    I -->|"원리를 알면"| II
    II -->|"운영을 알면"| III
```

| 권 | 성격 | 핵심 질문 | 주 재료 |
|----|------|----------|---------|
| [📘 **I권 — Internals**](./1-internals/README.md) | deep-dive, 엄밀한 원리 | *왜 · 무엇을 보장 · 어떤 구조 · 어떤 합의 알고리즘* | 새로 집필 + `KAFKA-ARCHITECTURE.md` 분해 |
| [📗 **II권 — Operations**](./2-operations/README.md) | 실무 운영 | *현장에서 어떻게 운영/판단하는가* | Step 9·10 + 신규 |
| [📙 **III권 — Spring**](./3-spring/README.md) | Spring Kafka 적용 | *코드로 어떻게 쓰는가* | Step 1~8 (spring-kafka 기반) |

---

## 어떻게 읽나

- **원리를 알고 싶다** → I권부터. 분산 시스템의 문제의식에서 출발한다.
- **운영 중 판단이 필요하다** → II권. 각 항목이 I권의 어느 원리에 기대는지 링크로 잇는다.
- **당장 코드를 짜야 한다** → III권. 각 설정이 무엇을 보장하는지 I권으로 거슬러 올라갈 수 있다.

> 부록: [용어 사전(GLOSSARY)](../../GLOSSARY.md)
