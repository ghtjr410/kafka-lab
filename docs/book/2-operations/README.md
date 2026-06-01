# 📗 II권 — Operations (실무 운영)

> ⚠️ **러프 초안 — II권 목차 골격.**
> *"현장에서 어떻게 운영하고 무엇을 기준으로 판단하는가."*
> 각 항목은 **I권의 어느 원리에 기대는지** 링크로 잇는다 (현상 암기 ❌).

---

## 이 권의 성격

```mermaid
graph LR
    I["📘 I권<br/>왜 ISR인가"] -->|"원리 →"| OP["📗 II권<br/>그래서 RF=3·min.isr=2로 잡고<br/>ISR 축소를 알람 건다"]
```

I권이 "왜"라면, II권은 "그래서 운영에서 **어떤 숫자로, 어떻게 감시하고, 터지면 어떻게**"를 다룬다.

---

## 목차 (운영 축)

| 장 | 제목 | 다루는 핵심 | 재료 | 상태 |
|----|------|------------|------|------|
| 1장 | 클러스터 사이징 | 브로커 수 · 파티션 수 결정 기준 · rack awareness | 신규 | 📋 예정 |
| 2장 | 내구성 운영 기준 | RF / `min.insync.replicas` / `acks` 조합, unclean leader election | I권 3장 기반 | 📋 예정 |
| 3장 | 모니터링 & 관측 | Consumer Lag · under-replicated · JMX → Prometheus/Grafana | Step 9 재료 | 🚧 일부 |
| 4장 | 토픽/브로커 설정 운영 | retention · `incrementalAlterConfigs` 동적 변경 · compaction 정책 | Step 10 재료 | 🚧 일부 |
| 5장 | 장애 대응 | 리더 선출 · ISR 복구 · partition reassignment · preferred leader | 신규(멀티브로커) | 📋 예정 |
| 6장 | 용량/보존 운영 | 디스크 · throttling/quotas · tiered storage | 신규 | 📋 예정 |

> 대부분 **멀티브로커 환경**을 전제로 한다. → [ROADMAP](../../ROADMAP.md)

---

← [전체 표지](../README.md) · [I권](../1-internals/README.md) · [III권](../3-spring/README.md)
