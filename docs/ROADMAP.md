# ROADMAP — kafka-lab 전체 흐름과 지도

> 이 문서는 **큰 흐름(Step 간 순서·의존성)** 과 **세부 흐름(각 Step의 명제·상태)** 을 정의한다.
> 원칙·버전·스코프는 [CHARTER.md](./CHARTER.md)를, 작성 규약은 [CONVENTIONS.md](./CONVENTIONS.md)를 따른다.
>
> **각 Step의 상세 명제는 해당 Step README가 SSOT다.** 여기서는 한 줄 요약과 의존 관계만 관리한다.

---

## 1. 상태 범례

| 표기 | 의미 |
|------|------|
| ✅ 완료 | 테스트 + README 존재, Baseline 버전에서 통과 |
| 🚧 작업중 | 일부만 존재하거나 보강 진행 중 |
| 📋 예정 | 명제만 정의, 코드 없음 |
| 💡 후보 | 편입 검토 중 (CHARTER §4 Candidate) |

---

## 2. 전체 지도

### Part 1 — 메시지를 안전하게 보내고 받고, 장애에 대응한다

| Step | 폴더 | 착각 질문 | 핵심 명제 | 상태 |
|------|------|----------|----------|------|
| 1 | `s01_producer` | "acks=all이면 안전한 거 아닌가?" | RF=1·min.insync.replicas=1이면 acks=1로 퇴화 | ✅ |
| 2 | `s02_consumer` | "예외를 try-catch로 삼키면 안전한 거 아닌가?" | 기본 AckMode(BATCH)에서 offset 커밋 → 메시지 유실 | ✅ |
| 3 | `s03_partition` | "파티션 늘리면 처리량 올라가니 좋은 거 아닌가?" | 파티션 수 변경 시 key 매핑이 깨진다 (rekey) | ✅ |
| 4 | `s04_rebalancing` | "롤링 배포 시 왜 순간 멈추는가?" | Eager 리밸런싱이 전 파티션을 회수 (stop-the-world) | ✅ |
| 5 | `s05_dlq` | "기본 에러 핸들러가 DLQ로 보내주는 거 아닌가?" | 아니다. 10회 시도 후 조용히 skip | ✅ |
| 6 | `s06_eos` | "Kafka가 Exactly-Once 지원하니 중복 걱정 없는 거 아닌가?" | EOS는 Kafka 내부에서만 보장 | ✅ |

### Part 2 — 직렬화, 파이프라인, 모니터링, 브로커 내부

| Step | 폴더 | 착각 질문 | 핵심 명제 | 상태 |
|------|------|----------|----------|------|
| 7 | `s07_serialization` | "필드 하나 추가했는데 왜 Consumer가 죽는가?" | ObjectMapper 기본값이 미지 필드를 거부 | ✅ |
| 8 | `s08_connect` | "DB 변경을 Kafka로 보내려면 Producer를 짜야 하나?" | Kafka Connect로 설정만으로 파이프라인 구성 | ✅ |
| 9 | `s09_monitoring` | "처리를 못 따라가는 걸 어떻게 아는가?" | Consumer Lag = LEO − Committed Offset | ✅ |
| 10 | `s10_broker` | "retention.ms를 잘못 설정하면?" | AdminClient로 재시작 없이 동적 변경 | ✅ |

### Part 3 — 심화 (후보, 영구 저장소 확장용)

> 아래는 [CHARTER.md §4 Candidate](./CHARTER.md#4-스코프-경계-다루는-것--다루지-않는-것)에서 편입 검토 중인 항목.
> 정식 채택 시 번호를 확정하고 상태를 📋로 승격한다.

| Step(가칭) | 폴더(가칭) | 착각 질문 | 핵심 명제 후보 | 상태 |
|-----------|-----------|----------|---------------|------|
| 11 | `s11_cluster` | "단일 브로커나 3대나 같은 거 아닌가?" | 멀티브로커에서만 ISR 축소·리더 선출·reassignment가 보인다 | 💡 |
| 12 | `s12_logengine` | "트랜잭션 abort된 메시지는 어디로 가나?" | segment 파일/control batch/compaction을 직접 열어 low↔high를 잇는다 | 💡 |
| 13 | `s13_schema_registry` | "JSON으로 충분한 거 아닌가?" | Schema Registry + Avro로 강제 호환성 검증 | 💡 |
| 14 | `s14_streams` | "Consumer로 집계하면 되는 거 아닌가?" | Kafka Streams의 상태 저장/윈도우/exactly-once | 💡 |

---

## 3. 의존 그래프

> "어떤 Step을 이해하려면 무엇이 선행되어야 하는가." 순서를 강제하진 않지만, 명제가 다른 Step의 개념에 기댄다.

```
s01 (acks/ISR) ──┬─────────────► s06 (EOS: 멱등/트랜잭션)
                 │                  ▲
s02 (offset) ────┴──► s04 (rebalancing) ──► s05 (DLQ)
   │                       │
   │                       └──► s09 (monitoring: lag)
   ▼
s03 (partition/ordering) ──► s04
s07 (serialization) ──► s05 (역직렬화 예외 → DLQ)
s10 (broker config) ◄── s09 (ISR/offset 조회 기반)

[심화]
s01 + s06  ──► s12 (log engine: control batch, LSO)
s01        ──► s11 (cluster: ISR/leader election)  ← RF=1 한계를 푸는 Step
s07        ──► s13 (schema registry)
s02 + s03  ──► s14 (streams)
```

**핵심 선행 관계**
- **s06(EOS)** 는 s01(acks/ISR)·s02(offset)·s04(중복) 개념 위에 선다.
- **s11(cluster)** 은 s01의 "RF=1 함정"을 멀티브로커에서 실제로 재현하는 Step → s01의 자연스러운 확장.
- **s12(log engine)** 은 s06의 "read_committed가 abort 메시지를 거른다"를 로그 레벨에서 설명 → low↔high 연결.

---

## 4. 작업 진행 규칙

1. 새 Step은 **명제(테스트 이름)부터** 정의한다. 코드보다 "무엇을 증명할지"가 먼저다.
2. Step README는 [CONVENTIONS.md](./CONVENTIONS.md)의 템플릿을 채운다.
3. 한 Step을 끝내기 전 다른 Step으로 넘어가지 않는다 (수정 범위 분산 방지).
4. 후보(💡) Step을 정식 채택하면: 번호 확정 → CHARTER §4 Candidate에서 제거 → 이 표의 상태를 📋로 → 작업 시작.

---

## 5. 변경 이력

- 2026-06-01 — 최초 초안. Step 1~10 ✅, 심화 Step 11~14 후보(💡) 등록.
