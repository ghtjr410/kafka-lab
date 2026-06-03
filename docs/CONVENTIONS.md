# CONVENTIONS — kafka-lab 작성 규약

> 이 문서는 **"바로 들어가면 난잡해진다"는 문제를 막는 틀**이다.
> Step 작업을 시작하기 전에 이 규약을 고정해두면, 새 Step을 추가해도 기존 것이 흔들리지 않는다.
> 목적·버전·스코프는 [CHARTER.md](./CHARTER.md), 전체 흐름은 [ROADMAP.md](./ROADMAP.md) 참조.

---

## 1. 디렉터리 / 패키지 규칙

```
src/test/java/com/example/kafka/
├── KafkaTestBase.java          공통 베이스 (토픽/그룹ID 격리, 헬퍼)
└── sNN_topic/                  Step 폴더: 2자리 번호 + 짧은 주제명 (snake_case)
    ├── README.md               Step 로컬 명세 (아래 §3 템플릿)
    └── XxxTest.java            테스트 클래스 (PascalCase + Test 접미사)
```

- 폴더명: `s01_producer`, `s10_broker`처럼 **2자리 zero-padding 번호** + `_` + 주제.
- 한 Step에 테스트 클래스가 여러 개면 **관심사별로 분리**한다 (예: `ProducerAcksTest`, `ProducerBatchingTest`).
- `src/main`은 비워둔다. 이 lab은 **테스트로만 증명**한다.

---

## 2. 테스트 작성 규약

### 2.1 테스트 이름 = 증명 명제

- **한글 서술형 문장.** "무엇이 일어나는가"를 그대로 적는다.
- 함정 테스트는 결과를 직접 명시한다.

```java
@Test
void acks_all에_min_insync_replicas_1이면_사실상_acks_1이다() { ... }   // 함정

@Test
void seek으로_특정_offset부터_재소비할_수_있다() { ... }                 // 해결/능력
```

- 클래스명은 영어 PascalCase, 메서드명은 한글 명제. (기존 컨벤션 유지)

### 2.2 테스트 본문 구조 (Given–When–Then)

```java
// given: 함정을 유발할 설정/상황을 만든다
// when: 동작을 실행한다
// then: 무엇이 깨졌는지 / 해결됐는지를 assertion으로 증명한다
```

- assertion은 **AssertJ** (`assertThat`)로 통일한다.
- "함정 → 해결" 쌍이 한 Step에 함께 있어야 한다. 함정만 보여주고 끝내지 않는다.

### 2.3 격리

- 토픽명·Consumer Group ID는 **테스트마다 고유 생성** (`KafkaTestBase` 헬퍼 사용). 하드코딩 금지.
- 테스트 간 상태 공유 금지. 순서에 의존하지 않는다.

### 2.4 설정 매핑 주석

- 핵심 설정에는 **실무 yml/properties 대응**을 주석으로 단다.

```java
// 실무: spring.kafka.producer.acks=all
props.put(ProducerConfig.ACKS_CONFIG, "all");
```

### 2.5 버전 의존 동작

- 버전에 따라 달라지는 기본값(partitioner, assignor, `enable.idempotence` 등)은
  **주석에 기준 버전을 명시**한다. → 버전 숫자 자체는 [CHARTER 버전 매트릭스](./CHARTER.md#3-버전-매트릭스-single-source-of-truth) 참조.

```java
// Kafka 3.0+ 기본값: enable.idempotence=true → acks=all 강제 (CHARTER 버전 매트릭스 기준)
```

---

## 3. Step README 템플릿

> 모든 Step README는 이 구조를 따른다. 섹션 순서·제목을 바꾸지 않는다.
> 복사해서 채운다.

```markdown
# Step NN — <제목>

> 착각 질문: "<~하면 안전한/되는 거 아닌가?>"
> 한 줄 명제: <무엇이 깨지고, 무엇으로 해결되는가>
> 선행: [Step X](../sXX_xxx/README.md) · 관련 용어: [GLOSSARY](../../../../../../../docs/GLOSSARY.md)

## 직접 답해보자
- (학습자가 테스트를 보기 전에 먼저 답해볼 질문 2~4개)

## 함정 — 무엇이 깨지는가
- (잘못된 통념과, 실제로 무슨 일이 일어나는지)

## 증명 테스트 목록
| 테스트 | 증명하는 것 |
|--------|------------|
| `메서드명` | ... |

## 해결 — 설정 매핑
| 상황 | 설정 (yml/properties) | 효과 |
|------|----------------------|------|
| ... | `spring.kafka...` | ... |

## 버전 노트 (선택)
- (이 Step의 동작이 Kafka 버전에 따라 달라지는 지점. 기준 버전 = CHARTER 매트릭스)

## 참조
- 아키텍처: [KAFKA-ARCHITECTURE.md](...)
- 다음 Step: ...
```

---

## 4. 문서 참조 규칙 (SSOT 유지)

- **버전 숫자**는 본문에 적지 않는다 → [CHARTER 버전 매트릭스](./CHARTER.md#3-버전-매트릭스-single-source-of-truth) 링크.
- **스코프(다룬다/안 다룬다)** 는 CHARTER §4에만 둔다.
- **Step 간 순서·의존**은 ROADMAP에만 둔다. Step README는 "선행 Step" 링크만 건다.
- 용어 정의는 GLOSSARY에만 둔다. 본문에서는 첫 등장 시 링크.
- 같은 사실이 두 곳에 보이면 → 하나를 지우고 링크로 바꾼다.

---

## 5. 커밋 규약

- 형식: `<type>: <한글 요약>`
- type: `feat`(새 Step/테스트), `fix`(테스트/동작 수정), `docs`(문서), `refactor`, `chore`
- **한 커밋 = 한 가지 일.** Step 코드와 문서 골격 변경을 섞지 않는다.
- 예:
  - `feat: Step 11 멀티브로커 클러스터 ISR 축소 테스트 추가`
  - `docs: CHARTER 버전 매트릭스를 Kafka 3.8로 갱신`

---

## 6. 새 Step 추가 체크리스트

1. [ ] ROADMAP에서 명제·번호·의존관계 확정 (💡 → 📋)
2. [ ] `sNN_topic/` 폴더 생성, README 템플릿(§3) 채우기 — **테스트 이름부터 정의**
3. [ ] 테스트 작성 (함정 → 해결 쌍, AssertJ, 격리, 설정 주석)
4. [ ] Baseline 버전에서 전체 통과 확인
5. [ ] 새 용어가 있으면 GLOSSARY에 추가
6. [ ] README.md(입구)의 테스트 목록/구조 갱신
7. [ ] ROADMAP 상태 ✅로, 필요 시 CHARTER §4 Candidate에서 제거

---

## 7. 변경 이력

- 2026-06-01 — 최초 초안. Step README 템플릿, 테스트/커밋 규약 정의.
