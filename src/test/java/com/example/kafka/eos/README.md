# Step 6 — Exactly-Once Semantics

## 핵심 질문

> Kafka가 Exactly-Once를 지원하니까 중복 걱정 없는 거 아닌가? — **아니다. EOS는 Kafka 내부에서만 보장된다.**

## 이 디렉터리가 증명하는 것

### 함정 (Before)
- **멱등 프로듀서**: 세션(PID) 내에서만 중복 방지 → 프로듀서 재시작 시 새 세션 → **중복 발생**
- **트랜잭셔널 프로듀서**: `isolation.level` 기본값이 `read_uncommitted` → 커밋 안 된 메시지도 보임
- **EOS 경계**: Kafka → Kafka는 EOS 가능, Kafka → DB는 EOS 범위 밖 → **DB 중복 가능**

### 해결 (After)
- `isolation.level=read_committed`으로 Consumer 설정 변경
- 트랜잭셔널 프로듀서의 `beginTransaction()` → `commitTransaction()` → 원자적 발행
- DB 중복 방어: Consumer 멱등키(event_id unique) 사용

## 테스트 파일

| 파일 | 테스트 수 | 핵심 |
|------|---------|------|
| IdempotentProducerTest | 2 | 멱등 프로듀서 세션 내 중복 방지, 재시작 시 중복 |
| TransactionalProducerTest | 3 | 원자적 발행, abort 필터링, read_committed vs read_uncommitted |
| EOSBoundaryTest | 3 | Kafka 내부 EOS, DB 중복(범위 밖), Consumer 멱등키 방어 |

## yml 대응

```yaml
spring.kafka.producer:
  transaction-id-prefix: tx-          # 트랜잭셔널 프로듀서 활성화
  acks: all                           # 트랜잭션 시 필수

spring.kafka.consumer:
  isolation-level: read_committed     # 기본값은 read_uncommitted!
```
