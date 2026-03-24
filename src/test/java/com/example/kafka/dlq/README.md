# Step 5 — DLQ & Error Handling

## 핵심 질문

> Spring Kafka의 기본 에러 핸들러가 DLQ로 보내주는 거 아닌가? — **아니다. 기본 동작은 10회 재시도 후 skip(버림)이다.**

## 이 디렉터리가 증명하는 것

### 함정 (Before)
- `DefaultErrorHandler` 기본 동작: `FixedBackOff(0L, 9)` → 10회 재시도 → **skip (로그만 남김)**
- DLQ로 보내려면 `DeadLetterPublishingRecoverer`를 명시적으로 설정해야 함
- 설정 없이 운영하면 실패 메시지가 조용히 사라진다

### 해결 (After)
- `DeadLetterPublishingRecoverer` + `DefaultErrorHandler` 조합
- 실패 메시지가 `{원본토픽}.DLT` 토픽으로 이동
- DLT 토픽을 별도 Consumer가 모니터링/재처리

## 테스트 파일

| 파일 | 테스트 수 | 핵심 |
|------|---------|------|
| DefaultErrorHandlerTrapTest | 2 | 기본 동작은 skip, DLQ 설정 시 .DLT 토픽 이동 |

## yml 대응

```yaml
# Spring Kafka 기본: DLQ 없음 (skip)
# DLQ 설정은 코드로 Bean 등록 필요:
#   @Bean DefaultErrorHandler(DeadLetterPublishingRecoverer, FixedBackOff)
```
