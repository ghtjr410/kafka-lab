# Step 2 — Consumer Offset + Advanced

## 핵심 질문

> 예외를 try-catch로 삼키면 안전한 거 아닌가? — **아니다. offset이 커밋되어 메시지가 영원히 유실된다.**

## 이 디렉터리가 증명하는 것

### 함정 (Before)
- `@KafkaListener`에서 예외를 삼키면 → offset 커밋 → 재소비 불가 → **데이터 유실**
- `enable.auto.commit=true`(native 기본값)면 처리 완료와 무관하게 커밋 → 유실
- `auto.offset.reset=latest`면 과거 메시지를 못 읽음

### 해결 (After)
- Spring Kafka가 `enable.auto.commit=false`로 강제 → AckMode로 제어
- AckMode.RECORD: 레코드마다 커밋 (가장 안전, 성능 ↓)
- AckMode.MANUAL_IMMEDIATE: 원하는 시점에 명시적 커밋
- `seekToBeginning()` / `seek(offset)`: 장애 시 재소비
- `AdminClient.alterConsumerGroupOffsets()`: Consumer 중단 후 offset 수동 변경

## 테스트 파일

| 파일 | 테스트 수 | 핵심 |
|------|---------|------|
| ConsumerAckModeTest | 4 | BATCH/RECORD/MANUAL, 예외 삼키기 함정 |
| ConsumerAutoCommitTrapTest | 3 | auto-commit 유실, manual commit, Spring 강제 |
| ConsumerOffsetResetTest | 2 | earliest vs latest |
| ConsumerLagBasicTest | 2 | LEO - Committed = Lag, lag 누적 |
| ConsumerOffsetResetToolTest | 3 | seek, seekToBeginning, alterConsumerGroupOffsets |

## yml 대응

```yaml
spring.kafka.consumer:
  enable-auto-commit: false          # Spring Kafka가 강제 (기본)
  auto-offset-reset: earliest        # earliest / latest
spring.kafka.listener:
  ack-mode: BATCH                    # BATCH / RECORD / MANUAL_IMMEDIATE
```
