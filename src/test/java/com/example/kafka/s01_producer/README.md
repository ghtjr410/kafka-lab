# Step 1 — Producer Guarantee + Advanced

## 핵심 질문

> `acks=all`이면 안전한 거 아닌가? — **아니다. 브로커가 1대면 acks=1과 같다.**

## 이 디렉터리가 증명하는 것

### 함정 (Before)
- `acks=all` + `min.insync.replicas=1` = 리더 1대만 기록해도 응답 → acks=1과 동일
- `acks=0`은 브로커 응답을 안 기다려서 유실 가능
- `send()`는 비동기지만 **버퍼/메타데이터 대기는 블로킹** → 브로커 불능 시 스레드 멈춤

### 해결 (After)
- `acks=all` + `min.insync.replicas=2` + RF=3 → 진짜 안전
- `linger.ms` + `batch.size`로 배치 효과 → 처리량 향상
- `flush()`로 linger.ms 무시하고 즉시 전송
- `max.block.ms` 설정으로 타임아웃 제어

## 테스트 파일

| 파일 | 테스트 수 | 핵심 |
|------|---------|------|
| ProducerAcksTest | 5 | acks=0/1/all 차이, min.insync.replicas 함정 |
| ProducerRecordStructureTest | 2 | RecordMetadata 구조, Header correlation-id |
| ProducerBatchingTest | 3 | linger.ms 배치, flush() 즉시 전송 |
| ProducerBackpressureTest | 2 | max.block.ms 타임아웃, buffer.memory 비동기 |

## yml 대응

```yaml
spring.kafka.producer:
  acks: all                          # 0, 1, all
  properties:
    min.insync.replicas: 2           # acks=all일 때만 의미
    linger.ms: 5                     # 배치 대기 시간
    batch.size: 16384                # 배치 크기 (바이트)
    buffer.memory: 33554432          # 전체 버퍼 (기본 32MB)
    max.block.ms: 60000              # 버퍼/메타데이터 대기 타임아웃
```
