# Step 3 — Partition & Ordering + Advanced

## 핵심 질문

> 파티션 수를 늘리면 처리량이 올라가니까 좋은 거 아닌가? — **기존 key의 파티션 매핑이 깨진다.**

## 이 디렉터리가 증명하는 것

### 함정 (Before)
- 파티션 수 변경 → murmur2 해시 결과 달라짐 → 같은 key가 다른 파티션으로 → **순서 보장 파괴**
- Consumer 수 > 파티션 수 → 초과 Consumer는 **파티션 없이 놀림 (IDLE)**
- 병렬 처리 상한 = 파티션 수 (Consumer를 아무리 늘려도 소용없음)

### 해결 (After)
- 같은 key → 항상 같은 파티션 → 파티션 내 순서 보장
- Consumer 수 = 파티션 수 → 1:1 이상적 분배
- 파티션 수는 처음에 충분히 설정하고 변경하지 않는 것이 원칙

## 테스트 파일

| 파일 | 테스트 수 | 핵심 |
|------|---------|------|
| PartitionKeyTest | 3 | null key 분산, 같은 key 같은 파티션, 다른 key 다른 파티션 |
| PartitionRekeyTest | 2 | 파티션 수 변경 시 매핑 깨짐 |
| PartitionConsumerTest | 2 | Consumer > 파티션 → IDLE, Consumer = 파티션 → 1:1 |

## yml 대응

```yaml
# 브로커 설정
KAFKA_NUM_PARTITIONS: 3              # 기본 파티션 수
# 토픽별 파티션 수는 AdminClient.createTopics()로 지정
```
