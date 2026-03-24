# Step 9 — Monitoring & Observability

## 핵심 질문

> Consumer가 처리를 못 따라가는 걸 어떻게 알 수 있는가? — **Consumer Lag = LEO - Committed Offset.**

## 이 디렉터리가 증명하는 것

### Consumer Lag 모니터링
- `AdminClient.listConsumerGroupOffsets()` → Committed Offset
- `AdminClient.listOffsets(OffsetSpec.latest())` → Log End Offset (LEO)
- **Lag = LEO - Committed** → 밀린 메시지 수
- Consumer 중단 시 Committed 고정 + LEO 증가 → **Lag 폭증**
- 멀티파티션: 파티션별 Lag 합산 = 전체 그룹 Lag

### 클러스터 & 클라이언트 메트릭
- `describeCluster()`: 브로커 수, 컨트롤러 노드
- `describeTopics()`: 파티션별 리더, ISR, Replicas
- `KafkaProducer.metrics()`: record-send-rate, request-latency
- `KafkaConsumer.metrics()`: records-consumed-rate, records-lag-max

## 테스트 파일

| 파일 | 테스트 수 | 핵심 |
|------|---------|------|
| ConsumerLagMonitoringTest | 3 | Lag 계산, lag 폭증, 멀티파티션 합산 |
| BrokerMetricsTest | 4 | 클러스터 정보, ISR, Producer/Consumer 메트릭 |

## 운영 도구 대응

```bash
# Consumer Lag 확인
kafka-consumer-groups.sh --describe --group my-group --bootstrap-server localhost:9092

# 토픽 상태 확인
kafka-topics.sh --describe --topic my-topic --bootstrap-server localhost:9092
```

## 알림 조건 예시

| 메트릭 | 임계값 | 의미 |
|--------|-------|------|
| Consumer Lag > 10000 | 위험 | Consumer 처리 속도 부족 |
| Lag 증가율 > 100/sec | 경고 | Producer 급증 or Consumer 장애 |
| ISR < Replicas | 긴급 | under-replicated → 데이터 유실 위험 |
| request-latency-avg > 100ms | 경고 | 브로커 과부하 |
