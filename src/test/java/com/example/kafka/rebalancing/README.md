# Step 4 — Rebalancing

## 핵심 질문

> Consumer를 롤링 배포하면 왜 순간적으로 처리가 멈추는가? — **Eager 리밸런싱이 모든 파티션을 회수하기 때문이다.**

## 이 디렉터리가 증명하는 것

### 함정 (Before)
- **Eager(RangeAssignor)**: Consumer 합류/이탈 시 전체 파티션 revoke → stop-the-world
- **Dynamic Membership**: 재접속할 때마다 새 멤버로 인식 → 매번 리밸런싱
- **max.poll.interval.ms 초과**: 처리 시간이 길면 Consumer 강제 퇴출 → 미커밋 메시지 재처리 (중복)

### 해결 (After)
- **CooperativeStickyAssignor**: 이동이 필요한 파티션만 revoke → 나머지는 계속 처리
- **group.instance.id (Static Membership)**: 같은 ID로 재접속 → 리밸런싱 없이 파티션 유지
- **max.poll.records 줄이기**: 한 번에 적게 가져와서 처리 시간 < max.poll.interval.ms 유지

## 테스트 파일

| 파일 | 테스트 수 | 핵심 |
|------|---------|------|
| RebalancingEagerVsCooperativeTest | 2 | Eager 전체 revoke vs Cooperative 부분 revoke |
| StaticMembershipTest | 2 | Dynamic 매번 리밸런싱 vs Static 파티션 유지 |
| MaxPollIntervalTest | 2 | poll interval 초과 → 퇴출, max-poll-records로 방지 |

## yml 대응

```yaml
spring.kafka.consumer:
  properties:
    partition.assignment.strategy: org.apache.kafka.clients.consumer.CooperativeStickyAssignor
    group.instance.id: ${HOSTNAME}     # Kubernetes에서 권장
    max.poll.interval.ms: 300000       # 기본 5분
    max.poll.records: 500              # 기본 500
    session.timeout.ms: 45000
    heartbeat.interval.ms: 3000
```
