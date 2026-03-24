# Step 10 — Broker Internals & KRaft

## 핵심 질문

> retention.ms를 실수로 잘못 설정하면 어떻게 되는가? — **AdminClient로 브로커 재시작 없이 동적으로 변경할 수 있다.**

## 이 디렉터리가 증명하는 것

### 토픽 설정 관리
- `describeConfigs()`: retention.ms, cleanup.policy, min.insync.replicas 조회
- `incrementalAlterConfigs()`: 브로커 재시작 없이 동적 변경
- source 필드로 설정 출처 구분: DEFAULT_CONFIG vs DYNAMIC_TOPIC_CONFIG

### KRaft 모드
- `KAFKA_PROCESS_ROLES=broker,controller` → ZooKeeper 없이 운영
- `describeCluster()` → 컨트롤러 노드, 클러스터 ID 확인
- Raft 프로토콜로 메타데이터 관리 → 운영 복잡도 감소

### 데이터 범위 & 토픽 관리
- Earliest Offset: retention으로 삭제된 후 남은 시작점
- Latest Offset (LEO): 다음에 쓰일 위치
- `listTopics()` / `deleteTopics()`: 토픽 목록 조회 및 정리

## 테스트 파일

| 파일 | 테스트 수 | 핵심 |
|------|---------|------|
| BrokerInternalsTest | 5 | 설정 조회/변경, KRaft, Offset 범위, 토픽 관리 |

## 운영 도구 대응

```bash
# 토픽 설정 조회
kafka-configs.sh --describe --entity-type topics --entity-name my-topic

# 토픽 설정 변경 (재시작 불필요)
kafka-configs.sh --alter --entity-type topics --entity-name my-topic \
  --add-config retention.ms=86400000

# KRaft 메타데이터 확인
kafka-metadata.sh --snapshot /path/to/metadata
```
