# kafka-lab

> Learn how to operate Kafka safely by stepping on every trap yourself.
> Misconfigured settings cause silent data loss, duplicate processing, and unnoticed outages — break things first, then fix them.

---

## 이 lab의 역할

Kafka의 **"동작 원리"**를 체험하는 lab입니다.
설정 하나의 차이가 데이터 유실, 중복 처리, 운영 사고로 이어지는 것을 직접 밟아봅니다.
"왜 Kafka를 쓰는가"는 messaging-lab에서 다루며, 이 lab은 "Kafka를 어떻게 안전하게 쓰는가"에 집중합니다.

### 학습 구조

각 Step은 **"이 설정을 안 하면 어떤 사고가 나는가"**를 먼저 재현합니다.

```
Step 1  "acks=1로 했더니 leader 장애 시 메시지 날아갔다"
Step 2  "auto-commit 켜뒀더니 처리 실패한 메시지가 skip됐다"
Step 3  "key 안 넣었더니 같은 주문의 이벤트 순서가 뒤집혔다"
Step 4  "배포할 때마다 리밸런싱 걸려서 30초간 소비 멈췄다"
Step 5  "재시도만 하다가 같은 메시지가 무한루프 돌았다"
Step 6  "Exactly Once라고 믿었는데 consumer 쪽에서 중복 처리됐다"
Step 7  "lag이 10만 쌓였는데 아무도 몰랐다"
```

---

## Step 1 — Producer Guarantee

> 메시지를 보냈다고 끝이 아니다. 브로커가 확실히 저장했는지까지 확인해야 한다.

### 밟아볼 함정: 메시지 유실

- acks=0 → 브로커 응답 안 기다림, leader 장애 시 유실 확인
- acks=1 → leader만 확인, leader 장애 + follower 미동기화 시 유실 확인
- acks=all → 모든 ISR 확인, 유실 없음 확인
- min.insync.replicas 설정과의 조합 확인
- retries + retry.backoff.ms 설정에 따른 재시도 동작 확인

### 학습 포인트

acks=all이 아니면 유실 가능하다. 대신 latency와 throughput의 트레이드오프가 있다.

---

## Step 2 — Consumer Offset

> 메시지를 "읽었다"와 "처리했다"는 다른 말이다.

### 밟아볼 함정: 처리 전 커밋 / 처리 후 미커밋

- enable.auto.commit=true → 처리 실패했는데 offset이 넘어가서 메시지 유실 (at-most-once)
- manual commit 전환 → 처리 완료 후 커밋 (at-least-once)
- commitSync vs commitAsync 차이 확인
- manual commit 직전에 consumer 강제 종료 → 재시작 시 같은 메시지 재수신 확인
- auto.offset.reset (earliest vs latest) → Consumer Group 최초 접속 시 동작 차이

### 학습 포인트

auto-commit은 편하지만 위험하다. manual commit이 기본이며, 그래도 중복은 발생할 수 있다.

---

## Step 3 — Partition & Ordering

> 순서 보장은 토픽 전체가 아니라 파티션 안에서만 된다.

### 밟아볼 함정: 순서 역전

- key 없이 발행 → round-robin으로 partition 분산 → 같은 주문의 이벤트 순서 뒤집힘 확인
- 같은 key(orderId)로 발행 → 같은 partition → 순서 보장 확인
- partition 수 vs consumer 수 관계 실험
    - consumer = partition → 1:1 매핑, 최적
    - consumer > partition → 놀고 있는 consumer 확인
    - consumer < partition → 한 consumer가 여러 partition 담당
- partition 수 변경 시 기존 key 매핑이 깨지는 걸 확인 (rekey 문제)

### 학습 포인트

key 설계가 곧 순서 설계다. partition 수는 한번 늘리면 기존 매핑이 깨진다.

---

## Step 4 — Rebalancing

> Consumer를 추가하거나 제거할 때마다 전체 소비가 멈출 수 있다.

### 밟아볼 함정: 배포 시 소비 중단

- consumer 추가/제거 시 rebalancing 발생 확인
- rebalancing 중 메시지 소비 멈추는 구간 측정
- session.timeout.ms, heartbeat.interval.ms 설정 영향 확인
- max.poll.interval.ms 초과 시 consumer 강제 퇴출 확인
- CooperativeStickyAssignor로 전환 후 stop-the-world 감소 확인

### 학습 포인트

rebalancing은 피할 수 없다. 최소화하는 전략(cooperative 할당, 적절한 timeout)이 필요하다.

---

## Step 5 — DLQ (Dead Letter Queue)

> 실패한 메시지를 무한 재시도하면 전체 파이프라인이 멈춘다.

### 밟아볼 함정: poison pill + 무한 재시도

- 파싱 불가능한 메시지(poison pill) 발행 → consumer 무한 실패 루프 재현
- retry 횟수 설정 (3회) + backoff 전략 구성
- 최대 재시도 초과 시 DLQ 토픽으로 이동하는 구조 구현
- Spring Kafka DefaultErrorHandler + DeadLetterPublishingRecoverer 활용
- DLQ에 쌓인 메시지 확인 및 수동 재처리 시나리오

### 학습 포인트

실패를 무시하면 안 되고, 무한 재시도도 안 된다. 격리 후 운영자가 후처리하는 구조가 필요하다.

---

## Step 6 — Exactly-Once Semantics

> Kafka의 Exactly-Once는 "Kafka 내부"에서만 보장된다.

### 밟아볼 함정: EOS를 믿었는데 비즈니스 중복 발생

- enable.idempotence=true → producer 레벨 중복 발행 방지 확인
- transactional producer → produce + offset commit 원자적 처리 확인
- EOS가 보장하는 범위 확인: Kafka 내부 (produce → consume offset)
- EOS가 보장하지 못하는 범위 확인: consumer 비즈니스 로직 (DB insert, API 호출)
- consumer 측 비즈니스 처리 후 외부 시스템(DB) 반영 → Kafka EOS만으로 중복 방지 불가 재현

### 학습 포인트

EOS는 Kafka 파이프라인 내부의 정합성 도구이지, consumer 측 비즈니스 멱등성의 대체재가 아니다.
결국 messaging-lab Step 6의 멱등 처리가 최종 방어선이라는 걸 재확인한다.

---

## Step 7 — Monitoring

> Kafka는 띄우는 게 끝이 아니다. 지표를 안 보면 조용히 죽는다.

### 밟아볼 함정: 장애를 아무도 몰랐다

- consumer lag 확인 (kafka-consumer-groups.sh --describe)
- producer 발행 속도 > consumer 처리 속도 → lag 누적 재현
- under-replicated partitions 확인 (브로커 장애 시뮬레이션)
- request latency 확인 (producer/consumer 응답 시간)

### 실무에서 봐야 할 핵심 지표

| 지표 | 의미 | 위험 신호 |
| --- | --- | --- |
| Consumer Lag | consumer가 얼마나 뒤쳐져 있는가 | 지속 증가 |
| ISR Count | 동기화된 replica 수 | min.insync.replicas 미만 |
| Request Latency | produce/consume 응답 시간 | 급격한 증가 |
| Under-Replicated Partitions | 복제 안 된 파티션 수 | 0이 아닌 값 |

### 학습 포인트

운영은 코드가 아니라 지표로 한다. lag이 쌓이는 걸 아무도 모르면 조용히 장애가 된다.

---

## 이 lab이 다루지 않는 것

| 주제 | 다루는 곳 |
| --- | --- |
| 왜 Kafka를 쓰는가, 다른 도구와의 비교 | messaging-lab |
| Command vs Event 구분, 토픽 설계 판단 | messaging-lab |
| 멱등 처리 패턴 (event_handled, upsert, version) | messaging-lab |
| Transactional Outbox Pattern의 존재 이유 | messaging-lab |
| CDC (Debezium) 기반 릴레이 | 별도 주제 |
| Kafka Streams, ksqlDB | 별도 주제 |
| Schema Registry, Avro/Protobuf 직렬화 | 별도 주제 |