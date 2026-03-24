# Step 8 — Kafka Connect

## 핵심 질문

> DB 변경사항을 Kafka로 보내려면 Producer를 직접 짜야 하나? — **Kafka Connect를 쓰면 설정만으로 데이터 파이프라인을 만들 수 있다.**

## 이 디렉터리가 증명하는 것

- **Source Connector**: 외부 시스템(파일) → Kafka 토픽으로 데이터 발행
- **Sink Connector**: Kafka 토픽 → 외부 시스템(파일)으로 데이터 내보내기
- **REST API**: Connector 생성, 상태 조회, 일시정지/재개, 삭제
- Connect가 **offset을 자동 관리** → 재시작 시 이어서 처리

## 테스트 파일

| 파일 | 테스트 수 | 핵심 |
|------|---------|------|
| KafkaConnectTest | 4 | REST API, Source/Sink 동작, pause/resume |

## 인프라

```bash
# Kafka Connect 워커 기동 (docker-compose.yml에 포함)
docker-compose up -d kafka-connect

# REST API 확인
curl http://localhost:8083/
curl http://localhost:8083/connector-plugins
```

## 실무 확장

| 이 lab의 커넥터 | 실무 대응 |
|----------------|----------|
| FileStreamSourceConnector | Debezium (DB CDC), JDBC Source |
| FileStreamSinkConnector | S3 Sink, Elasticsearch Sink, JDBC Sink |
