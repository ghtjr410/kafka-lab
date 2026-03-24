# Step 7 — Serialization & Schema Evolution

## 핵심 질문

> Producer가 필드를 하나 추가했는데 왜 Consumer가 죽는가? — **ObjectMapper 기본값이 알 수 없는 필드를 거부하기 때문이다.**

## 이 디렉터리가 증명하는 것

### 함정 (Before)
- `JsonDeserializer`의 `trusted.packages` 미설정 → 역직렬화 거부 (보안 차단)
- Producer V2(필드 추가) → Consumer V1: `UnrecognizedPropertyException` → **Consumer 장애**
- `ObjectMapper` 기본값: `FAIL_ON_UNKNOWN_PROPERTIES=true`

### 해결 (After)
- `trusted.packages=com.example.*` 설정
- `FAIL_ON_UNKNOWN_PROPERTIES=false` → 알 수 없는 필드 무시 (하위 호환)
- 새 필드는 기본값으로 채워짐 (상위 호환)
- 메시지 헤더에 `schema.version` → 버전별 분기 처리 (간이 스키마 관리)

## 테스트 파일

| 파일 | 테스트 수 | 핵심 |
|------|---------|------|
| JsonSerializerTest | 3 | String vs JsonSerializer, trusted.packages 함정 |
| SchemaEvolutionTest | 4 | 필드 추가/삭제 호환성, 헤더 기반 버전 관리 |

## yml 대응

```yaml
spring.kafka.producer:
  value-serializer: org.springframework.kafka.support.serializer.JsonSerializer

spring.kafka.consumer:
  value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
  properties:
    spring.json.trusted.packages: com.example.*

spring.jackson.deserialization:
  fail-on-unknown-properties: false    # 하위 호환성 확보
```
