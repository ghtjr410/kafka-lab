package com.example.kafka.serialization;

import com.example.kafka.KafkaTestBase;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 스키마 진화(Schema Evolution) 시 역직렬화 호환성 문제를 증명한다.
 *
 * 사고 시나리오:
 *   Producer가 OrderEventV2(필드 추가)로 업그레이드했는데 Consumer는 아직 V1이다.
 *   ObjectMapper 기본 설정에서는 알 수 없는 필드가 있으면 예외가 발생한다.
 *   FAIL_ON_UNKNOWN_PROPERTIES=false를 설정해야 하위 호환성을 유지할 수 있다.
 *
 * Avro/Protobuf는 Schema Registry로 호환성을 강제하지만,
 * JSON에서는 개발자가 직접 호환성을 관리해야 한다.
 *
 * yml 대응:
 *   spring.kafka.consumer.properties.spring.json.use.type.headers=false
 *   spring.jackson.deserialization.fail-on-unknown-properties=false
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class SchemaEvolutionTest extends KafkaTestBase {

    /**
     * Producer가_필드를_추가하면_기존_Consumer는_역직렬화에_실패한다.
     *
     * ObjectMapper의 기본값은 FAIL_ON_UNKNOWN_PROPERTIES=true다.
     * V2(필드 추가)로 발행된 메시지를 V1 DTO로 읽으면 예외가 발생한다.
     * 이것이 JSON 스키마 진화의 가장 흔한 함정이다.
     */
    @Test
    void Producer가_필드를_추가하면_기존_Consumer는_역직렬화에_실패한다() throws Exception {
        String testTopic = topic();
        String testGroupId = groupId();
        createTopic(testTopic, 1);

        // V2 Producer: 새 필드(discount) 추가
        ObjectMapper strictMapper = new ObjectMapper();
        String v2Json = strictMapper.writeValueAsString(
                new OrderEventV2("order-100", "MacBook", 2_990_000, 10));

        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of()))) {
            sendSync(producer, testTopic, "order-100", v2Json);
            System.out.printf("  [V2 Producer] 발행: %s%n", v2Json);
        }

        // V1 Consumer: 알 수 없는 필드 → 기본 설정에서 예외
        try (var consumer = new KafkaConsumer<String, String>(
                consumerProps(testGroupId, Map.of(
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false
                )))) {
            consumer.subscribe(List.of(testTopic));
            List<String> values = pollValues(consumer, 1, Duration.ofSeconds(10));

            // 기본 ObjectMapper: 알 수 없는 필드 → UnrecognizedPropertyException
            assertThatThrownBy(() -> strictMapper.readValue(values.get(0), OrderEventV1.class))
                    .isInstanceOf(com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException.class)
                    .hasMessageContaining("discount");

            System.out.println("  [V1 Consumer] 역직렬화 실패: UnrecognizedPropertyException (discount)");
            System.out.println("  → 기본 ObjectMapper는 알 수 없는 필드를 거부한다");
        }
    }

    /**
     * FAIL_ON_UNKNOWN_PROPERTIES를_끄면_하위_호환성을_유지할_수_있다.
     *
     * ObjectMapper에 FAIL_ON_UNKNOWN_PROPERTIES=false를 설정하면
     * V2의 새 필드를 무시하고 V1 DTO로 안전하게 역직렬화할 수 있다.
     * 이것이 JSON 기반 스키마 진화의 핵심 설정이다.
     *
     * yml 대응: spring.jackson.deserialization.fail-on-unknown-properties=false
     */
    @Test
    void FAIL_ON_UNKNOWN_PROPERTIES를_끄면_하위_호환성을_유지할_수_있다() throws Exception {
        String testTopic = topic();
        String testGroupId = groupId();
        createTopic(testTopic, 1);

        // V2 발행 (discount 필드 추가)
        ObjectMapper mapper = new ObjectMapper();
        String v2Json = mapper.writeValueAsString(
                new OrderEventV2("order-200", "iPad", 899_000, 15));

        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of()))) {
            sendSync(producer, testTopic, "order-200", v2Json);
            System.out.printf("  [V2 Producer] 발행: %s%n", v2Json);
        }

        // V1 Consumer: FAIL_ON_UNKNOWN_PROPERTIES=false
        ObjectMapper lenientMapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        try (var consumer = new KafkaConsumer<String, String>(
                consumerProps(testGroupId, Map.of(
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false
                )))) {
            consumer.subscribe(List.of(testTopic));
            List<String> values = pollValues(consumer, 1, Duration.ofSeconds(10));

            // lenient mapper: 알 수 없는 필드(discount) 무시 → 정상 파싱
            OrderEventV1 v1 = lenientMapper.readValue(values.get(0), OrderEventV1.class);
            assertThat(v1.orderId()).isEqualTo("order-200");
            assertThat(v1.productName()).isEqualTo("iPad");
            assertThat(v1.price()).isEqualTo(899_000);

            System.out.printf("  [V1 Consumer] 역직렬화 성공: %s (discount 무시)%n", v1);
            System.out.println("  → FAIL_ON_UNKNOWN_PROPERTIES=false로 하위 호환성 확보");
        }
    }

    /**
     * 필수_필드가_삭제되면_기본값으로_채워진다.
     *
     * V1 Producer가 보낸 메시지를 V2 Consumer가 읽으면
     * V2에 추가된 필드(discount)는 JSON에 없으므로 기본값(0)이 된다.
     * 이것이 "상위 호환성(Forward Compatibility)"이다.
     */
    @Test
    void 필수_필드가_없으면_기본값으로_채워진다() throws Exception {
        String testTopic = topic();
        String testGroupId = groupId();
        createTopic(testTopic, 1);

        // V1 Producer: discount 필드 없음
        ObjectMapper mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        String v1Json = mapper.writeValueAsString(
                new OrderEventV1("order-300", "AirPods", 249_000));

        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of()))) {
            sendSync(producer, testTopic, "order-300", v1Json);
            System.out.printf("  [V1 Producer] 발행: %s%n", v1Json);
        }

        // V2 Consumer: discount 필드가 없으므로 기본값(0)
        try (var consumer = new KafkaConsumer<String, String>(
                consumerProps(testGroupId, Map.of(
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false
                )))) {
            consumer.subscribe(List.of(testTopic));
            List<String> values = pollValues(consumer, 1, Duration.ofSeconds(10));

            OrderEventV2 v2 = mapper.readValue(values.get(0), OrderEventV2.class);
            assertThat(v2.orderId()).isEqualTo("order-300");
            assertThat(v2.productName()).isEqualTo("AirPods");
            assertThat(v2.price()).isEqualTo(249_000);
            assertThat(v2.discount()).isEqualTo(0); // JSON에 없으면 record 기본값

            System.out.printf("  [V2 Consumer] 역직렬화 성공: %s (discount=0 기본값)%n", v2);
            System.out.println("  → V1 메시지를 V2로 읽으면 새 필드는 기본값");
            System.out.println("  → 상위 호환성(Forward Compatibility): 새 Consumer가 옛 메시지를 읽을 수 있다");
        }
    }

    /**
     * 메시지_헤더에_스키마_버전을_넣어_호환성을_관리할_수_있다.
     *
     * Schema Registry 없이도 메시지 헤더에 버전 정보를 넣으면
     * Consumer가 버전별로 다른 역직렬화 로직을 적용할 수 있다.
     * 간이 스키마 관리 전략이다.
     */
    @Test
    void 메시지_헤더에_스키마_버전을_넣어_호환성을_관리할_수_있다() throws Exception {
        String testTopic = topic();
        String testGroupId = groupId();
        createTopic(testTopic, 1);

        ObjectMapper mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        // V1과 V2 메시지를 헤더로 구분하여 발행
        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of()))) {
            // V1 메시지
            ProducerRecord<String, String> v1Record = new ProducerRecord<>(
                    testTopic, "order-400",
                    mapper.writeValueAsString(new OrderEventV1("order-400", "iPhone", 1_550_000)));
            v1Record.headers().add(new RecordHeader("schema.version", "1".getBytes(StandardCharsets.UTF_8)));
            producer.send(v1Record).get();

            // V2 메시지
            ProducerRecord<String, String> v2Record = new ProducerRecord<>(
                    testTopic, "order-401",
                    mapper.writeValueAsString(new OrderEventV2("order-401", "Galaxy", 1_350_000, 20)));
            v2Record.headers().add(new RecordHeader("schema.version", "2".getBytes(StandardCharsets.UTF_8)));
            producer.send(v2Record).get();

            System.out.println("  발행: V1(iPhone), V2(Galaxy) - 헤더로 버전 구분");
        }

        // Consumer: 헤더의 schema.version으로 분기
        try (var consumer = new KafkaConsumer<String, String>(
                consumerProps(testGroupId, Map.of(
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false
                )))) {
            consumer.subscribe(List.of(testTopic));

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
            assertThat(records.count()).isEqualTo(2);

            int v1Count = 0, v2Count = 0;
            for (var record : records) {
                String version = new String(
                        record.headers().lastHeader("schema.version").value(),
                        StandardCharsets.UTF_8);

                if ("1".equals(version)) {
                    OrderEventV1 v1 = mapper.readValue(record.value(), OrderEventV1.class);
                    System.out.printf("  수신 [V%s]: %s%n", version, v1);
                    assertThat(v1.orderId()).isEqualTo("order-400");
                    v1Count++;
                } else if ("2".equals(version)) {
                    OrderEventV2 v2 = mapper.readValue(record.value(), OrderEventV2.class);
                    System.out.printf("  수신 [V%s]: %s (discount=%d%%)%n", version, v2.orderId(), v2.discount());
                    assertThat(v2.orderId()).isEqualTo("order-401");
                    assertThat(v2.discount()).isEqualTo(20);
                    v2Count++;
                }
            }

            assertThat(v1Count).isEqualTo(1);
            assertThat(v2Count).isEqualTo(1);

            System.out.println();
            System.out.println("  → 헤더 기반 버전 관리: Schema Registry 없이도 호환성 확보");
            System.out.println("  → 단, 버전 관리를 개발자가 직접 해야 함 → 실수 가능");
            System.out.println("  → 대규모 시스템에서는 Avro + Schema Registry 권장");
        }
    }

    /** V1 DTO: 기본 필드 */
    public record OrderEventV1(String orderId, String productName, int price) {}

    /** V2 DTO: discount 필드 추가 (하위 호환 깨짐 가능) */
    public record OrderEventV2(String orderId, String productName, int price, int discount) {}
}
