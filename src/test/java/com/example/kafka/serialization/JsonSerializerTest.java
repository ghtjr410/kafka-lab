package com.example.kafka.serialization;

import com.example.kafka.KafkaTestBase;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * JSON 직렬화/역직렬화의 동작과 함정을 증명한다.
 *
 * 사고 시나리오:
 *   String으로 JSON을 보내다가 Spring Kafka의 JsonSerializer로 전환했더니
 *   기존 Consumer가 역직렬화에 실패했다. 타입 헤더와 trusted packages 설정 때문이다.
 *
 * yml 대응:
 *   spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JsonSerializer
 *   spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.JsonDeserializer
 *   spring.kafka.consumer.properties.spring.json.trusted.packages=com.example.kafka.*
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class JsonSerializerTest extends KafkaTestBase {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * String_직렬화로_JSON을_보내면_Consumer는_수동_파싱이_필요하다.
     *
     * StringSerializer로 JSON 문자열을 보내면 Consumer는 String으로 받아서
     * 수동으로 ObjectMapper.readValue()를 호출해야 한다.
     * 가장 단순하지만 타입 안전성이 없다.
     *
     * yml 대응:
     *   spring.kafka.producer.value-serializer=StringSerializer (기본)
     */
    @Test
    void String_직렬화로_JSON을_보내면_Consumer는_수동_파싱이_필요하다() throws Exception {
        String testTopic = topic();
        String testGroupId = groupId();
        createTopic(testTopic, 1);

        OrderEvent order = new OrderEvent("order-001", "MacBook Pro", 2_990_000);

        // StringSerializer로 JSON 문자열 발행
        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of()))) {
            String json = objectMapper.writeValueAsString(order);
            sendSync(producer, testTopic, order.orderId(), json);
            System.out.printf("  발행: %s%n", json);
        }

        // StringDeserializer로 수신 → 수동 파싱
        try (var consumer = new KafkaConsumer<String, String>(
                consumerProps(testGroupId, Map.of(
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false
                )))) {
            consumer.subscribe(List.of(testTopic));

            List<String> values = pollValues(consumer, 1, Duration.ofSeconds(10));
            assertThat(values).hasSize(1);

            OrderEvent parsed = objectMapper.readValue(values.get(0), OrderEvent.class);
            assertThat(parsed.orderId()).isEqualTo("order-001");
            assertThat(parsed.productName()).isEqualTo("MacBook Pro");
            assertThat(parsed.price()).isEqualTo(2_990_000);

            System.out.printf("  수신 → 수동 파싱: %s%n", parsed);
            System.out.println("  → 단순하지만 타입 안전성 없음, 매번 ObjectMapper 호출 필요");
        }
    }

    /**
     * JsonSerializer로_객체를_직접_보내고_JsonDeserializer로_받을_수_있다.
     *
     * Spring Kafka의 JsonSerializer/JsonDeserializer를 사용하면
     * 객체를 직접 직렬화/역직렬화할 수 있다.
     * 단, trusted.packages 설정이 없으면 역직렬화가 거부된다.
     *
     * yml 대응:
     *   spring.kafka.producer.value-serializer=JsonSerializer
     *   spring.kafka.consumer.value-deserializer=JsonDeserializer
     *   spring.kafka.consumer.properties.spring.json.trusted.packages=*
     */
    @Test
    void JsonSerializer로_객체를_직접_보내고_JsonDeserializer로_받을_수_있다() {
        String testTopic = topic();
        String testGroupId = groupId();
        createTopic(testTopic, 1);

        OrderEvent order = new OrderEvent("order-002", "iPad Air", 899_000);

        // JsonSerializer로 객체 직접 발행
        Map<String, Object> prodProps = new HashMap<>();
        prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        try (var producer = new KafkaProducer<String, OrderEvent>(prodProps)) {
            producer.send(new ProducerRecord<>(testTopic, order.orderId(), order)).get();
            System.out.printf("  발행 (JsonSerializer): %s%n", order);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // JsonDeserializer로 객체 직접 수신
        Map<String, Object> consProps = new HashMap<>();
        consProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        consProps.put(ConsumerConfig.GROUP_ID_CONFIG, testGroupId);
        consProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        // trusted packages 설정 — 이것이 없으면 역직렬화 거부!
        consProps.put(JsonDeserializer.TRUSTED_PACKAGES, "com.example.kafka.*");
        consProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, OrderEvent.class.getName());

        try (var consumer = new KafkaConsumer<String, OrderEvent>(consProps)) {
            consumer.subscribe(List.of(testTopic));

            ConsumerRecords<String, OrderEvent> records = consumer.poll(Duration.ofSeconds(10));
            assertThat(records.count()).isEqualTo(1);

            OrderEvent received = records.iterator().next().value();
            assertThat(received.orderId()).isEqualTo("order-002");
            assertThat(received.productName()).isEqualTo("iPad Air");
            assertThat(received.price()).isEqualTo(899_000);

            System.out.printf("  수신 (JsonDeserializer): %s%n", received);
            System.out.println("  → 타입 안전, 자동 변환, trusted.packages 필수!");
        }
    }

    /**
     * trusted_packages를_설정하지_않으면_역직렬화가_거부된다.
     *
     * Spring Kafka의 JsonDeserializer는 보안을 위해 기본적으로
     * java.util, java.lang 패키지만 허용한다.
     * 커스텀 클래스를 역직렬화하려면 반드시 trusted.packages를 설정해야 한다.
     *
     * yml 대응: spring.kafka.consumer.properties.spring.json.trusted.packages=com.example.*
     */
    @Test
    void trusted_packages를_설정하지_않으면_역직렬화가_거부된다() {
        String testTopic = topic();
        String testGroupId = groupId();
        createTopic(testTopic, 1);

        OrderEvent order = new OrderEvent("order-003", "AirPods", 249_000);

        // JsonSerializer로 발행
        Map<String, Object> prodProps = new HashMap<>();
        prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        try (var producer = new KafkaProducer<String, OrderEvent>(prodProps)) {
            producer.send(new ProducerRecord<>(testTopic, order.orderId(), order)).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // trusted.packages 없이 역직렬화 시도 → 실패
        Map<String, Object> consProps = new HashMap<>();
        consProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        consProps.put(ConsumerConfig.GROUP_ID_CONFIG, testGroupId);
        consProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        // trusted.packages 미설정! → 기본값은 java.util, java.lang만 허용

        try (var consumer = new KafkaConsumer<String, OrderEvent>(consProps)) {
            consumer.subscribe(List.of(testTopic));

            // poll에서 역직렬화 실패: 에러가 wrapper 안에 포함
            assertThatThrownBy(() -> consumer.poll(Duration.ofSeconds(10)))
                    .hasRootCauseInstanceOf(IllegalArgumentException.class)
                    .rootCause()
                    .hasMessageContaining("not in the trusted packages");

            System.out.println("  → trusted.packages 미설정 시 역직렬화 거부됨");
            System.out.println("  → 보안 때문에 기본적으로 커스텀 클래스를 차단");
            System.out.println("  → yml에서 spring.json.trusted.packages 설정 필수!");
        }
    }

    /** 테스트용 주문 이벤트 DTO */
    public record OrderEvent(String orderId, String productName, int price) {}
}
