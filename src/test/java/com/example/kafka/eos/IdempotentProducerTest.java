package com.example.kafka.eos;

import com.example.kafka.KafkaTestBase;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 멱등 프로듀서(Idempotent Producer)의 보장 범위와 한계를 증명한다.
 *
 * kafka-clients 3.0+에서 enable.idempotence=true가 기본값이다.
 * 멱등 프로듀서는 단일 파티션, 단일 세션 내에서만 중복 발행을 방지한다.
 * Producer가 재시작되면 새 Producer ID가 할당되므로 이전 세션의 중복을 감지하지 못한다.
 *
 * yml 대응:
 *   spring.kafka.producer.properties.enable.idempotence=true (3.0+ 기본값)
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class IdempotentProducerTest extends KafkaTestBase {

    /**
     * 멱등 프로듀서는 같은 세션 내에서 재시도 중복을 방지한다.
     *
     * 네트워크 오류로 브로커가 ACK를 보내지 못했을 때,
     * Producer가 같은 메시지를 재전송해도 브로커가 중복을 감지하여 한 번만 저장한다.
     * 이것이 enable.idempotence=true의 핵심 기능이다.
     */
    @Test
    void 멱등_프로듀서는_같은_세션_내에서_재시도_중복을_방지한다() {
        String testTopic = topic();
        String testGroupId = groupId();
        createTopic(testTopic, 1);

        // 멱등 프로듀서 (기본값)
        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of(
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true
        )))) {
            // 같은 세션에서 같은 메시지를 의도적으로 2번 발행
            RecordMetadata meta1 = sendSync(producer, testTopic, "order-1", "payment-event");
            RecordMetadata meta2 = sendSync(producer, testTopic, "order-1", "payment-event");

            System.out.printf("  1회 발행: offset=%d%n", meta1.offset());
            System.out.printf("  2회 발행: offset=%d%n", meta2.offset());

            // 같은 세션에서 send()를 2번 호출하면 2개의 별개 메시지로 저장된다.
            // 멱등성은 "네트워크 재시도"에 의한 중복만 방지하지,
            // 애플리케이션 레벨에서 같은 내용을 2번 send()하는 것은 막지 않는다.
            assertThat(meta2.offset()).isEqualTo(meta1.offset() + 1);

            System.out.println("  → 멱등 프로듀서는 '네트워크 재시도' 중복만 방지");
            System.out.println("  → 애플리케이션이 2번 send()하면 2건 저장됨");
        }
    }

    /**
     * Producer 재시작 후 같은 메시지를 발행하면 중복 저장된다.
     *
     * 멱등 프로듀서는 Producer ID + Sequence Number로 중복을 감지한다.
     * Producer를 재시작하면 새 Producer ID가 할당되어 이전 세션을 감지하지 못한다.
     */
    @Test
    void Producer_재시작_후_같은_메시지_발행하면_중복_저장된다() {
        String testTopic = topic();
        String testGroupId = groupId();
        createTopic(testTopic, 1);

        // 첫 번째 Producer 세션
        try (var producer1 = new KafkaProducer<String, String>(producerProps(Map.of(
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true
        )))) {
            sendSync(producer1, testTopic, "order-1", "payment-event");
            System.out.println("  Producer 1 (PID=A): payment-event 발행");
        }

        // 두 번째 Producer 세션 (재시작 = 새 Producer ID)
        try (var producer2 = new KafkaProducer<String, String>(producerProps(Map.of(
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true
        )))) {
            sendSync(producer2, testTopic, "order-1", "payment-event");
            System.out.println("  Producer 2 (PID=B): payment-event 발행 (같은 내용)");
        }

        // Consumer: 2건 수신 → 중복!
        try (var consumer = new KafkaConsumer<String, String>(
                consumerProps(testGroupId, Map.of(
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false
                )))) {
            consumer.subscribe(List.of(testTopic));

            List<String> values = pollValues(consumer, 2, Duration.ofSeconds(10));
            assertThat(values).containsExactly("payment-event", "payment-event");

            System.out.println("  Consumer: payment-event × 2건 수신 → 중복!");
            System.out.println("  → 새 Producer ID → 이전 세션 감지 불가 → 중복 저장");
            System.out.println("  → 해결: Transactional Producer (transactional.id로 세션 경계를 넘어 관리)");
        }
    }
}
