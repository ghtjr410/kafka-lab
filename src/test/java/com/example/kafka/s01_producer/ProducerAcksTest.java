package com.example.kafka.s01_producer;

import com.example.kafka.KafkaTestBase;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Producer acks 설정에 따른 발행 보장 수준을 증명한다.
 *
 * 사고 시나리오:
 *   kafkaTemplate.send()를 fire-and-forget으로 호출하던 결제 서비스에서
 *   브로커 장애 시 500건의 결제 이벤트가 유실됐다.
 *
 * yml 설정:
 *   spring.kafka.producer.acks = all (kafka-clients 3.0+ 기본값)
 *
 * 이 테스트는 acks 값에 따라 브로커 응답 동작이 어떻게 달라지는지를 증명한다.
 * 단일 브로커 환경이므로 실제 유실은 재현하지 않지만,
 * RecordMetadata 반환 여부와 offset 할당을 통해 보장 수준의 차이를 확인한다.
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class ProducerAcksTest extends KafkaTestBase {

    /**
     * acks=0이면 브로커 응답을 기다리지 않는다.
     *
     * fire-and-forget 방식이다. 최고 throughput이지만 브로커 장애 시 유실을 감지할 수 없다.
     * RecordMetadata가 반환되긴 하지만, offset=-1로 브로커 확인 없이 즉시 반환된다.
     *
     * yml 대응: spring.kafka.producer.acks=0
     */
    @Test
    void acks_0이면_브로커_응답을_기다리지_않는다() {
        String testTopic = topic();
        createTopic(testTopic, 1);

        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of(
                ProducerConfig.ACKS_CONFIG, "0"
        )))) {
            // acks=0: send()가 브로커 확인 없이 즉시 반환
            RecordMetadata metadata = sendSync(producer, testTopic, null, "fire-and-forget");

            // offset은 -1 — 브로커가 실제로 저장했는지 알 수 없다
            System.out.printf("  acks=0 → offset=%d, partition=%d%n",
                    metadata.offset(), metadata.partition());
            assertThat(metadata.offset()).isEqualTo(-1);

            System.out.println("  → 브로커 응답을 기다리지 않으므로 유실 감지 불가");
        }
    }

    /**
     * acks=1이면 leader만 확인한다.
     *
     * leader가 기록을 확인하면 성공 응답을 반환한다.
     * follower에 복제되기 전에 leader가 죽으면 유실될 수 있다.
     * 단일 브로커에서는 leader=유일한 브로커이므로 정상 offset이 반환된다.
     *
     * yml 대응: spring.kafka.producer.acks=1
     */
    @Test
    void acks_1이면_leader만_확인한다() {
        String testTopic = topic();
        createTopic(testTopic, 1);

        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of(
                ProducerConfig.ACKS_CONFIG, "1"
        )))) {
            RecordMetadata metadata = sendSync(producer, testTopic, null, "leader-only");

            // leader 확인 → 유효한 offset 반환
            System.out.printf("  acks=1 → offset=%d, partition=%d%n",
                    metadata.offset(), metadata.partition());
            assertThat(metadata.offset()).isGreaterThanOrEqualTo(0);

            System.out.println("  → leader만 확인. follower 미동기화 + leader 장애 시 유실 가능");
        }
    }

    /**
     * acks=all이면 모든 ISR이 확인한다.
     *
     * 모든 ISR(In-Sync Replica)이 기록을 확인해야 성공 응답을 반환한다.
     * 유실 없음이 보장되지만, latency가 증가한다.
     * kafka-clients 3.0+의 기본값이다.
     *
     * yml 대응: spring.kafka.producer.acks=all (기본값)
     */
    @Test
    void acks_all이면_모든_ISR이_확인한다() {
        String testTopic = topic();
        createTopic(testTopic, 1);

        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of(
                ProducerConfig.ACKS_CONFIG, "all"
        )))) {
            RecordMetadata metadata = sendSync(producer, testTopic, null, "all-isr");

            // 모든 ISR 확인 → 유효한 offset 반환
            System.out.printf("  acks=all → offset=%d, partition=%d%n",
                    metadata.offset(), metadata.partition());
            assertThat(metadata.offset()).isGreaterThanOrEqualTo(0);

            System.out.println("  → 모든 ISR 확인. 유실 없음 보장 (단, latency 증가)");
        }
    }

    /**
     * acks=all + min.insync.replicas=1이면 사실상 acks=1이다.
     *
     * 단일 브로커 환경에서 ISR은 항상 1뿐이다.
     * acks=all이라도 min.insync.replicas=1(기본값)이면,
     * leader 하나만 확인하면 되므로 실질적으로 acks=1과 같다.
     *
     * 프로덕션에서는 3-broker + min.insync.replicas=2가 표준이다.
     */
    @Test
    void acks_all에_min_insync_replicas_1이면_사실상_acks_1이다() {
        String testTopic = topic();
        createTopic(testTopic, 1);

        // acks=all 로 발행
        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of(
                ProducerConfig.ACKS_CONFIG, "all"
        )))) {
            RecordMetadata metadata = sendSync(producer, testTopic, null, "acks-all-single-broker");

            assertThat(metadata.offset()).isGreaterThanOrEqualTo(0);

            System.out.println("  단일 브로커 → ISR = {broker-1} (1개뿐)");
            System.out.println("  acks=all이지만 확인할 ISR이 1개 → 사실상 acks=1");
            System.out.println("  → 프로덕션: 3-broker + min.insync.replicas=2 필수");
        }
    }

    /**
     * send()의 반환값을 확인하지 않으면 유실을 감지할 수 없다.
     *
     * KafkaTemplate.send()는 CompletableFuture를 반환한다.
     * 이 반환값을 무시하면(fire-and-forget) 브로커 장애 시 유실이 조용히 발생한다.
     * 반환값을 확인해야 발행 성공/실패를 알 수 있다.
     */
    @Test
    void send의_반환값을_확인해야_발행_성공을_보장할_수_있다() {
        String testTopic = topic();
        createTopic(testTopic, 1);

        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of(
                ProducerConfig.ACKS_CONFIG, "all"
        )))) {
            // 동기 확인: get()으로 블로킹하여 결과를 확인
            RecordMetadata metadata = sendSync(producer, testTopic, "order-1", "payment-completed");

            assertThat(metadata.topic()).isEqualTo(testTopic);
            assertThat(metadata.offset()).isGreaterThanOrEqualTo(0);
            assertThat(metadata.partition()).isGreaterThanOrEqualTo(0);

            System.out.printf("  발행 확인: topic=%s, partition=%d, offset=%d%n",
                    metadata.topic(), metadata.partition(), metadata.offset());

            // Consumer로 실제 메시지 도착 확인
            try (var consumer = new KafkaConsumer<String, String>(
                    consumerProps(groupId(), Map.of()))) {
                consumer.subscribe(List.of(testTopic));
                List<String> values = pollValues(consumer, 1, Duration.ofSeconds(10));

                assertThat(values).containsExactly("payment-completed");
                System.out.println("  → Consumer에서 메시지 수신 확인 완료");
            }
        }
    }
}
