package com.example.kafka.consumer;

import com.example.kafka.KafkaTestBase;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Spring Kafka의 AckMode에 따른 offset 커밋 동작을 증명한다.
 *
 * 사고 시나리오:
 *   @KafkaListener에서 예외를 삼켰더니 offset이 자동으로 넘어갔다.
 *   3만 건의 주문 이벤트가 처리 안 됐는데, AckMode가 BATCH(기본값)라서 다음 poll() 시 커밋됐다.
 *
 * Spring Kafka는 기본적으로 enable.auto.commit=false로 오버라이드하고,
 * 자체 AckMode로 offset을 관리한다.
 *
 * 이 테스트는 native kafka-clients API로 AckMode 동작을 재현한다.
 * Spring의 @KafkaListener가 내부적으로 하는 일을 직접 구현해서 이해한다.
 *
 * yml 대응:
 *   spring.kafka.consumer.enable-auto-commit: false (Spring Kafka 기본)
 *   spring.kafka.listener.ack-mode: BATCH (기본값)
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class ConsumerAckModeTest extends KafkaTestBase {

    /**
     * AckMode.BATCH 기본값: poll()이 반환한 레코드를 전부 처리한 후 한 번에 커밋한다.
     *
     * 처리 중 장애가 나면 전체 배치가 재소비된다.
     * Spring Kafka의 기본 동작을 native API로 재현한다.
     *
     * yml 대응: spring.kafka.listener.ack-mode=BATCH
     */
    @Test
    void AckMode_BATCH_기본값은_poll_반환_레코드_전부_처리_후_커밋한다() {
        String testTopic = topic();
        String testGroupId = groupId();
        createTopic(testTopic, 1);

        // 3건 발행
        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of()))) {
            for (int i = 0; i < 3; i++) {
                sendSync(producer, testTopic, null, "msg-" + i);
            }
        }

        // BATCH 모드 시뮬레이션: poll() → 전체 처리 → commitSync()
        try (var consumer = new KafkaConsumer<String, String>(
                consumerProps(testGroupId, Map.of(
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                        ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10
                )))) {
            consumer.subscribe(List.of(testTopic));

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
            assertThat(records.count()).isEqualTo(3);

            // 전체 배치 처리 완료 후 한 번에 커밋 (BATCH 동작)
            consumer.commitSync();

            // 커밋 확인: 다음 poll()에서 새 메시지 없음
            ConsumerRecords<String, String> next = consumer.poll(Duration.ofSeconds(3));
            assertThat(next.count()).isEqualTo(0);

            System.out.println("  BATCH: 3건 poll() → 전체 처리 → commitSync() → 커밋 완료");
            System.out.println("  → 처리 중 장애 시 3건 전부 재소비");
        }
    }

    /**
     * AckMode.RECORD: 레코드 하나를 처리할 때마다 커밋한다.
     *
     * at-least-once 보장. 가장 안전하지만 커밋 빈도가 높아 성능 저하.
     *
     * yml 대응: spring.kafka.listener.ack-mode=RECORD
     */
    @Test
    void AckMode_RECORD는_레코드_하나_처리할_때마다_커밋한다() {
        String testTopic = topic();
        String testGroupId = groupId();
        createTopic(testTopic, 1);

        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of()))) {
            for (int i = 0; i < 3; i++) {
                sendSync(producer, testTopic, null, "msg-" + i);
            }
        }

        // RECORD 모드 시뮬레이션: 레코드마다 commitSync()
        try (var consumer = new KafkaConsumer<String, String>(
                consumerProps(testGroupId, Map.of(
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false
                )))) {
            consumer.subscribe(List.of(testTopic));

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
            int commitCount = 0;
            for (ConsumerRecord<String, String> record : records) {
                // 레코드 처리
                System.out.printf("  처리: offset=%d, value=%s%n", record.offset(), record.value());

                // 레코드마다 즉시 커밋
                consumer.commitSync(Map.of(
                        new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1)
                ));
                commitCount++;
            }

            assertThat(commitCount).isEqualTo(3);
            System.out.printf("  RECORD: 3건 각각 커밋 (총 %d회 commitSync)%n", commitCount);
            System.out.println("  → 가장 안전하지만 커밋 빈도 높아 성능 저하");
        }
    }

    /**
     * AckMode.MANUAL_IMMEDIATE: Acknowledgment.acknowledge() 호출 시 즉시 커밋한다.
     *
     * 가장 세밀한 제어. 특정 레코드만 선택적으로 커밋할 수 있다.
     *
     * yml 대응: spring.kafka.listener.ack-mode=MANUAL_IMMEDIATE
     */
    @Test
    void AckMode_MANUAL_IMMEDIATE는_명시적_호출_시_즉시_커밋한다() {
        String testTopic = topic();
        String testGroupId = groupId();
        createTopic(testTopic, 1);

        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of()))) {
            for (int i = 0; i < 3; i++) {
                sendSync(producer, testTopic, null, "msg-" + i);
            }
        }

        // MANUAL_IMMEDIATE 시뮬레이션: 원하는 시점에 명시적 커밋
        try (var consumer = new KafkaConsumer<String, String>(
                consumerProps(testGroupId, Map.of(
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false
                )))) {
            consumer.subscribe(List.of(testTopic));

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
            List<String> processed = new ArrayList<>();

            for (ConsumerRecord<String, String> record : records) {
                processed.add(record.value());

                // 2번째 레코드(offset=1) 처리 후에만 커밋
                if (record.offset() == 1) {
                    consumer.commitSync(Map.of(
                            new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1)
                    ));
                    System.out.printf("  offset=%d에서 명시적 커밋%n", record.offset());
                }
            }

            assertThat(processed).containsExactly("msg-0", "msg-1", "msg-2");
        }

        // 새 Consumer로 재접속: offset=2부터 (커밋된 다음 위치)
        try (var consumer2 = new KafkaConsumer<String, String>(
                consumerProps(testGroupId, Map.of(
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false
                )))) {
            consumer2.subscribe(List.of(testTopic));

            ConsumerRecords<String, String> remaining = consumer2.poll(Duration.ofSeconds(10));
            List<String> values = new ArrayList<>();
            remaining.forEach(r -> values.add(r.value()));

            // offset=2(msg-2)는 커밋 안 됐으므로 재소비
            assertThat(values).containsExactly("msg-2");
            System.out.println("  → offset=1까지만 커밋 → msg-2 재소비됨");
            System.out.println("  → MANUAL: acknowledge() 안 부르면 offset 안 넘어감");
        }
    }

    /**
     * 예외를 try-catch로 삼키면 offset이 커밋되어 메시지가 유실된다.
     *
     * @KafkaListener에서 예외를 삼키면 Spring Kafka는 정상 처리로 간주한다.
     * DefaultErrorHandler에 도달하지 않으므로 DLQ로도 가지 않는다.
     * 이것이 가장 위험한 패턴이다.
     *
     * yml 대응: 어떤 AckMode든 예외를 삼키면 같은 문제가 발생한다
     */
    @Test
    void 예외를_삼키면_offset이_커밋되어_메시지가_유실된다() {
        String testTopic = topic();
        String testGroupId = groupId();
        createTopic(testTopic, 1);

        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of()))) {
            sendSync(producer, testTopic, null, "poison-message");
            sendSync(producer, testTopic, null, "normal-message");
        }

        // @KafkaListener에서 예외를 삼키는 패턴 재현
        List<String> successfullyProcessed = new ArrayList<>();
        try (var consumer = new KafkaConsumer<String, String>(
                consumerProps(testGroupId, Map.of(
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false
                )))) {
            consumer.subscribe(List.of(testTopic));

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
            for (ConsumerRecord<String, String> record : records) {
                try {
                    // poison-message 처리 실패
                    if (record.value().equals("poison-message")) {
                        throw new RuntimeException("파싱 실패!");
                    }
                    successfullyProcessed.add(record.value());
                } catch (Exception e) {
                    // 예외를 삼킴 — 이것이 함정!
                    System.out.printf("  [함정] 예외 삼킴: %s (offset=%d)%n",
                            e.getMessage(), record.offset());
                }
            }

            // BATCH 모드처럼 전체 커밋 — poison-message도 커밋됨
            consumer.commitSync();
        }

        // 새 Consumer로 재접속: poison-message는 이미 커밋돼서 다시 받을 수 없다
        try (var consumer2 = new KafkaConsumer<String, String>(
                consumerProps(testGroupId, Map.of(
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false
                )))) {
            consumer2.subscribe(List.of(testTopic));
            ConsumerRecords<String, String> remaining = consumer2.poll(Duration.ofSeconds(3));

            assertThat(remaining.count()).isEqualTo(0);
            System.out.println("  → 예외를 삼켰더니 poison-message가 커밋됨 → 재소비 불가");
            System.out.println("  → ErrorHandler에 도달하지 않으므로 DLQ에도 안 감");
            System.out.println("  → 절대 예외를 삼키지 말 것!");
        }
    }
}
