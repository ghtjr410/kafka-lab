package com.example.kafka.s02_consumer;

import com.example.kafka.KafkaTestBase;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * auto.offset.reset 설정에 따른 새 Consumer Group의 시작 위치를 증명한다.
 *
 * 새 Consumer Group이 토픽에 처음 접속할 때,
 * 커밋된 offset이 없으므로 auto.offset.reset 설정에 따라 시작 위치가 결정된다.
 *
 * yml 대응:
 *   spring.kafka.consumer.auto-offset-reset: latest (기본값)
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class ConsumerOffsetResetTest extends KafkaTestBase {

    /**
     * auto.offset.reset=latest → 기존 메시지는 무시하고 이후 메시지만 소비한다.
     *
     * 새 Consumer Group은 토픽의 끝(latest)부터 시작한다.
     * 이전에 발행된 메시지는 소비하지 않는다.
     *
     * yml 대응: spring.kafka.consumer.auto-offset-reset=latest (기본값)
     */
    @Test
    void auto_offset_reset_latest이면_기존_메시지를_무시한다() {
        String testTopic = topic();
        createTopic(testTopic, 1);

        // 먼저 메시지 3건 발행
        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of()))) {
            for (int i = 0; i < 3; i++) {
                sendSync(producer, testTopic, null, "old-msg-" + i);
            }
        }

        // 새 Consumer Group (latest) — 기존 메시지를 못 받음
        try (var consumer = new KafkaConsumer<String, String>(
                consumerProps(groupId(), Map.of(
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"
                )))) {
            consumer.subscribe(List.of(testTopic));

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            assertThat(records.count()).isEqualTo(0);

            System.out.println("  latest: 기존 3건 무시 → 0건 수신");
            System.out.println("  → 새 Consumer Group은 이후 메시지만 소비");
        }
    }

    /**
     * auto.offset.reset=earliest → 토픽의 처음부터 모든 메시지를 소비한다.
     *
     * 새 Consumer Group이 토픽의 시작(earliest)부터 읽는다.
     * 모든 기존 메시지를 소비한다.
     *
     * yml 대응: spring.kafka.consumer.auto-offset-reset=earliest
     */
    @Test
    void auto_offset_reset_earliest이면_처음부터_모든_메시지를_소비한다() {
        String testTopic = topic();
        createTopic(testTopic, 1);

        // 먼저 메시지 3건 발행
        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of()))) {
            for (int i = 0; i < 3; i++) {
                sendSync(producer, testTopic, null, "msg-" + i);
            }
        }

        // 새 Consumer Group (earliest) — 처음부터 읽음
        try (var consumer = new KafkaConsumer<String, String>(
                consumerProps(groupId(), Map.of(
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
                )))) {
            consumer.subscribe(List.of(testTopic));

            List<String> values = pollValues(consumer, 3, Duration.ofSeconds(10));
            assertThat(values).containsExactly("msg-0", "msg-1", "msg-2");

            System.out.println("  earliest: 3건 전부 수신");
            System.out.println("  → 새 Consumer Group은 토픽의 처음부터 소비");
        }
    }
}
