package com.example.kafka.s02_consumer;

import com.example.kafka.KafkaTestBase;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Consumer Lag의 기본 개념을 증명한다.
 *
 * Lag = LEO(Log End Offset) - Committed Offset
 *   = "Consumer가 얼마나 뒤쳐져 있는가"
 *
 * lag이 쌓이는 것은 장애의 첫 번째 신호다.
 * 처리가 느려지면 max.poll.interval.ms를 초과해서 강제 퇴출될 수 있다 (Step 4).
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class ConsumerLagBasicTest extends KafkaTestBase {

    /**
     * LEO - Committed Offset = Consumer Lag.
     *
     * Producer가 10건을 발행하고 Consumer가 3건만 커밋하면,
     * lag = 10 - 3 = 7이다.
     */
    @Test
    void LEO에서_Committed_Offset을_빼면_Consumer_Lag이다() {
        String testTopic = topic();
        String testGroupId = groupId();
        createTopic(testTopic, 1);

        // 10건 발행
        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of()))) {
            for (int i = 0; i < 10; i++) {
                sendSync(producer, testTopic, null, "msg-" + i);
            }
        }

        TopicPartition tp = new TopicPartition(testTopic, 0);

        // Consumer: 3건만 처리하고 커밋
        try (var consumer = new KafkaConsumer<String, String>(
                consumerProps(testGroupId, Map.of(
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                        ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 3
                )))) {
            consumer.subscribe(List.of(testTopic));

            var records = consumer.poll(Duration.ofSeconds(10));
            assertThat(records.count()).isEqualTo(3);
            consumer.commitSync();
        }

        // AdminClient로 LEO와 Committed Offset 확인
        try (AdminClient admin = AdminClient.create(Map.of(
                "bootstrap.servers", BOOTSTRAP_SERVERS))) {

            // LEO (Log End Offset)
            var leoResult = admin.listOffsets(Map.of(tp, OffsetSpec.latest()));
            long leo = leoResult.partitionResult(tp).get().offset();

            // Committed Offset
            var committedResult = admin.listConsumerGroupOffsets(testGroupId);
            Map<TopicPartition, OffsetAndMetadata> committed = committedResult.partitionsToOffsetAndMetadata().get();
            long committedOffset = committed.get(tp).offset();

            long lag = leo - committedOffset;

            System.out.printf("  LEO = %d (총 발행 건수)%n", leo);
            System.out.printf("  Committed Offset = %d (커밋된 위치)%n", committedOffset);
            System.out.printf("  Consumer Lag = %d - %d = %d%n", leo, committedOffset, lag);

            assertThat(leo).isEqualTo(10);
            assertThat(committedOffset).isEqualTo(3);
            assertThat(lag).isEqualTo(7);

            System.out.println("  → Lag = 7: Consumer가 7건 뒤쳐져 있다");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Producer 발행 속도 > Consumer 처리 속도이면 lag이 누적된다.
     *
     * Consumer가 처리하는 동안에도 Producer가 계속 발행하면 lag이 증가한다.
     */
    @Test
    void producer_속도가_consumer보다_빠르면_lag이_누적된다() {
        String testTopic = topic();
        String testGroupId = groupId();
        createTopic(testTopic, 1);

        TopicPartition tp = new TopicPartition(testTopic, 0);

        // 1차: 5건 발행
        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of()))) {
            for (int i = 0; i < 5; i++) {
                sendSync(producer, testTopic, null, "batch1-" + i);
            }
        }

        // Consumer: 2건만 처리
        try (var consumer = new KafkaConsumer<String, String>(
                consumerProps(testGroupId, Map.of(
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                        ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 2
                )))) {
            consumer.subscribe(List.of(testTopic));
            consumer.poll(Duration.ofSeconds(10));
            consumer.commitSync();
        }

        // 2차: 추가 5건 발행 (Consumer는 중단된 상태)
        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of()))) {
            for (int i = 0; i < 5; i++) {
                sendSync(producer, testTopic, null, "batch2-" + i);
            }
        }

        // lag 확인
        try (AdminClient admin = AdminClient.create(Map.of(
                "bootstrap.servers", BOOTSTRAP_SERVERS))) {

            long leo = admin.listOffsets(Map.of(tp, OffsetSpec.latest()))
                    .partitionResult(tp).get().offset();
            long committedOffset = admin.listConsumerGroupOffsets(testGroupId)
                    .partitionsToOffsetAndMetadata().get().get(tp).offset();
            long lag = leo - committedOffset;

            System.out.printf("  1차 발행: 5건, Consumer 처리: 2건%n");
            System.out.printf("  2차 발행: 5건 추가 (Consumer 중단)%n");
            System.out.printf("  LEO=%d, Committed=%d, Lag=%d%n", leo, committedOffset, lag);

            assertThat(leo).isEqualTo(10);
            assertThat(committedOffset).isEqualTo(2);
            assertThat(lag).isEqualTo(8);

            System.out.println("  → Lag 8: Consumer가 따라가지 못하면 계속 쌓인다");
            System.out.println("  → Lag은 장애의 첫 번째 신호 (Step 9 모니터링)");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
