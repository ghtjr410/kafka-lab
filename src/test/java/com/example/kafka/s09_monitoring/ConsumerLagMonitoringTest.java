package com.example.kafka.s09_monitoring;

import com.example.kafka.KafkaTestBase;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * AdminClient를 사용한 Consumer Lag 모니터링을 증명한다.
 *
 * 사고 시나리오:
 *   Consumer가 처리를 못 따라가는데 아무도 모르고 있었다.
 *   lag이 100만 건 쌓인 후에야 발견. AdminClient로 실시간 모니터링을 해야 했다.
 *
 * 운영 도구 대응:
 *   kafka-consumer-groups.sh --describe --group my-group
 *   → LAG 컬럼이 이 테스트에서 계산하는 값과 동일
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class ConsumerLagMonitoringTest extends KafkaTestBase {

    /**
     * AdminClient로_Consumer_Lag을_계산할_수_있다.
     *
     * Lag = Log End Offset(LEO) - Committed Offset
     * AdminClient의 listConsumerGroupOffsets()와 listOffsets()를 조합하면
     * 프로그래밍 방식으로 lag을 계산할 수 있다.
     *
     * 운영 도구: kafka-consumer-groups.sh --describe --group {group}
     */
    @Test
    void AdminClient로_Consumer_Lag을_계산할_수_있다() throws Exception {
        String testTopic = topic();
        String testGroupId = groupId();
        createTopic(testTopic, 1);

        // 10건 발행
        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of()))) {
            for (int i = 0; i < 10; i++) {
                sendSync(producer, testTopic, null, "msg-" + i);
            }
        }

        // Consumer: 5건만 소비 후 커밋
        try (var consumer = new KafkaConsumer<String, String>(
                consumerProps(testGroupId, Map.of(
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                        ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5
                )))) {
            consumer.subscribe(List.of(testTopic));
            consumer.poll(Duration.ofSeconds(10));
            consumer.commitSync();
            System.out.println("  5건 소비 + 커밋 완료");
        }

        // AdminClient로 Lag 계산
        try (AdminClient admin = AdminClient.create(Map.of(
                "bootstrap.servers", BOOTSTRAP_SERVERS))) {

            TopicPartition tp = new TopicPartition(testTopic, 0);

            // 1) Committed Offset 조회
            Map<TopicPartition, OffsetAndMetadata> committed =
                    admin.listConsumerGroupOffsets(testGroupId)
                            .partitionsToOffsetAndMetadata().get();
            long committedOffset = committed.get(tp).offset();

            // 2) Log End Offset (LEO) 조회
            Map<TopicPartition, OffsetSpec> request = Map.of(tp, OffsetSpec.latest());
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets =
                    admin.listOffsets(request).all().get();
            long logEndOffset = endOffsets.get(tp).offset();

            // 3) Lag = LEO - Committed
            long lag = logEndOffset - committedOffset;

            System.out.printf("  Committed Offset: %d%n", committedOffset);
            System.out.printf("  Log End Offset (LEO): %d%n", logEndOffset);
            System.out.printf("  Lag: %d (= %d - %d)%n", lag, logEndOffset, committedOffset);

            assertThat(committedOffset).isEqualTo(5);
            assertThat(logEndOffset).isEqualTo(10);
            assertThat(lag).isEqualTo(5);

            System.out.println();
            System.out.println("  → Lag = LEO - Committed Offset");
            System.out.println("  → kafka-consumer-groups.sh --describe와 동일한 계산");
            System.out.println("  → 이 값을 Grafana/Prometheus로 수집하면 실시간 모니터링 가능");
        }
    }

    /**
     * Consumer가_중단되면_lag이_계속_증가한다.
     *
     * Consumer가 중단된 상태에서 Producer가 계속 발행하면
     * Committed Offset은 고정되고 LEO만 증가하므로 lag이 폭증한다.
     * 이것이 Consumer 장애 시 가장 먼저 감지해야 하는 지표다.
     */
    @Test
    void Consumer가_중단되면_lag이_계속_증가한다() throws Exception {
        String testTopic = topic();
        String testGroupId = groupId();
        createTopic(testTopic, 1);

        // Consumer: 처음 5건 소비 후 중단
        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of()))) {
            for (int i = 0; i < 5; i++) {
                sendSync(producer, testTopic, null, "batch1-" + i);
            }
        }

        try (var consumer = new KafkaConsumer<String, String>(
                consumerProps(testGroupId, Map.of(
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false
                )))) {
            consumer.subscribe(List.of(testTopic));
            pollValues(consumer, 5, Duration.ofSeconds(10));
            consumer.commitSync();
        }
        // Consumer 중단됨!

        // Producer: 계속 발행 (Consumer 없음)
        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of()))) {
            for (int i = 0; i < 20; i++) {
                sendSync(producer, testTopic, null, "batch2-" + i);
            }
        }

        // Lag 추이 확인
        try (AdminClient admin = AdminClient.create(Map.of(
                "bootstrap.servers", BOOTSTRAP_SERVERS))) {

            TopicPartition tp = new TopicPartition(testTopic, 0);

            Map<TopicPartition, OffsetAndMetadata> committed =
                    admin.listConsumerGroupOffsets(testGroupId)
                            .partitionsToOffsetAndMetadata().get();
            long committedOffset = committed.get(tp).offset();

            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets =
                    admin.listOffsets(Map.of(tp, OffsetSpec.latest())).all().get();
            long logEndOffset = endOffsets.get(tp).offset();
            long lag = logEndOffset - committedOffset;

            System.out.printf("  Committed: %d (Consumer 중단 시점에 고정)%n", committedOffset);
            System.out.printf("  LEO: %d (Producer가 계속 발행)%n", logEndOffset);
            System.out.printf("  Lag: %d건 밀림!%n", lag);

            assertThat(committedOffset).isEqualTo(5);
            assertThat(logEndOffset).isEqualTo(25); // 5 + 20
            assertThat(lag).isEqualTo(20);

            System.out.println();
            System.out.println("  → Consumer 중단 → Committed 고정 + LEO 증가 → Lag 폭증");
            System.out.println("  → 알림 조건: Lag > 임계값 or Lag 증가율 > 임계값");
        }
    }

    /**
     * 여러_파티션의_lag을_합산하여_전체_그룹_lag을_계산한다.
     *
     * 멀티 파티션 토픽에서는 파티션별 lag을 합산해야
     * Consumer Group 전체의 밀림 정도를 알 수 있다.
     *
     * 운영 도구: kafka-consumer-groups.sh에서 LAG 컬럼의 합
     */
    @Test
    void 여러_파티션의_lag을_합산하여_전체_그룹_lag을_계산한다() throws Exception {
        String testTopic = topic();
        String testGroupId = groupId();
        createTopic(testTopic, 3);

        // 각 파티션에 분산 발행
        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of()))) {
            for (int i = 0; i < 15; i++) {
                sendSync(producer, testTopic, "key-" + i, "msg-" + i);
            }
        }

        // 일부만 소비
        try (var consumer = new KafkaConsumer<String, String>(
                consumerProps(testGroupId, Map.of(
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                        ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5
                )))) {
            consumer.subscribe(List.of(testTopic));
            consumer.poll(Duration.ofSeconds(10));
            consumer.commitSync();
        }

        // 파티션별 lag 계산
        try (AdminClient admin = AdminClient.create(Map.of(
                "bootstrap.servers", BOOTSTRAP_SERVERS))) {

            Map<TopicPartition, OffsetAndMetadata> committed =
                    admin.listConsumerGroupOffsets(testGroupId)
                            .partitionsToOffsetAndMetadata().get();

            // 모든 파티션의 LEO 조회
            Map<TopicPartition, OffsetSpec> latestRequest = new HashMap<>();
            for (int p = 0; p < 3; p++) {
                latestRequest.put(new TopicPartition(testTopic, p), OffsetSpec.latest());
            }
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets =
                    admin.listOffsets(latestRequest).all().get();

            long totalLag = 0;
            for (int p = 0; p < 3; p++) {
                TopicPartition tp = new TopicPartition(testTopic, p);
                long leo = endOffsets.get(tp).offset();
                long com = committed.containsKey(tp) ? committed.get(tp).offset() : 0;
                long partitionLag = leo - com;
                totalLag += partitionLag;
                System.out.printf("  P%d: LEO=%d, Committed=%d, Lag=%d%n", p, leo, com, partitionLag);
            }

            System.out.printf("  Total Lag: %d건%n", totalLag);
            assertThat(totalLag).isGreaterThan(0);

            System.out.println();
            System.out.println("  → 전체 Lag = 파티션별 Lag의 합");
            System.out.println("  → 파티션별 Lag 편차가 크면 특정 Consumer가 느린 것");
            System.out.println("  → Grafana: kafka_consumer_group_lag{group, topic, partition}");
        }
    }
}
