package com.example.kafka.rebalancing;

import com.example.kafka.KafkaTestBase;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * max.poll.interval.ms 초과 시 Consumer 강제 퇴출을 증명한다.
 *
 * 사고 시나리오:
 *   poll() 후 처리에 5분 걸리는데 max.poll.interval.ms가 기본값(5분)이면
 *   Consumer가 "죽은 것"으로 판단되어 강제로 그룹에서 퇴출된다.
 *   이때 이미 가져온 메시지를 처리 중이었지만 offset은 커밋되지 않았으므로
 *   다른 Consumer가 같은 메시지를 재처리한다 (중복 처리).
 *
 * yml 대응:
 *   spring.kafka.consumer.max-poll-interval-ms=300000 (기본 5분)
 *   spring.kafka.consumer.max-poll-records=500 (기본)
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class MaxPollIntervalTest extends KafkaTestBase {

    /**
     * max_poll_interval을_초과하면_Consumer가_강제_퇴출된다.
     *
     * poll() 간격이 max.poll.interval.ms를 초과하면 브로커가 Consumer를 그룹에서
     * 제거하고 리밸런싱을 트리거한다.
     *
     * 퇴출된 Consumer는 다음 poll() 시 CommitFailedException을 받는다.
     * (이 테스트에서는 짧은 interval로 퇴출 상황을 재현한다)
     *
     * yml 대응: spring.kafka.consumer.max-poll-interval-ms=300000
     */
    @Test
    void max_poll_interval을_초과하면_Consumer가_강제_퇴출된다() throws Exception {
        String testTopic = topic();
        String testGroupId = groupId();
        createTopic(testTopic, 2);

        // 메시지 발행
        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of()))) {
            for (int i = 0; i < 6; i++) {
                sendSync(producer, testTopic, "key-" + i, "msg-" + i);
            }
        }

        // 매우 짧은 max.poll.interval.ms로 설정하여 퇴출 재현
        int shortPollInterval = 5000; // 5초

        Map<String, Object> overrides = Map.of(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, shortPollInterval,
                ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000,
                ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 1000
        );

        AtomicBoolean wasRevoked = new AtomicBoolean(false);
        List<String> revokedPartitions = new CopyOnWriteArrayList<>();

        try (var consumer = new KafkaConsumer<String, String>(
                consumerProps(testGroupId, overrides))) {
            consumer.subscribe(List.of(testTopic), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    if (!partitions.isEmpty()) {
                        wasRevoked.set(true);
                        for (TopicPartition tp : partitions) {
                            revokedPartitions.add("P" + tp.partition());
                        }
                        System.out.printf("  [Revoked] 파티션 회수됨: %s%n", partitions);
                    }
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    System.out.printf("  [Assigned] 파티션 할당됨: %s%n", partitions);
                }
            });

            // 1차 poll: 정상 할당
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
            System.out.printf("  [1차 poll] %d건 수신%n", records.count());
            assertThat(records.count()).isGreaterThan(0);

            // max.poll.interval.ms(5초) 초과 대기 → 처리가 너무 오래 걸리는 상황 시뮬레이션
            System.out.printf("  [대기] %d초 대기 (max.poll.interval.ms=%d 초과)%n",
                    shortPollInterval / 1000 + 3, shortPollInterval);
            Thread.sleep(shortPollInterval + 3000);

            // 2차 poll: 퇴출 후 재합류 → 리밸런싱 발생
            ConsumerRecords<String, String> afterTimeout = consumer.poll(Duration.ofSeconds(10));
            System.out.printf("  [2차 poll] 퇴출 후 재합류 → %d건 재수신%n", afterTimeout.count());
        }

        assertThat(wasRevoked.get())
                .as("max.poll.interval.ms 초과로 파티션이 revoke되어야 한다")
                .isTrue();

        System.out.println();
        System.out.println("  → max.poll.interval.ms 초과 = Consumer 강제 퇴출");
        System.out.println("  → 퇴출된 동안 커밋 못 한 메시지는 다른 Consumer가 재처리 (중복!)");
        System.out.println("  → 대응: max-poll-records 줄이기 or max.poll.interval.ms 늘리기");
    }

    /**
     * max_poll_records를_줄이면_처리_시간_내에_poll을_완료할_수_있다.
     *
     * max.poll.records를 줄이면 한 번에 가져오는 메시지 수가 줄어들어
     * 처리 시간이 단축되고, max.poll.interval.ms를 초과하지 않는다.
     *
     * yml 대응:
     *   spring.kafka.consumer.max-poll-records=100
     *   spring.kafka.consumer.max-poll-interval-ms=300000
     */
    @Test
    void max_poll_records를_줄이면_퇴출을_방지할_수_있다() throws Exception {
        String testTopic = topic();
        String testGroupId = groupId();
        createTopic(testTopic, 1);

        // 10건 발행
        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of()))) {
            for (int i = 0; i < 10; i++) {
                sendSync(producer, testTopic, "key-" + i, "msg-" + i);
            }
        }

        // max.poll.records=2로 제한
        Map<String, Object> overrides = Map.of(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 2,
                ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 10000
        );

        List<String> allValues = new ArrayList<>();
        int pollCount = 0;

        try (var consumer = new KafkaConsumer<String, String>(
                consumerProps(testGroupId, overrides))) {
            consumer.subscribe(List.of(testTopic));

            // 여러 번 poll하여 전체 메시지 소비
            while (allValues.size() < 10) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
                if (records.isEmpty()) break;

                pollCount++;
                int batchSize = records.count();
                records.forEach(r -> allValues.add(r.value()));

                // 레코드별 100ms 처리 (max.poll.records=2이면 200ms → interval 10초 내 충분)
                Thread.sleep(batchSize * 100L);

                consumer.commitSync();
                System.out.printf("  [poll #%d] %d건 처리 + 커밋 (누적: %d건)%n",
                        pollCount, batchSize, allValues.size());
            }
        }

        assertThat(allValues).hasSize(10);
        assertThat(pollCount).isGreaterThanOrEqualTo(5); // 10건 / 2건 = 최소 5회

        System.out.println();
        System.out.printf("  → max.poll.records=2: %d회 poll로 10건 소비%n", pollCount);
        System.out.println("  → 한 번에 적게 가져와서 처리 시간 < max.poll.interval.ms 유지");
        System.out.println("  → 퇴출 방지의 핵심: 처리량(max-poll-records)과 시간(max-poll-interval) 밸런스");
    }
}
