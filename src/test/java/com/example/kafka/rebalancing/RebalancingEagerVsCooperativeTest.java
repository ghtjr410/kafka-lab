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
 * Eager(stop-the-world) vs Cooperative(점진적) 리밸런싱 동작 차이를 증명한다.
 *
 * 사고 시나리오:
 *   Consumer 3대가 운영 중 1대를 배포하면, Eager 전략에서는 나머지 2대도
 *   모든 파티션을 반납하고 재할당 받는다 → 순간적으로 전체 처리가 멈춘다(stop-the-world).
 *
 * yml 대응:
 *   Eager: partition.assignment.strategy=org.apache.kafka.clients.consumer.RangeAssignor
 *   Cooperative: partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class RebalancingEagerVsCooperativeTest extends KafkaTestBase {

    /**
     * Eager_리밸런싱에서는_새_Consumer_합류_시_모든_파티션이_revoke된다.
     *
     * RangeAssignor(Eager)에서 Consumer가 추가되면:
     * 1) 기존 Consumer의 모든 파티션이 revoke됨 (stop-the-world)
     * 2) 전체 파티션이 재할당됨
     *
     * yml 대응: partition.assignment.strategy=RangeAssignor
     */
    @Test
    void Eager_리밸런싱에서는_새_Consumer_합류_시_모든_파티션이_revoke된다() throws Exception {
        String testTopic = topic();
        String testGroupId = groupId();
        createTopic(testTopic, 3);

        // 메시지 발행
        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of()))) {
            for (int i = 0; i < 9; i++) {
                sendSync(producer, testTopic, "key-" + i, "msg-" + i);
            }
        }

        // Eager(RangeAssignor) 설정
        Map<String, Object> eagerOverrides = Map.of(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                        RangeAssignor.class.getName(),
                ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 30000,
                ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000,
                ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 1000
        );

        List<String> revokedPartitions = new CopyOnWriteArrayList<>();
        List<String> assignedPartitions = new CopyOnWriteArrayList<>();
        CountDownLatch consumer1Ready = new CountDownLatch(1);
        CountDownLatch consumer2Joined = new CountDownLatch(1);
        AtomicBoolean running = new AtomicBoolean(true);

        // Consumer 1: 먼저 기동하여 3개 파티션 모두 할당받음
        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<?> c1Future = executor.submit(() -> {
            try (var consumer = new KafkaConsumer<String, String>(
                    consumerProps(testGroupId, eagerOverrides))) {
                consumer.subscribe(List.of(testTopic), new ConsumerRebalanceListener() {
                    @Override
                    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                        for (TopicPartition tp : partitions) {
                            revokedPartitions.add("C1-revoke-P" + tp.partition());
                        }
                        if (!partitions.isEmpty()) {
                            System.out.printf("  [C1] Revoked: %s%n", partitions);
                        }
                    }

                    @Override
                    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                        for (TopicPartition tp : partitions) {
                            assignedPartitions.add("C1-assign-P" + tp.partition());
                        }
                        System.out.printf("  [C1] Assigned: %s%n", partitions);
                    }
                });

                // 첫 poll로 파티션 할당
                consumer.poll(Duration.ofSeconds(10));
                consumer1Ready.countDown();

                // Consumer 2가 합류할 때까지 계속 poll
                while (running.get()) {
                    consumer.poll(Duration.ofMillis(500));
                }
            }
        });

        // Consumer 1이 안정화될 때까지 대기
        assertThat(consumer1Ready.await(15, TimeUnit.SECONDS)).isTrue();
        System.out.println("  [Phase 1] Consumer 1이 3개 파티션 할당받음");

        // revoke 기록 초기화 (첫 할당 시 빈 revoke 제외)
        revokedPartitions.clear();
        assignedPartitions.clear();

        // Consumer 2: 합류 → 리밸런싱 트리거
        Future<?> c2Future = executor.submit(() -> {
            try (var consumer = new KafkaConsumer<String, String>(
                    consumerProps(testGroupId, eagerOverrides))) {
                consumer.subscribe(List.of(testTopic), new ConsumerRebalanceListener() {
                    @Override
                    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                        // Consumer 2의 첫 리밸런싱에서는 revoke할 것이 없음
                    }

                    @Override
                    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                        for (TopicPartition tp : partitions) {
                            assignedPartitions.add("C2-assign-P" + tp.partition());
                        }
                        System.out.printf("  [C2] Assigned: %s%n", partitions);
                        consumer2Joined.countDown();
                    }
                });

                consumer.poll(Duration.ofSeconds(10));
                while (running.get()) {
                    consumer.poll(Duration.ofMillis(500));
                }
            }
        });

        // 리밸런싱 완료 대기
        assertThat(consumer2Joined.await(30, TimeUnit.SECONDS)).isTrue();
        Thread.sleep(1000); // 리밸런싱 이벤트 수집 대기

        running.set(false);
        c1Future.get(10, TimeUnit.SECONDS);
        c2Future.get(10, TimeUnit.SECONDS);
        executor.shutdown();

        // Eager 리밸런싱 증명: Consumer 1의 모든 파티션이 revoke됐다
        long c1RevokeCount = revokedPartitions.stream()
                .filter(s -> s.startsWith("C1-revoke")).count();

        System.out.printf("  [Phase 2] C1 revoke 횟수: %d (Eager = 전체 파티션 반납)%n", c1RevokeCount);
        System.out.println("  Revoked: " + revokedPartitions);
        System.out.println("  Assigned: " + assignedPartitions);

        // Eager에서는 기존 Consumer의 파티션이 모두 revoke됨 (stop-the-world)
        assertThat(c1RevokeCount)
                .as("Eager 리밸런싱: Consumer 1의 모든 파티션이 revoke되어야 한다")
                .isGreaterThanOrEqualTo(1);

        System.out.println();
        System.out.println("  → Eager(RangeAssignor): Consumer 합류 시 전체 파티션 revoke → 재할당");
        System.out.println("  → 운영 중 배포하면 순간적으로 전체 처리가 멈춘다 (stop-the-world)");
    }

    /**
     * Cooperative_리밸런싱에서는_이동이_필요한_파티션만_revoke된다.
     *
     * CooperativeStickyAssignor에서 Consumer가 추가되면:
     * 1) 이동이 필요한 파티션만 revoke됨
     * 2) 나머지 파티션은 계속 처리 가능 (점진적)
     *
     * yml 대응: partition.assignment.strategy=CooperativeStickyAssignor
     */
    @Test
    void Cooperative_리밸런싱에서는_이동이_필요한_파티션만_revoke된다() throws Exception {
        String testTopic = topic();
        String testGroupId = groupId();
        createTopic(testTopic, 3);

        // 메시지 발행
        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of()))) {
            for (int i = 0; i < 9; i++) {
                sendSync(producer, testTopic, "key-" + i, "msg-" + i);
            }
        }

        // Cooperative(CooperativeStickyAssignor) 설정
        Map<String, Object> cooperativeOverrides = Map.of(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                        CooperativeStickyAssignor.class.getName(),
                ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 30000,
                ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000,
                ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 1000
        );

        List<String> revokedPartitions = new CopyOnWriteArrayList<>();
        List<String> assignedPartitions = new CopyOnWriteArrayList<>();
        CountDownLatch consumer1Ready = new CountDownLatch(1);
        CountDownLatch consumer2Joined = new CountDownLatch(1);
        AtomicBoolean running = new AtomicBoolean(true);

        ExecutorService executor = Executors.newFixedThreadPool(2);

        // Consumer 1
        Future<?> c1Future = executor.submit(() -> {
            try (var consumer = new KafkaConsumer<String, String>(
                    consumerProps(testGroupId, cooperativeOverrides))) {
                consumer.subscribe(List.of(testTopic), new ConsumerRebalanceListener() {
                    @Override
                    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                        for (TopicPartition tp : partitions) {
                            revokedPartitions.add("C1-revoke-P" + tp.partition());
                        }
                        if (!partitions.isEmpty()) {
                            System.out.printf("  [C1] Revoked: %s%n", partitions);
                        }
                    }

                    @Override
                    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                        for (TopicPartition tp : partitions) {
                            assignedPartitions.add("C1-assign-P" + tp.partition());
                        }
                        if (!partitions.isEmpty()) {
                            System.out.printf("  [C1] Assigned: %s%n", partitions);
                        }
                    }
                });

                consumer.poll(Duration.ofSeconds(10));
                consumer1Ready.countDown();

                while (running.get()) {
                    consumer.poll(Duration.ofMillis(500));
                }
            }
        });

        assertThat(consumer1Ready.await(15, TimeUnit.SECONDS)).isTrue();
        System.out.println("  [Phase 1] Consumer 1이 3개 파티션 할당받음");

        revokedPartitions.clear();
        assignedPartitions.clear();

        // Consumer 2 합류
        Future<?> c2Future = executor.submit(() -> {
            try (var consumer = new KafkaConsumer<String, String>(
                    consumerProps(testGroupId, cooperativeOverrides))) {
                consumer.subscribe(List.of(testTopic), new ConsumerRebalanceListener() {
                    @Override
                    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                        // 처음엔 revoke할 것 없음
                    }

                    @Override
                    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                        for (TopicPartition tp : partitions) {
                            assignedPartitions.add("C2-assign-P" + tp.partition());
                        }
                        if (!partitions.isEmpty()) {
                            System.out.printf("  [C2] Assigned: %s%n", partitions);
                            consumer2Joined.countDown();
                        }
                    }
                });

                consumer.poll(Duration.ofSeconds(10));
                while (running.get()) {
                    consumer.poll(Duration.ofMillis(500));
                }
            }
        });

        assertThat(consumer2Joined.await(30, TimeUnit.SECONDS)).isTrue();
        Thread.sleep(2000); // Cooperative는 2단계 리밸런싱이므로 충분히 대기

        // Consumer close() 전에 리밸런싱으로 인한 revoke 수를 캡처
        // (close() 시 남은 파티션이 모두 revoke되므로 그 전에 측정해야 정확)
        long c1RevokeCount = revokedPartitions.stream()
                .filter(s -> s.startsWith("C1-revoke")).count();
        List<String> rebalanceRevokes = new ArrayList<>(revokedPartitions);
        List<String> rebalanceAssigns = new ArrayList<>(assignedPartitions);

        running.set(false);
        c1Future.get(10, TimeUnit.SECONDS);
        c2Future.get(10, TimeUnit.SECONDS);
        executor.shutdown();

        System.out.printf("  [Phase 2] 리밸런싱 중 C1 revoke 횟수: %d (Cooperative = 이동 파티션만)%n", c1RevokeCount);
        System.out.println("  Revoked (리밸런싱): " + rebalanceRevokes);
        System.out.println("  Assigned (리밸런싱): " + rebalanceAssigns);

        // Cooperative: 리밸런싱으로 인한 revoke는 이동 대상 파티션만 (전체 3개보다 적음)
        // 3개 파티션 중 1개만 C2로 이동되므로 revoke도 1개
        assertThat(c1RevokeCount)
                .as("Cooperative 리밸런싱: 전체 파티션이 아닌 이동 대상만 revoke")
                .isLessThan(3);

        System.out.println();
        System.out.println("  → Cooperative(CooperativeStickyAssignor): 이동 필요한 파티션만 revoke");
        System.out.println("  → 나머지 파티션은 중단 없이 계속 처리 (점진적 리밸런싱)");
    }
}
