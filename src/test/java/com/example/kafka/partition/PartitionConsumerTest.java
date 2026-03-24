package com.example.kafka.partition;

import com.example.kafka.KafkaTestBase;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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
 * 파티션 수와 Consumer 수의 관계를 증명한다.
 *
 * 사고 시나리오:
 *   파티션이 3개인 토픽에 Consumer를 5대 띄웠는데 2대가 놀고 있다.
 *   파티션 수가 Consumer 수의 상한이라는 것을 몰랐다.
 *   Consumer를 늘려도 파티션 수 이상은 병렬 처리가 불가능하다.
 *
 * yml 대응:
 *   KAFKA_NUM_PARTITIONS: 3 (브로커 기본 파티션 수)
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class PartitionConsumerTest extends KafkaTestBase {

    /**
     * Consumer_수가_파티션_수보다_많으면_놀리는_Consumer가_발생한다.
     *
     * Kafka는 하나의 파티션을 같은 그룹의 여러 Consumer에 할당하지 않는다.
     * 따라서 Consumer 수 > 파티션 수이면 초과 Consumer는 아무 파티션도 할당받지 못한다.
     */
    @Test
    void Consumer_수가_파티션_수보다_많으면_놀리는_Consumer가_발생한다() throws Exception {
        String testTopic = topic();
        String testGroupId = groupId();
        int partitionCount = 2;
        int consumerCount = 4;
        createTopic(testTopic, partitionCount);

        // 메시지 발행
        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of()))) {
            for (int i = 0; i < 10; i++) {
                sendSync(producer, testTopic, "key-" + i, "msg-" + i);
            }
        }

        Map<String, Object> overrides = Map.of(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000,
                ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 1000
        );

        Map<Integer, Set<Integer>> consumerAssignments = new ConcurrentHashMap<>();
        CountDownLatch allReady = new CountDownLatch(consumerCount);
        AtomicBoolean running = new AtomicBoolean(true);
        ExecutorService executor = Executors.newFixedThreadPool(consumerCount);
        List<Future<?>> futures = new ArrayList<>();

        // Consumer 4대 기동 (파티션 2개)
        for (int c = 0; c < consumerCount; c++) {
            final int consumerId = c;
            futures.add(executor.submit(() -> {
                try (var consumer = new KafkaConsumer<String, String>(
                        consumerProps(testGroupId, overrides))) {
                    consumer.subscribe(List.of(testTopic));

                    // 파티션 할당 대기
                    consumer.poll(Duration.ofSeconds(15));
                    Set<Integer> assigned = new HashSet<>();
                    consumer.assignment().forEach(tp -> assigned.add(tp.partition()));
                    consumerAssignments.put(consumerId, assigned);
                    allReady.countDown();

                    while (running.get()) {
                        consumer.poll(Duration.ofMillis(500));
                        // 할당 업데이트 (리밸런싱 후)
                        Set<Integer> current = new HashSet<>();
                        consumer.assignment().forEach(tp -> current.add(tp.partition()));
                        consumerAssignments.put(consumerId, current);
                    }
                }
            }));
        }

        assertThat(allReady.await(30, TimeUnit.SECONDS)).isTrue();
        Thread.sleep(3000); // 리밸런싱 안정화 대기

        running.set(false);
        for (var future : futures) {
            future.get(10, TimeUnit.SECONDS);
        }
        executor.shutdown();

        // 결과 분석
        int activeConsumers = 0;
        int idleConsumers = 0;
        for (var entry : consumerAssignments.entrySet()) {
            boolean isIdle = entry.getValue().isEmpty();
            System.out.printf("  Consumer %d: %s%s%n",
                    entry.getKey(),
                    entry.getValue().isEmpty() ? "파티션 없음 (놀림)" : "파티션 " + entry.getValue(),
                    isIdle ? " ← IDLE!" : "");
            if (isIdle) idleConsumers++;
            else activeConsumers++;
        }

        System.out.println();
        System.out.printf("  파티션: %d개, Consumer: %d대%n", partitionCount, consumerCount);
        System.out.printf("  활성: %d대, 유휴: %d대%n", activeConsumers, idleConsumers);

        assertThat(activeConsumers).isEqualTo(partitionCount);
        assertThat(idleConsumers).isEqualTo(consumerCount - partitionCount);

        System.out.println();
        System.out.println("  → Consumer 수 > 파티션 수 → 초과분은 놀림");
        System.out.println("  → 병렬 처리 상한 = 파티션 수");
        System.out.println("  → Consumer 늘리기 전에 파티션 수를 먼저 확인!");
    }

    /**
     * Consumer_수가_파티션_수와_같으면_1대_1로_할당된다.
     *
     * Consumer 수 = 파티션 수일 때 가장 이상적인 분배가 이루어진다.
     * 각 Consumer가 정확히 1개의 파티션을 담당한다.
     */
    @Test
    void Consumer_수가_파티션_수와_같으면_1대_1로_할당된다() throws Exception {
        String testTopic = topic();
        String testGroupId = groupId();
        int count = 3; // 파티션 수 = Consumer 수
        createTopic(testTopic, count);

        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of()))) {
            for (int i = 0; i < 9; i++) {
                sendSync(producer, testTopic, "key-" + i, "msg-" + i);
            }
        }

        Map<String, Object> overrides = Map.of(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000,
                ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 1000
        );

        Map<Integer, Set<Integer>> consumerAssignments = new ConcurrentHashMap<>();
        CountDownLatch allReady = new CountDownLatch(count);
        AtomicBoolean running = new AtomicBoolean(true);
        ExecutorService executor = Executors.newFixedThreadPool(count);
        List<Future<?>> futures = new ArrayList<>();

        for (int c = 0; c < count; c++) {
            final int consumerId = c;
            futures.add(executor.submit(() -> {
                try (var consumer = new KafkaConsumer<String, String>(
                        consumerProps(testGroupId, overrides))) {
                    consumer.subscribe(List.of(testTopic));
                    consumer.poll(Duration.ofSeconds(15));
                    Set<Integer> assigned = new HashSet<>();
                    consumer.assignment().forEach(tp -> assigned.add(tp.partition()));
                    consumerAssignments.put(consumerId, assigned);
                    allReady.countDown();

                    while (running.get()) {
                        consumer.poll(Duration.ofMillis(500));
                        Set<Integer> current = new HashSet<>();
                        consumer.assignment().forEach(tp -> current.add(tp.partition()));
                        consumerAssignments.put(consumerId, current);
                    }
                }
            }));
        }

        assertThat(allReady.await(30, TimeUnit.SECONDS)).isTrue();
        Thread.sleep(3000);

        running.set(false);
        for (var future : futures) {
            future.get(10, TimeUnit.SECONDS);
        }
        executor.shutdown();

        // 모든 Consumer가 정확히 1개 파티션 담당
        Set<Integer> allPartitions = new HashSet<>();
        for (var entry : consumerAssignments.entrySet()) {
            System.out.printf("  Consumer %d: 파티션 %s%n", entry.getKey(), entry.getValue());
            assertThat(entry.getValue()).hasSize(1);
            allPartitions.addAll(entry.getValue());
        }

        // 모든 파티션이 커버됨
        assertThat(allPartitions).hasSize(count);

        System.out.println();
        System.out.printf("  파티션 %d개 = Consumer %d대 → 1:1 이상적 분배%n", count, count);
        System.out.println("  → 최적 설정: Consumer 수 = 파티션 수");
    }
}
