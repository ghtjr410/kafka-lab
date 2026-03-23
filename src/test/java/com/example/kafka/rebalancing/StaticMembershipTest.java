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
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Static Membership(group.instance.id)으로 불필요한 리밸런싱을 방지하는 것을 증명한다.
 *
 * 사고 시나리오:
 *   Consumer를 롤링 재시작할 때마다 리밸런싱이 발생한다.
 *   Kubernetes에서 Pod이 재시작될 때마다 전체 Consumer Group이 흔들린다.
 *   group.instance.id를 설정하면 session.timeout 내 재접속 시 리밸런싱이 발생하지 않는다.
 *
 * yml 대응:
 *   spring.kafka.consumer.group-instance-id=${HOSTNAME}
 *   spring.kafka.consumer.session-timeout-ms=30000
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class StaticMembershipTest extends KafkaTestBase {

    /**
     * Static_Membership_없이_재접속하면_리밸런싱이_발생한다.
     *
     * group.instance.id 없이(Dynamic Membership) Consumer를 끊었다 다시 연결하면
     * 매번 새로운 멤버로 인식되어 리밸런싱이 발생한다.
     */
    @Test
    void Static_Membership_없이_재접속하면_리밸런싱이_발생한다() throws Exception {
        String testTopic = topic();
        String testGroupId = groupId();
        createTopic(testTopic, 2);

        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of()))) {
            for (int i = 0; i < 4; i++) {
                sendSync(producer, testTopic, "key-" + i, "msg-" + i);
            }
        }

        Map<String, Object> dynamicOverrides = Map.of(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000,
                ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 1000
        );

        AtomicInteger rebalanceCount = new AtomicInteger(0);

        // 1차 접속
        Set<Integer> firstAssignment;
        try (var consumer = new KafkaConsumer<String, String>(
                consumerProps(testGroupId, dynamicOverrides))) {
            consumer.subscribe(List.of(testTopic), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    if (!partitions.isEmpty()) {
                        rebalanceCount.incrementAndGet();
                        System.out.printf("  [1차] Assigned: %s (리밸런싱 #%d)%n",
                                partitions, rebalanceCount.get());
                    }
                }
            });

            consumer.poll(Duration.ofSeconds(10));
            firstAssignment = new HashSet<>();
            consumer.assignment().forEach(tp -> firstAssignment.add(tp.partition()));
        }
        // Consumer 닫힘 → 세션 종료

        System.out.println("  [1차 종료] Consumer 닫힘 → 세션 종료");

        // 2차 접속: 새 멤버로 인식 → 리밸런싱 발생
        Set<Integer> secondAssignment;
        try (var consumer = new KafkaConsumer<String, String>(
                consumerProps(testGroupId, dynamicOverrides))) {
            consumer.subscribe(List.of(testTopic), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    if (!partitions.isEmpty()) {
                        rebalanceCount.incrementAndGet();
                        System.out.printf("  [2차] Assigned: %s (리밸런싱 #%d)%n",
                                partitions, rebalanceCount.get());
                    }
                }
            });

            consumer.poll(Duration.ofSeconds(10));
            secondAssignment = new HashSet<>();
            consumer.assignment().forEach(tp -> secondAssignment.add(tp.partition()));
        }

        // Dynamic: 매번 새 멤버 → 매번 리밸런싱
        assertThat(rebalanceCount.get()).isGreaterThanOrEqualTo(2);
        System.out.printf("  → Dynamic Membership: 총 %d회 리밸런싱 발생%n", rebalanceCount.get());
        System.out.println("  → 재접속할 때마다 리밸런싱 → 롤링 배포 시 불안정");
    }

    /**
     * Static_Membership으로_재접속_시_같은_파티션을_유지한다.
     *
     * group.instance.id를 설정하면 Consumer가 재접속해도 같은 멤버로 인식된다.
     * session.timeout 내에 재접속하면 파티션 재할당 없이 기존 할당을 유지한다.
     *
     * yml 대응:
     *   spring.kafka.consumer.group-instance-id=${HOSTNAME}
     */
    @Test
    void Static_Membership으로_재접속_시_같은_파티션을_유지한다() throws Exception {
        String testTopic = topic();
        String testGroupId = groupId();
        createTopic(testTopic, 2);

        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of()))) {
            for (int i = 0; i < 4; i++) {
                sendSync(producer, testTopic, "key-" + i, "msg-" + i);
            }
        }

        String instanceId = "static-member-" + UUID.randomUUID();

        Map<String, Object> staticOverrides = Map.of(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, instanceId,
                ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000,
                ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 1000
        );

        // 1차 접속: 파티션 할당
        Set<Integer> firstAssignment = new HashSet<>();
        try (var consumer = new KafkaConsumer<String, String>(
                consumerProps(testGroupId, staticOverrides))) {
            consumer.subscribe(List.of(testTopic));
            consumer.poll(Duration.ofSeconds(10));
            consumer.assignment().forEach(tp -> firstAssignment.add(tp.partition()));
            System.out.printf("  [1차] Assigned partitions: %s (instance=%s)%n",
                    firstAssignment, instanceId);
        }
        // Consumer 닫힘 — 하지만 session.timeout 내에 재접속하면 리밸런싱 없음

        // 즉시 2차 접속: 같은 group.instance.id
        Set<Integer> secondAssignment = new HashSet<>();
        try (var consumer = new KafkaConsumer<String, String>(
                consumerProps(testGroupId, staticOverrides))) {
            consumer.subscribe(List.of(testTopic));
            consumer.poll(Duration.ofSeconds(10));
            consumer.assignment().forEach(tp -> secondAssignment.add(tp.partition()));
            System.out.printf("  [2차] Assigned partitions: %s (instance=%s)%n",
                    secondAssignment, instanceId);
        }

        // Static Membership: 같은 파티션을 유지
        assertThat(secondAssignment).isEqualTo(firstAssignment);

        System.out.println();
        System.out.println("  → Static Membership: 같은 instance.id → 같은 파티션 유지");
        System.out.println("  → 롤링 재시작 시 불필요한 리밸런싱 방지");
        System.out.println("  → Kubernetes: group.instance.id=${HOSTNAME} 권장");
    }
}
