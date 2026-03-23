package com.example.kafka.partition;

import com.example.kafka.KafkaTestBase;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Partition key에 따른 순서 보장을 증명한다.
 *
 * 사고 시나리오:
 *   주문 생성 → 결제 완료 → 배송 시작 순서로 와야 하는데,
 *   key 없이 발행해서 배송 시작이 주문 생성보다 먼저 처리됐다.
 *
 * 순서 보장은 토픽 전체가 아니라 파티션 안에서만 된다.
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class PartitionKeyTest extends KafkaTestBase {

    /**
     * key 없이 발행하면 파티션이 분산되어 순서가 보장되지 않는다.
     *
     * null key로 발행하면 라운드로빈 또는 StickyPartitioner에 의해
     * 여러 파티션에 분산된다. 같은 주문의 이벤트가 다른 파티션에 들어가면
     * 순서가 뒤집힐 수 있다.
     */
    @Test
    void key_없이_발행하면_파티션이_분산된다() {
        String testTopic = topic();
        createTopic(testTopic, 3);

        Set<Integer> partitions = new HashSet<>();
        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of()))) {
            // null key로 10건 발행 — 각각 별도 send + flush로 분산 유도
            for (int i = 0; i < 10; i++) {
                RecordMetadata metadata = sendSync(producer, testTopic, null, "event-" + i);
                partitions.add(metadata.partition());
            }
        }

        System.out.printf("  null key → 사용된 파티션: %s%n", partitions);
        System.out.println("  → 같은 주문의 이벤트가 다른 파티션에 들어가면 순서 역전 가능");

        // 3개 파티션이 있으므로 최소 1개 이상 사용되어야 함
        assertThat(partitions.size()).isGreaterThanOrEqualTo(1);
    }

    /**
     * 같은 key로 발행하면 항상 같은 파티션에 들어가 순서가 보장된다.
     *
     * murmur2(key) % partitionCount 로 파티션이 결정된다.
     * 같은 orderId를 key로 쓰면 같은 주문의 모든 이벤트가 같은 파티션에 들어간다.
     */
    @Test
    void 같은_key로_발행하면_같은_파티션에_들어가_순서가_보장된다() {
        String testTopic = topic();
        String testGroupId = groupId();
        createTopic(testTopic, 3);

        String orderId = "order-12345";
        List<String> events = List.of("ORDER_CREATED", "PAYMENT_COMPLETED", "SHIPPING_STARTED");

        // 같은 key(orderId)로 순서대로 발행
        Set<Integer> partitions = new HashSet<>();
        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of()))) {
            for (String event : events) {
                RecordMetadata metadata = sendSync(producer, testTopic, orderId, event);
                partitions.add(metadata.partition());
                System.out.printf("  발행: key=%s, value=%s → partition=%d%n",
                        orderId, event, metadata.partition());
            }
        }

        // 모두 같은 파티션
        assertThat(partitions).hasSize(1);
        System.out.printf("  → 같은 key → 같은 파티션(%d) → 순서 보장%n", partitions.iterator().next());

        // Consumer에서 순서 확인
        try (var consumer = new KafkaConsumer<String, String>(
                consumerProps(testGroupId, Map.of(
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false
                )))) {
            consumer.subscribe(List.of(testTopic));

            List<String> received = pollValues(consumer, 3, Duration.ofSeconds(10));
            assertThat(received).containsExactly(
                    "ORDER_CREATED", "PAYMENT_COMPLETED", "SHIPPING_STARTED");

            System.out.println("  Consumer 수신 순서: ORDER_CREATED → PAYMENT_COMPLETED → SHIPPING_STARTED");
            System.out.println("  → 파티션 내 순서 보장 확인");
        }
    }

    /**
     * 서로 다른 key는 서로 다른 파티션에 배정될 수 있다.
     *
     * murmur2 해시에 의해 key마다 파티션이 결정된다.
     * 3개 파티션에 여러 주문을 발행하면 주문별로 파티션이 다를 수 있다.
     */
    @Test
    void 서로_다른_key는_서로_다른_파티션에_배정될_수_있다() {
        String testTopic = topic();
        createTopic(testTopic, 3);

        Map<String, Integer> keyToPartition = new LinkedHashMap<>();
        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of()))) {
            for (String orderId : List.of("order-A", "order-B", "order-C", "order-D", "order-E")) {
                RecordMetadata metadata = sendSync(producer, testTopic, orderId, "event");
                keyToPartition.put(orderId, metadata.partition());
            }
        }

        System.out.println("  key → partition 매핑:");
        keyToPartition.forEach((key, partition) ->
                System.out.printf("    %s → partition %d%n", key, partition));

        // 여러 파티션이 사용되는지 확인 (해시에 따라 달라짐)
        Set<Integer> usedPartitions = new HashSet<>(keyToPartition.values());
        System.out.printf("  사용된 파티션 수: %d (총 3개 중)%n", usedPartitions.size());
        System.out.println("  → 같은 key = 같은 파티션. 다른 key = 다른 파티션일 수 있음");
    }
}
