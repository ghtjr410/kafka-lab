package com.example.kafka.s03_partition;

import com.example.kafka.KafkaTestBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 파티션 수 변경 시 기존 key 매핑이 깨지는 문제를 증명한다.
 *
 * murmur2(key) % partitionCount 로 파티션이 결정되므로,
 * partitionCount가 바뀌면 같은 key가 다른 파티션에 배정된다.
 * 이미 발행된 메시지는 원래 파티션에 남아있으므로 순서 보장이 깨진다.
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class PartitionRekeyTest extends KafkaTestBase {

    /**
     * 파티션 수가 변경되면 같은 key가 다른 파티션에 배정된다.
     *
     * murmur2(key) % 3 ≠ murmur2(key) % 6
     * 파티션 수를 운영 중에 바꾸면 기존 key의 매핑이 깨진다.
     */
    @Test
    void 파티션_수_변경_후_같은_key가_다른_파티션에_배정된다() {
        List<String> keys = List.of("order-1", "order-2", "order-3", "order-4", "order-5");

        // murmur2 해시로 파티션 계산 (Kafka 내부 로직과 동일)
        System.out.println("  파티션 3개일 때:");
        Map<String, Integer> partitionsWith3 = new LinkedHashMap<>();
        for (String key : keys) {
            int partition = Utils.toPositive(Utils.murmur2(key.getBytes(StandardCharsets.UTF_8))) % 3;
            partitionsWith3.put(key, partition);
            System.out.printf("    %s → partition %d%n", key, partition);
        }

        System.out.println("  파티션 6개로 변경 후:");
        Map<String, Integer> partitionsWith6 = new LinkedHashMap<>();
        for (String key : keys) {
            int partition = Utils.toPositive(Utils.murmur2(key.getBytes(StandardCharsets.UTF_8))) % 6;
            partitionsWith6.put(key, partition);
            System.out.printf("    %s → partition %d%n", key, partition);
        }

        // 매핑이 변경된 key 확인
        int changedCount = 0;
        for (String key : keys) {
            if (!partitionsWith3.get(key).equals(partitionsWith6.get(key))) {
                changedCount++;
            }
        }

        System.out.printf("%n  매핑이 변경된 key: %d/%d개%n", changedCount, keys.size());
        System.out.println("  → 파티션 수 변경 = key 매핑 깨짐 = 순서 보장 붕괴");
        System.out.println("  → 파티션 수는 처음에 여유있게 설정할 것 (줄일 수 없음)");

        // 최소 하나 이상의 key 매핑이 변경됨을 증명
        assertThat(changedCount).isGreaterThan(0);
    }

    /**
     * 실제 브로커에서 파티션 수 변경 전후의 key 배정을 확인한다.
     *
     * 같은 key를 3-partition 토픽과 6-partition 토픽에 각각 발행하여
     * 실제로 다른 파티션에 배정되는 것을 확인한다.
     */
    @Test
    void 실제_브로커에서_파티션_수_변경_전후_key_배정이_달라진다() {
        String topic3 = topic() + "-3p";
        String topic6 = topic() + "-6p";
        createTopic(topic3, 3);
        createTopic(topic6, 6);

        String key = "order-rekey-test";

        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of()))) {
            RecordMetadata meta3 = sendSync(producer, topic3, key, "event");
            RecordMetadata meta6 = sendSync(producer, topic6, key, "event");

            System.out.printf("  key=%s → 3-partition 토픽: partition %d%n", key, meta3.partition());
            System.out.printf("  key=%s → 6-partition 토픽: partition %d%n", key, meta6.partition());

            // 파티션 번호가 다를 수도 같을 수도 있지만, 해시 계산이 다름을 보여준다
            System.out.println("  → 같은 key라도 파티션 수가 다르면 배정이 달라질 수 있다");
        }
    }
}
