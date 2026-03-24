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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Consumer offset 수동 조작(seek, AdminClient alterConsumerGroupOffsets)을 증명한다.
 *
 * 사고 시나리오:
 *   버그로 인해 최근 1000건의 메시지가 잘못 처리되었다.
 *   offset을 되돌려서 재처리해야 하는데, auto.offset.reset은 새 그룹에만 적용된다.
 *   기존 그룹의 offset을 수동으로 조작해야 한다.
 *
 * 운영 도구 대응:
 *   kafka-consumer-groups.sh --reset-offsets --to-earliest --group my-group --topic my-topic --execute
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class ConsumerOffsetResetToolTest extends KafkaTestBase {

    /**
     * seekToBeginning으로_처음부터_재소비할_수_있다.
     *
     * Consumer.seekToBeginning()은 현재 할당된 파티션의 offset을
     * 가장 처음으로 되돌린다. 커밋하면 그룹의 committed offset이 변경된다.
     *
     * 운영 도구: kafka-consumer-groups.sh --reset-offsets --to-earliest
     */
    @Test
    void seekToBeginning으로_처음부터_재소비할_수_있다() {
        String testTopic = topic();
        String testGroupId = groupId();
        createTopic(testTopic, 1);

        // 10건 발행
        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of()))) {
            for (int i = 0; i < 10; i++) {
                sendSync(producer, testTopic, null, "msg-" + i);
            }
        }

        // 1차: 전부 소비 + 커밋
        try (var consumer = new KafkaConsumer<String, String>(
                consumerProps(testGroupId, Map.of(
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false
                )))) {
            consumer.subscribe(List.of(testTopic));
            List<String> values = pollValues(consumer, 10, Duration.ofSeconds(10));
            consumer.commitSync();
            System.out.printf("  [1차 소비] %d건 소비 + 커밋 완료%n", values.size());
        }

        // 2차: seekToBeginning → 처음부터 재소비
        try (var consumer = new KafkaConsumer<String, String>(
                consumerProps(testGroupId, Map.of(
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false
                )))) {
            consumer.subscribe(List.of(testTopic));
            consumer.poll(Duration.ofSeconds(5)); // 파티션 할당 대기

            // 처음으로 되돌리기
            consumer.seekToBeginning(consumer.assignment());
            System.out.println("  [seekToBeginning] offset을 처음으로 되돌림");

            List<String> reValues = pollValues(consumer, 10, Duration.ofSeconds(10));
            assertThat(reValues).hasSize(10);
            assertThat(reValues.get(0)).isEqualTo("msg-0");

            System.out.printf("  [재소비] %d건 재소비 (msg-0부터)%n", reValues.size());
            System.out.println("  → seekToBeginning: 할당된 파티션의 offset을 처음으로");
        }
    }

    /**
     * seek으로_특정_offset부터_재소비할_수_있다.
     *
     * Consumer.seek(partition, offset)으로 특정 위치로 이동할 수 있다.
     * "최근 5건만 재처리" 같은 시나리오에 활용한다.
     *
     * 운영 도구: kafka-consumer-groups.sh --reset-offsets --to-offset 5
     */
    @Test
    void seek으로_특정_offset부터_재소비할_수_있다() {
        String testTopic = topic();
        String testGroupId = groupId();
        createTopic(testTopic, 1);

        // 10건 발행
        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of()))) {
            for (int i = 0; i < 10; i++) {
                sendSync(producer, testTopic, null, "msg-" + i);
            }
        }

        // 전부 소비 + 커밋
        try (var consumer = new KafkaConsumer<String, String>(
                consumerProps(testGroupId, Map.of(
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false
                )))) {
            consumer.subscribe(List.of(testTopic));
            pollValues(consumer, 10, Duration.ofSeconds(10));
            consumer.commitSync();
        }

        // seek(offset=7) → msg-7, msg-8, msg-9만 재소비
        try (var consumer = new KafkaConsumer<String, String>(
                consumerProps(testGroupId, Map.of(
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false
                )))) {
            consumer.subscribe(List.of(testTopic));
            consumer.poll(Duration.ofSeconds(5)); // 파티션 할당 대기

            TopicPartition tp = new TopicPartition(testTopic, 0);
            consumer.seek(tp, 7);
            System.out.println("  [seek] offset=7로 이동");

            List<String> values = pollValues(consumer, 3, Duration.ofSeconds(10));
            assertThat(values).containsExactly("msg-7", "msg-8", "msg-9");

            System.out.printf("  [재소비] %s%n", values);
            System.out.println("  → seek(partition, offset): 특정 위치부터 재소비");
        }
    }

    /**
     * AdminClient로_Consumer_Group의_offset을_수동_변경할_수_있다.
     *
     * alterConsumerGroupOffsets()로 Consumer가 중단된 상태에서
     * 그룹의 committed offset을 변경할 수 있다.
     * 운영 중 장애 복구 시 가장 많이 사용하는 방법이다.
     *
     * 운영 도구: kafka-consumer-groups.sh --reset-offsets --to-offset 3 --execute
     */
    @Test
    void AdminClient로_Consumer_Group의_offset을_수동_변경할_수_있다() throws Exception {
        String testTopic = topic();
        String testGroupId = groupId();
        createTopic(testTopic, 1);

        // 10건 발행
        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of()))) {
            for (int i = 0; i < 10; i++) {
                sendSync(producer, testTopic, null, "msg-" + i);
            }
        }

        // 전부 소비 + 커밋 (committed offset = 10)
        try (var consumer = new KafkaConsumer<String, String>(
                consumerProps(testGroupId, Map.of(
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false
                )))) {
            consumer.subscribe(List.of(testTopic));
            pollValues(consumer, 10, Duration.ofSeconds(10));
            consumer.commitSync();
        }

        // AdminClient로 offset을 3으로 변경 (Consumer 중단 상태에서)
        TopicPartition tp = new TopicPartition(testTopic, 0);
        try (AdminClient admin = AdminClient.create(Map.of(
                "bootstrap.servers", BOOTSTRAP_SERVERS))) {

            // 변경 전 확인
            Map<TopicPartition, OffsetAndMetadata> before =
                    admin.listConsumerGroupOffsets(testGroupId)
                            .partitionsToOffsetAndMetadata().get();
            System.out.printf("  변경 전 committed offset: %d%n", before.get(tp).offset());

            // offset을 3으로 변경
            Map<TopicPartition, OffsetAndMetadata> newOffsets = new HashMap<>();
            newOffsets.put(tp, new OffsetAndMetadata(3));
            admin.alterConsumerGroupOffsets(testGroupId, newOffsets).all().get();

            // 변경 후 확인
            Map<TopicPartition, OffsetAndMetadata> after =
                    admin.listConsumerGroupOffsets(testGroupId)
                            .partitionsToOffsetAndMetadata().get();
            System.out.printf("  변경 후 committed offset: %d%n", after.get(tp).offset());

            assertThat(after.get(tp).offset()).isEqualTo(3);
        }

        // 새 Consumer: offset=3부터 재소비
        try (var consumer = new KafkaConsumer<String, String>(
                consumerProps(testGroupId, Map.of(
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false
                )))) {
            consumer.subscribe(List.of(testTopic));
            List<String> values = pollValues(consumer, 7, Duration.ofSeconds(10));

            assertThat(values).hasSize(7);
            assertThat(values.get(0)).isEqualTo("msg-3");

            System.out.printf("  [재소비] offset=3부터 %d건: %s ... %s%n",
                    values.size(), values.get(0), values.get(values.size() - 1));
            System.out.println();
            System.out.println("  → alterConsumerGroupOffsets: Consumer 중단 후 offset 변경");
            System.out.println("  → 장애 복구: 잘못 처리된 구간의 offset을 되돌려 재처리");
        }
    }
}
