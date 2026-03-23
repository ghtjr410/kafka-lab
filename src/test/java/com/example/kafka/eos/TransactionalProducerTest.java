package com.example.kafka.eos;

import com.example.kafka.KafkaTestBase;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 트랜잭셔널 프로듀서의 동작과 isolation-level 함정을 증명한다.
 *
 * yml 대응:
 *   spring.kafka.producer.transaction-id-prefix=tx-
 *   spring.kafka.consumer.isolation-level=read_committed (기본값은 read_uncommitted!)
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class TransactionalProducerTest extends KafkaTestBase {

    /**
     * 트랜잭셔널 프로듀서로 원자적 발행을 할 수 있다.
     *
     * beginTransaction() → send() → commitTransaction() 으로
     * 여러 메시지를 하나의 트랜잭션으로 묶을 수 있다.
     * 커밋되면 모두 보이고, abort되면 모두 안 보인다 (read_committed 기준).
     *
     * yml 대응: spring.kafka.producer.transaction-id-prefix=tx-
     */
    @Test
    void 트랜잭셔널_프로듀서로_원자적_발행을_할_수_있다() {
        String testTopic = topic();
        String testGroupId = groupId();
        createTopic(testTopic, 1);

        // 트랜잭셔널 프로듀서
        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of(
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tx-test-" + testGroupId
        )))) {
            producer.initTransactions();

            // 트랜잭션 내에서 2건 발행
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(testTopic, "order-1", "event-A"));
            producer.send(new ProducerRecord<>(testTopic, "order-1", "event-B"));
            producer.commitTransaction();

            System.out.println("  트랜잭션 커밋: event-A, event-B 원자적 발행");
        }

        // read_committed Consumer: 커밋된 메시지만 보인다
        try (var consumer = new KafkaConsumer<String, String>(
                consumerProps(testGroupId, Map.of(
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                        ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"
                )))) {
            consumer.subscribe(List.of(testTopic));

            List<String> values = pollValues(consumer, 2, Duration.ofSeconds(10));
            assertThat(values).containsExactly("event-A", "event-B");

            System.out.println("  read_committed Consumer: 2건 수신 (커밋된 트랜잭션)");
        }
    }

    /**
     * 트랜잭션 abort 시 read_committed Consumer는 메시지를 볼 수 없다.
     *
     * abortTransaction()을 호출하면 트랜잭션이 롤백된다.
     * read_committed Consumer는 abort된 메시지를 건너뛴다.
     */
    @Test
    void 트랜잭션_abort_시_read_committed_Consumer는_메시지를_볼_수_없다() {
        String testTopic = topic();
        String testGroupId = groupId();
        createTopic(testTopic, 1);

        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of(
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tx-abort-" + testGroupId
        )))) {
            producer.initTransactions();

            // abort될 트랜잭션
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(testTopic, "key", "aborted-event"));
            producer.abortTransaction();
            System.out.println("  트랜잭션 abort: aborted-event");

            // 커밋될 트랜잭션
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(testTopic, "key", "committed-event"));
            producer.commitTransaction();
            System.out.println("  트랜잭션 커밋: committed-event");
        }

        // read_committed: abort된 메시지는 안 보임
        try (var consumer = new KafkaConsumer<String, String>(
                consumerProps(testGroupId, Map.of(
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                        ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"
                )))) {
            consumer.subscribe(List.of(testTopic));

            List<String> values = pollValues(consumer, 1, Duration.ofSeconds(10));
            assertThat(values).containsExactly("committed-event");

            System.out.println("  read_committed Consumer: committed-event만 수신");
            System.out.println("  → abort된 aborted-event는 건너뜀");
        }
    }

    /**
     * read_uncommitted(기본값)이면 커밋되지 않은 트랜잭션의 메시지가 즉시 보인다.
     *
     * Consumer의 isolation.level 기본값은 read_uncommitted다!
     * 트랜잭션이 아직 진행 중(커밋/abort 전)인 메시지도 Consumer에 전달된다.
     * 즉, Producer에만 트랜잭션을 설정하고 Consumer 설정을 안 바꾸면
     * 트랜잭션의 원자성을 보장받지 못한다.
     *
     * 이 테스트는 같은 데이터를 read_committed vs read_uncommitted로 읽어
     * 차이를 비교한다.
     *
     * yml 대응:
     *   spring.kafka.consumer.isolation-level=read_committed (반드시 변경!)
     */
    @Test
    void read_committed와_read_uncommitted의_차이를_확인한다() {
        String testTopic = topic();
        createTopic(testTopic, 1);

        // 트랜잭션 커밋 + abort 발행
        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of(
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tx-compare-" + groupId()
        )))) {
            producer.initTransactions();

            // abort 트랜잭션
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(testTopic, "key", "should-be-hidden"));
            producer.abortTransaction();

            // 정상 커밋 트랜잭션
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(testTopic, "key", "visible-event"));
            producer.commitTransaction();
        }

        // read_committed: 커밋된 메시지만 보임
        List<String> committedValues;
        try (var consumer = new KafkaConsumer<String, String>(
                consumerProps(groupId(), Map.of(
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                        ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"
                )))) {
            consumer.subscribe(List.of(testTopic));
            committedValues = pollValues(consumer, 1, Duration.ofSeconds(10));
        }

        assertThat(committedValues).containsExactly("visible-event");
        System.out.printf("  read_committed: %d건 수신 → %s%n",
                committedValues.size(), committedValues);
        System.out.println("  → abort된 should-be-hidden은 필터링됨");

        // read_uncommitted: abort 마커 처리 방식에 따라 다를 수 있음
        List<String> uncommittedValues;
        try (var consumer = new KafkaConsumer<String, String>(
                consumerProps(groupId(), Map.of(
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                        ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_uncommitted"
                )))) {
            consumer.subscribe(List.of(testTopic));
            uncommittedValues = pollValues(consumer, 2, Duration.ofSeconds(10));
        }

        System.out.printf("  read_uncommitted: %d건 수신 → %s%n",
                uncommittedValues.size(), uncommittedValues);

        // read_uncommitted는 abort된 메시지를 포함할 수 있어 더 많은 메시지를 반환
        assertThat(uncommittedValues.size()).isGreaterThanOrEqualTo(committedValues.size());
        System.out.println();
        System.out.println("  → 기본값(read_uncommitted)은 트랜잭션 무결성을 보장하지 않는다");
        System.out.println("  → 트랜잭셔널 Producer 사용 시 반드시 read_committed로 변경!");
    }
}
