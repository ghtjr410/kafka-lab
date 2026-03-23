package com.example.kafka.eos;

import com.example.kafka.KafkaTestBase;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * EOS(Exactly-Once Semantics)의 보장 범위를 증명한다.
 *
 * 이 lab에서 가장 중요한 테스트.
 *
 * 사고 시나리오:
 *   KafkaTransactionManager를 설정했으니 중복이 없을 거라고 믿었는데,
 *   Consumer → DB insert 후 장애 → 재시작 시 같은 주문이 2건 생성됐다.
 *   EOS의 보장 범위를 몰랐기 때문.
 *
 * EOS가 보장하는 것: Kafka 내부 (produce → consume offset commit) 원자성
 * EOS가 보장하지 않는 것: 외부 시스템 (DB insert, API 호출)
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class EOSBoundaryTest extends KafkaTestBase {

    /**
     * Kafka 내부(produce → consume offset)는 exactly-once가 보장된다.
     *
     * 트랜잭셔널 프로듀서가 메시지 발행과 offset 커밋을 하나의 트랜잭션으로 묶으면,
     * 둘 다 성공하거나 둘 다 실패한다. Kafka 내부에서는 중복이 없다.
     */
    @Test
    void Kafka_내부_produce에서_consume_offset까지는_exactly_once가_보장된다() {
        String sourceTopic = topic() + "-source";
        String sinkTopic = topic() + "-sink";
        String testGroupId = groupId();
        createTopic(sourceTopic, 1);
        createTopic(sinkTopic, 1);

        // Source 토픽에 메시지 발행
        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of()))) {
            for (int i = 0; i < 5; i++) {
                sendSync(producer, sourceTopic, "key-" + i, "event-" + i);
            }
        }

        // Kafka-to-Kafka 파이프라인: consume → produce → offset commit (트랜잭셔널)
        String txId = "tx-eos-" + testGroupId;
        try (var consumer = new KafkaConsumer<String, String>(
                consumerProps(testGroupId, Map.of(
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                        ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"
                )));
             var txProducer = new KafkaProducer<String, String>(producerProps(Map.of(
                     ProducerConfig.ACKS_CONFIG, "all",
                     ProducerConfig.TRANSACTIONAL_ID_CONFIG, txId
             )))) {

            txProducer.initTransactions();
            consumer.subscribe(List.of(sourceTopic));

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
            assertThat(records.count()).isEqualTo(5);

            // 트랜잭션: produce + offset commit 원자적
            txProducer.beginTransaction();
            for (ConsumerRecord<String, String> record : records) {
                txProducer.send(new ProducerRecord<>(sinkTopic, record.key(),
                        "processed-" + record.value()));
            }
            txProducer.sendOffsetsToTransaction(
                    Map.of(new org.apache.kafka.common.TopicPartition(sourceTopic, 0),
                            new org.apache.kafka.clients.consumer.OffsetAndMetadata(5)),
                    consumer.groupMetadata());
            txProducer.commitTransaction();
        }

        // Sink 토픽에서 결과 확인
        try (var sinkConsumer = new KafkaConsumer<String, String>(
                consumerProps(groupId(), Map.of(
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                        ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"
                )))) {
            sinkConsumer.subscribe(List.of(sinkTopic));

            List<String> values = pollValues(sinkConsumer, 5, Duration.ofSeconds(10));
            assertThat(values).hasSize(5);
            assertThat(values).allMatch(v -> v.startsWith("processed-"));

            System.out.println("  Kafka-to-Kafka 파이프라인: 5건 exactly-once 처리");
            System.out.println("  → produce + offset commit이 하나의 트랜잭션");
            System.out.println("  → Kafka 내부에서는 중복 없음");
        }
    }

    /**
     * Consumer → DB insert 후 장애 → DB 중복.
     * EOS는 외부 시스템(DB)을 보장하지 않는다.
     *
     * 시나리오:
     *   1. Consumer가 메시지를 받아 DB에 insert한다.
     *   2. offset commit 전에 장애가 발생한다.
     *   3. 재시작 → 같은 메시지를 다시 받는다.
     *   4. DB에 같은 데이터가 2번 insert된다.
     *
     * EOS가 보장하는 건 Kafka 내부(produce → offset commit)의 원자성이지,
     * DB insert는 처음부터 보장 대상이 아니다.
     */
    @Test
    void Consumer에서_DB_insert_후_장애나면_DB에_중복이_발생한다_EOS_범위_밖() {
        String testTopic = topic();
        String testGroupId = groupId();
        createTopic(testTopic, 1);

        // 주문 이벤트 발행
        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of()))) {
            sendSync(producer, testTopic, "order-1", "payment-completed");
        }

        // 가상 DB (Map으로 시뮬레이션)
        Map<String, Integer> db = new ConcurrentHashMap<>();

        // 첫 번째 Consumer: DB insert 후 offset commit 전에 장애
        try (var consumer1 = new KafkaConsumer<String, String>(
                consumerProps(testGroupId, Map.of(
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false
                )))) {
            consumer1.subscribe(List.of(testTopic));

            ConsumerRecords<String, String> records = consumer1.poll(Duration.ofSeconds(10));
            for (ConsumerRecord<String, String> record : records) {
                // DB insert (성공)
                db.merge(record.key(), 1, Integer::sum);
                System.out.printf("  Consumer 1: DB insert (key=%s, count=%d)%n",
                        record.key(), db.get(record.key()));

                // offset commit 전에 장애 발생!
                System.out.println("  Consumer 1: offset commit 전에 장애 발생!");
                // commitSync()를 호출하지 않음
            }
        }

        // 두 번째 Consumer: 재시작 → 같은 메시지 재수신 → DB 중복 insert
        try (var consumer2 = new KafkaConsumer<String, String>(
                consumerProps(testGroupId, Map.of(
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false
                )))) {
            consumer2.subscribe(List.of(testTopic));

            ConsumerRecords<String, String> records = consumer2.poll(Duration.ofSeconds(10));
            for (ConsumerRecord<String, String> record : records) {
                // 같은 메시지를 다시 DB insert → 중복!
                db.merge(record.key(), 1, Integer::sum);
                System.out.printf("  Consumer 2: DB insert (key=%s, count=%d) → 중복!%n",
                        record.key(), db.get(record.key()));
            }
            consumer2.commitSync();
        }

        // order-1이 DB에 2번 들어감
        assertThat(db.get("order-1")).isEqualTo(2);

        System.out.println();
        System.out.println("  DB 상태: order-1 = 2건 (중복!)");
        System.out.println("  → EOS는 Kafka 내부(produce→offset)만 보장");
        System.out.println("  → DB insert는 EOS 범위 밖 → 중복 발생");
        System.out.println("  → 해결: Consumer 측 멱등키 (INSERT ... ON CONFLICT DO NOTHING)");
    }

    /**
     * Consumer 측 멱등키로 외부 시스템 중복을 방어한다.
     *
     * EOS가 보장하지 못하는 외부 시스템(DB, API) 중복은
     * Consumer가 멱등키(idempotency key)를 사용해 직접 방어해야 한다.
     * 이것이 최종 방어선이다.
     */
    @Test
    void Consumer_측_멱등키로_외부_시스템_중복을_방어한다() {
        String testTopic = topic();
        String testGroupId = groupId();
        createTopic(testTopic, 1);

        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of()))) {
            sendSync(producer, testTopic, "order-1", "payment-completed");
        }

        // 멱등 DB (중복 방어)
        Set<String> processedKeys = ConcurrentHashMap.newKeySet(); // INSERT ... ON CONFLICT 시뮬레이션
        Map<String, Integer> db = new ConcurrentHashMap<>();

        // 첫 번째 처리 (commit 안 함)
        try (var consumer1 = new KafkaConsumer<String, String>(
                consumerProps(testGroupId, Map.of(
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false
                )))) {
            consumer1.subscribe(List.of(testTopic));

            ConsumerRecords<String, String> records = consumer1.poll(Duration.ofSeconds(10));
            for (ConsumerRecord<String, String> record : records) {
                if (processedKeys.add(record.key())) { // 멱등키 체크
                    db.merge(record.key(), 1, Integer::sum);
                    System.out.printf("  Consumer 1: 처리 완료 (key=%s)%n", record.key());
                }
                // commit 안 함 → 장애
            }
        }

        // 두 번째 처리 (재수신)
        try (var consumer2 = new KafkaConsumer<String, String>(
                consumerProps(testGroupId, Map.of(
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false
                )))) {
            consumer2.subscribe(List.of(testTopic));

            ConsumerRecords<String, String> records = consumer2.poll(Duration.ofSeconds(10));
            for (ConsumerRecord<String, String> record : records) {
                if (processedKeys.add(record.key())) { // 멱등키 체크 → 이미 처리됨
                    db.merge(record.key(), 1, Integer::sum);
                } else {
                    System.out.printf("  Consumer 2: 멱등키로 중복 차단 (key=%s)%n", record.key());
                }
            }
            consumer2.commitSync();
        }

        // DB에 1건만 존재
        assertThat(db.get("order-1")).isEqualTo(1);

        System.out.println();
        System.out.println("  DB 상태: order-1 = 1건 (정상)");
        System.out.println("  → 멱등키(INSERT ON CONFLICT DO NOTHING)로 중복 방어");
        System.out.println("  → EOS의 보장 범위 밖은 Consumer 멱등성이 최종 방어선");
    }
}
