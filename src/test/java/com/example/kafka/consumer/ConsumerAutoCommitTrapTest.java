package com.example.kafka.consumer;

import com.example.kafka.KafkaTestBase;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * native kafka-clients의 auto-commit이 왜 위험한지 증명한다.
 *
 * Spring Kafka가 enable.auto.commit=false로 강제하는 이유를 이해하기 위해,
 * native auto-commit의 함정을 먼저 직접 밟아본다.
 *
 * yml 대응:
 *   spring.kafka.consumer.enable-auto-commit: false
 *   → Spring Kafka가 강제. yml에 true를 적어도 무시된다!
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class ConsumerAutoCommitTrapTest extends KafkaTestBase {

    /**
     * native auto-commit: 처리 실패해도 offset이 넘어간다.
     *
     * auto.commit.interval.ms(기본 5초)마다 poll() 시점에 자동 커밋된다.
     * 처리 성공/실패와 무관하게 커밋되므로, 실패한 메시지가 유실된다.
     * 이것이 at-most-once 동작이다.
     */
    @Test
    void auto_commit이면_처리_실패해도_offset이_넘어간다() {
        String testTopic = topic();
        String testGroupId = groupId();
        createTopic(testTopic, 1);

        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of()))) {
            sendSync(producer, testTopic, null, "important-event");
        }

        // auto-commit ON + 짧은 interval
        try (var consumer = new KafkaConsumer<String, String>(
                consumerProps(testGroupId, Map.of(
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true,
                        ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100
                )))) {
            consumer.subscribe(List.of(testTopic));

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
            assertThat(records.count()).isEqualTo(1);

            // 처리 "실패" — 하지만 커밋은 신경 안 씀
            System.out.println("  처리 실패 발생! (예외 throw 가정)");

            // 다음 poll()이 자동 커밋을 트리거
            Thread.sleep(200); // auto-commit interval 대기
            consumer.poll(Duration.ofMillis(100)); // 이 poll()에서 이전 offset 자동 커밋
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // 새 Consumer로 재접속 — 이미 커밋됨, 메시지 유실
        try (var consumer2 = new KafkaConsumer<String, String>(
                consumerProps(testGroupId, Map.of(
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false
                )))) {
            consumer2.subscribe(List.of(testTopic));
            ConsumerRecords<String, String> remaining = consumer2.poll(Duration.ofSeconds(3));

            assertThat(remaining.count()).isEqualTo(0);
            System.out.println("  → auto-commit이 처리 실패와 무관하게 offset을 넘김");
            System.out.println("  → 메시지 유실 (at-most-once)");
        }
    }

    /**
     * manual commit에서 커밋 전에 Consumer가 죽으면 중복 소비된다.
     *
     * 처리는 완료했지만 commitSync()를 호출하기 전에 장애가 발생하면,
     * offset이 커밋되지 않았으므로 재시작 시 같은 메시지를 다시 받는다.
     * 이것이 at-least-once 동작이다 — 유실은 없지만 중복은 발생한다.
     */
    @Test
    void manual_commit에서_커밋_전에_죽으면_중복_소비된다() {
        String testTopic = topic();
        String testGroupId = groupId();
        createTopic(testTopic, 1);

        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of()))) {
            sendSync(producer, testTopic, null, "payment-event");
        }

        // 첫 번째 Consumer: 메시지를 처리했지만 commitSync() 전에 장애 발생
        try (var consumer1 = new KafkaConsumer<String, String>(
                consumerProps(testGroupId, Map.of(
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false
                )))) {
            consumer1.subscribe(List.of(testTopic));

            ConsumerRecords<String, String> records = consumer1.poll(Duration.ofSeconds(10));
            assertThat(records.count()).isEqualTo(1);
            System.out.println("  Consumer 1: payment-event 수신 → DB insert 완료");
            System.out.println("  Consumer 1: commitSync() 호출 전에 장애 발생!");
            // commitSync()를 호출하지 않고 종료
        }

        // 두 번째 Consumer: 같은 그룹으로 재접속 → 같은 메시지 재수신
        try (var consumer2 = new KafkaConsumer<String, String>(
                consumerProps(testGroupId, Map.of(
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false
                )))) {
            consumer2.subscribe(List.of(testTopic));

            ConsumerRecords<String, String> records = consumer2.poll(Duration.ofSeconds(10));
            assertThat(records.count()).isEqualTo(1);

            System.out.println("  Consumer 2: payment-event 다시 수신 → 중복!");
            System.out.println("  → DB에는 이미 insert됨 + 같은 메시지 재처리 → 결제 2건");
            System.out.println("  → at-least-once: 유실은 없지만 중복은 발생한다");
            System.out.println("  → 해결: Consumer 측 멱등 처리 (Step 6)");
        }
    }

    /**
     * Spring Kafka가 auto-commit을 false로 강제하는 이유.
     *
     * 위 두 테스트에서 본 것처럼:
     * - auto-commit ON → 처리 실패해도 커밋됨 (유실)
     * - auto-commit ON → 커밋 전 죽으면 중복
     *
     * 어느 쪽이든 문제다. Spring Kafka는 이 문제를 AckMode로 해결한다.
     * enable.auto.commit=true를 yml에 적어도 Spring Kafka가 무시한다.
     */
    @Test
    void Spring_Kafka가_auto_commit을_false로_강제하는_이유를_이해한다() {
        String testTopic = topic();
        String testGroupId = groupId();
        createTopic(testTopic, 1);

        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of()))) {
            for (int i = 0; i < 3; i++) {
                sendSync(producer, testTopic, null, "event-" + i);
            }
        }

        // manual commit으로 전환: 처리 완료 후에만 커밋
        List<String> processed = new ArrayList<>();
        try (var consumer = new KafkaConsumer<String, String>(
                consumerProps(testGroupId, Map.of(
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false
                )))) {
            consumer.subscribe(List.of(testTopic));

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
            records.forEach(r -> {
                processed.add(r.value());
                System.out.printf("  처리 완료: %s%n", r.value());
            });

            // 처리 완료 확인 후에만 커밋
            consumer.commitSync();
        }

        assertThat(processed).containsExactly("event-0", "event-1", "event-2");
        System.out.println("  → manual commit: 처리 완료를 확인한 후에만 커밋");
        System.out.println("  → Spring Kafka의 AckMode가 이 패턴을 자동화한다");
    }
}
