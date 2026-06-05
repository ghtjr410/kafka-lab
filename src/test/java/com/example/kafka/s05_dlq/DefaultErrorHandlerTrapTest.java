package com.example.kafka.s05_dlq;

import com.example.kafka.KafkaTestBase;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * DefaultErrorHandler의 기본 동작이 DLQ가 아님을 증명한다.
 *
 * 사고 시나리오:
 *   @KafkaListener에서 파싱 실패가 발생했는데, DefaultErrorHandler가
 *   기본 10회 재시도 후 "로그만 남기고 skip"했다. DLQ로 보낸 줄 알았는데
 *   DLQ 설정을 안 했던 것. 실패한 메시지 수천 건이 조용히 사라졌다.
 *
 * DefaultErrorHandler의 기본 동작: FixedBackOff(0L, 9) → 0초 간격 9회 재시도 후 로그만 남기고 skip.
 * DLQ로 보내려면 DeadLetterPublishingRecoverer를 명시적으로 설정해야 한다.
 *
 * 이 테스트는 Spring Kafka의 @KafkaListener 대신 native API로 재현한다.
 * 재시도 로직과 DLQ 라우팅을 직접 구현하여 내부 동작을 이해한다.
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class DefaultErrorHandlerTrapTest extends KafkaTestBase {

    /**
     * DefaultErrorHandler 기본 동작: 재시도 후 로그만 남기고 skip한다. DLQ가 아니다!
     *
     * Spring Kafka의 DefaultErrorHandler는 기본적으로:
     *   1. FixedBackOff(0L, 9) → 0초 간격 9회 재시도
     *   2. 최대 재시도 초과 → 로그만 남기고 skip
     *   3. offset은 커밋됨 → 메시지가 조용히 사라짐
     *
     * DeadLetterPublishingRecoverer를 설정하지 않으면 DLQ로 가지 않는다.
     */
    @Test
    void DefaultErrorHandler_기본_동작은_재시도_후_skip이다_DLQ가_아니다() {
        String testTopic = topic();
        String testGroupId = groupId();
        createTopic(testTopic, 1);

        // 실패할 메시지 발행
        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of()))) {
            sendSync(producer, testTopic, null, "invalid-json-{{{");
            sendSync(producer, testTopic, null, "valid-message");
        }

        // DefaultErrorHandler 기본 동작 시뮬레이션
        AtomicInteger retryCount = new AtomicInteger(0);
        int maxRetries = 9; // FixedBackOff(0L, 9)
        List<String> processedMessages = new ArrayList<>();
        List<String> skippedMessages = new ArrayList<>();

        try (var consumer = new KafkaConsumer<String, String>(
                consumerProps(testGroupId, Map.of(
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false
                )))) {
            consumer.subscribe(List.of(testTopic));

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
            for (ConsumerRecord<String, String> record : records) {
                boolean success = false;

                // 재시도 루프 (DefaultErrorHandler 동작 재현)
                for (int attempt = 0; attempt <= maxRetries; attempt++) {
                    try {
                        // 파싱 시도 — invalid-json은 항상 실패
                        if (record.value().startsWith("invalid")) {
                            throw new RuntimeException("파싱 실패: " + record.value());
                        }
                        processedMessages.add(record.value());
                        success = true;
                        break;
                    } catch (Exception e) {
                        retryCount.incrementAndGet();
                    }
                }

                if (!success) {
                    // DLQ 설정 없음 → 로그만 남기고 skip!
                    skippedMessages.add(record.value());
                    System.out.printf("  [SKIP] %d회 재시도 후 skip: %s%n",
                            maxRetries + 1, record.value());
                }
            }

            // offset 커밋 — skip된 메시지도 커밋됨!
            consumer.commitSync();
        }

        assertThat(processedMessages).containsExactly("valid-message");
        assertThat(skippedMessages).containsExactly("invalid-json-{{{");
        assertThat(retryCount.get()).isEqualTo(maxRetries + 1); // 초기 시도 + 9회 재시도

        System.out.printf("  재시도 횟수: %d (0초 간격 × %d회)%n", retryCount.get(), maxRetries);
        System.out.println("  → skip된 메시지는 offset이 커밋되어 다시 받을 수 없다");
        System.out.println("  → DLQ 설정 안 하면 메시지가 조용히 사라진다!");
    }

    /**
     * DLQ를 설정하면 실패한 메시지가 DLT 토픽으로 이동한다.
     *
     * DeadLetterPublishingRecoverer를 설정하면:
     *   최대 재시도 초과 → topic-dlt 토픽으로 발행 → 원본 토픽의 offset 커밋
     *
     * DLQ 메시지에는 원본 토픽, 파티션, offset 정보가 Header로 포함된다.
     */
    @Test
    void DLQ를_설정하면_실패한_메시지가_DLT_토픽으로_이동한다() {
        String testTopic = topic();
        String dltTopic = testTopic + "-dlt"; // Spring Kafka 기본 DLT 네이밍
        String testGroupId = groupId();
        createTopic(testTopic, 1);
        createTopic(dltTopic, 1);

        // 실패할 메시지 + 정상 메시지 발행
        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of()))) {
            sendSync(producer, testTopic, "key-1", "poison-pill");
            sendSync(producer, testTopic, "key-2", "normal-event");
        }

        int maxRetries = 3;
        List<String> processed = new ArrayList<>();

        // Consumer: 실패 시 DLT로 라우팅 (DeadLetterPublishingRecoverer 동작 재현)
        try (var consumer = new KafkaConsumer<String, String>(
                consumerProps(testGroupId, Map.of(
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false
                )));
             var dlqProducer = new KafkaProducer<String, String>(producerProps(Map.of(
                     ProducerConfig.ACKS_CONFIG, "all"
             )))) {

            consumer.subscribe(List.of(testTopic));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));

            for (ConsumerRecord<String, String> record : records) {
                boolean success = false;
                Exception lastError = null;

                for (int attempt = 0; attempt <= maxRetries; attempt++) {
                    try {
                        if (record.value().equals("poison-pill")) {
                            throw new RuntimeException("처리 불가!");
                        }
                        processed.add(record.value());
                        success = true;
                        break;
                    } catch (Exception e) {
                        lastError = e;
                        System.out.printf("  [RETRY %d/%d] %s: %s%n",
                                attempt + 1, maxRetries + 1, record.value(), e.getMessage());
                    }
                }

                if (!success) {
                    // DLQ로 라우팅 (DeadLetterPublishingRecoverer 동작)
                    sendSync(dlqProducer, dltTopic, record.key(),
                            record.value() + " | error: " + lastError.getMessage());
                    System.out.printf("  [DLQ] → %s로 이동%n", dltTopic);
                }
            }

            consumer.commitSync();
        }

        // DLT 토픽에서 실패 메시지 확인
        try (var dltConsumer = new KafkaConsumer<String, String>(
                consumerProps(groupId(), Map.of(
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false
                )))) {
            dltConsumer.subscribe(List.of(dltTopic));

            List<String> dltValues = pollValues(dltConsumer, 1, Duration.ofSeconds(10));
            assertThat(dltValues).hasSize(1);
            assertThat(dltValues.get(0)).contains("poison-pill");

            System.out.printf("  DLT 토픽 메시지: %s%n", dltValues.get(0));
        }

        assertThat(processed).containsExactly("normal-event");
        System.out.println("  → DLQ 설정 시: 실패 → 재시도 → DLT 토픽 → 정상 메시지 계속 처리");
        System.out.println("  → DLQ 미설정 시: 실패 → 재시도 → 로그만 남기고 skip → 유실!");
    }
}
