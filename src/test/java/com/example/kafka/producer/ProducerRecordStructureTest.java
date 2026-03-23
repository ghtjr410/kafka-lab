package com.example.kafka.producer;

import com.example.kafka.KafkaTestBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ProducerRecord의 구조를 확인한다.
 *
 * Kafka 메시지는 key, value, headers, timestamp로 구성된다.
 * 이 테스트는 각 필드의 역할과 Consumer에서의 확인 방법을 증명한다.
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class ProducerRecordStructureTest extends KafkaTestBase {

    /**
     * ProducerRecord는 key, value, headers, timestamp를 포함한다.
     *
     * RecordMetadata에서 topic, partition, offset, timestamp를 확인할 수 있다.
     */
    @Test
    void send의_반환값_RecordMetadata에서_메시지_구조를_확인할_수_있다() {
        String testTopic = topic();
        createTopic(testTopic, 1);

        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of(
                ProducerConfig.ACKS_CONFIG, "all"
        )))) {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    testTopic, "order-123", "payment-completed");

            try {
                RecordMetadata metadata = producer.send(record).get();

                assertThat(metadata.topic()).isEqualTo(testTopic);
                assertThat(metadata.partition()).isGreaterThanOrEqualTo(0);
                assertThat(metadata.offset()).isGreaterThanOrEqualTo(0);
                assertThat(metadata.timestamp()).isGreaterThan(0);

                System.out.printf("  topic=%s, partition=%d, offset=%d, timestamp=%d%n",
                        metadata.topic(), metadata.partition(),
                        metadata.offset(), metadata.timestamp());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Header에 correlation-id를 추가하면 Consumer에서 확인할 수 있다.
     *
     * 분산 시스템에서 요청 추적(tracing)의 기본이다.
     * Header는 key-value 쌍으로, byte[]로 저장된다.
     */
    @Test
    void Header에_correlation_id를_추가하면_Consumer에서_확인할_수_있다() {
        String testTopic = topic();
        String testGroupId = groupId();
        createTopic(testTopic, 1);
        String correlationId = "req-abc-123";

        // Producer: Header에 correlation-id 추가
        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of(
                ProducerConfig.ACKS_CONFIG, "all"
        )))) {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    testTopic, "order-1", "event-data");
            record.headers().add("correlation-id",
                    correlationId.getBytes(StandardCharsets.UTF_8));

            producer.send(record).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // Consumer: Header에서 correlation-id 확인
        try (var consumer = new KafkaConsumer<String, String>(
                consumerProps(testGroupId, Map.of()))) {
            consumer.subscribe(List.of(testTopic));

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
            assertThat(records.count()).isEqualTo(1);

            ConsumerRecord<String, String> received = records.iterator().next();
            Header header = received.headers().lastHeader("correlation-id");

            assertThat(header).isNotNull();
            String receivedCorrelationId = new String(header.value(), StandardCharsets.UTF_8);
            assertThat(receivedCorrelationId).isEqualTo(correlationId);

            System.out.printf("  Producer → Header[correlation-id]=%s%n", correlationId);
            System.out.printf("  Consumer ← Header[correlation-id]=%s%n", receivedCorrelationId);
            System.out.println("  → 분산 추적의 기본: 요청 ID를 Header로 전파");
        }
    }
}
