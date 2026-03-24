package com.example.kafka.s01_producer;

import com.example.kafka.KafkaTestBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Producer의 백프레셔(backpressure) 동작을 증명한다.
 *
 * 사고 시나리오:
 *   브로커가 느려서 send() 내부 버퍼(buffer.memory)가 가득 찼다.
 *   max.block.ms 동안 블로킹되다가 시간 초과 시 TimeoutException이 발생한다.
 *   이것이 Kafka의 자연스러운 백프레셔 메커니즘이다.
 *
 * yml 대응:
 *   spring.kafka.producer.properties.buffer.memory=33554432 (기본 32MB)
 *   spring.kafka.producer.properties.max.block.ms=60000 (기본 1분)
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class ProducerBackpressureTest extends KafkaTestBase {

    /**
     * max_block_ms가_만료되면_send에서_TimeoutException이_발생한다.
     *
     * 존재하지 않는 브로커로 전송하면 메타데이터를 가져올 수 없어
     * max.block.ms 동안 블로킹된 후 TimeoutException이 발생한다.
     * 이는 buffer.memory 초과 시와 동일한 메커니즘이다.
     *
     * yml 대응:
     *   max.block.ms=60000 (기본 1분)
     */
    @Test
    void max_block_ms가_만료되면_send에서_TimeoutException이_발생한다() {
        // 존재하지 않는 브로커 → 메타데이터 타임아웃으로 백프레셔 재현
        Map<String, Object> props = new java.util.HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19999"); // 존재하지 않는 포트
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 2000L); // 2초 블로킹 후 타임아웃

        try (var producer = new KafkaProducer<String, String>(props)) {
            long start = System.currentTimeMillis();

            boolean timedOut = false;
            try {
                producer.send(new ProducerRecord<>("any-topic", null, "test")).get();
            } catch (ExecutionException e) {
                if (e.getCause() instanceof TimeoutException) {
                    timedOut = true;
                }
            } catch (Exception e) {
                if (e instanceof TimeoutException) {
                    timedOut = true;
                }
            }

            long elapsed = System.currentTimeMillis() - start;

            assertThat(timedOut).as("max.block.ms 초과 시 TimeoutException 발생").isTrue();
            assertThat(elapsed).isGreaterThanOrEqualTo(1800); // 최소 ~2초 대기

            System.out.printf("  max.block.ms=2000: %dms 후 TimeoutException%n", elapsed);
            System.out.println();
            System.out.println("  → send()는 비동기지만 메타데이터/버퍼 대기는 블로킹!");
            System.out.println("  → 브로커 불능 시 send() 호출 스레드가 멈출 수 있다");
            System.out.println("  → 대응: max.block.ms를 적절히 설정 + 에러 핸들링");
        }
    }

    /**
     * buffer_memory가_충분하면_send는_즉시_반환된다.
     *
     * 정상 상황에서 send()는 내부 버퍼에 메시지를 넣고 즉시 반환한다.
     * 실제 브로커 전송은 별도 스레드(Sender)에서 비동기로 처리된다.
     * buffer.memory가 충분하면 send()는 블로킹되지 않는다.
     */
    @Test
    void buffer_memory가_충분하면_send는_즉시_반환된다() {
        String testTopic = topic();
        createTopic(testTopic, 1);

        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of(
                ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432L, // 32MB (기본값)
                ProducerConfig.LINGER_MS_CONFIG, 100            // 배치 대기
        )))) {
            // send()의 반환 시간 측정 (비동기이므로 매우 빠름)
            long start = System.currentTimeMillis();
            for (int i = 0; i < 100; i++) {
                producer.send(new ProducerRecord<>(testTopic, null, "msg-" + i));
                // future.get()을 호출하지 않음 → send()만 측정
            }
            long elapsed = System.currentTimeMillis() - start;

            System.out.printf("  buffer.memory=32MB: 100건 send() → %dms (블로킹 없음)%n", elapsed);

            // 버퍼가 충분하면 send()는 거의 즉시 반환
            assertThat(elapsed).isLessThan(1000);

            // flush로 실제 전송 완료 대기
            producer.flush();

            System.out.println("  → send()는 버퍼에 넣고 즉시 반환 (비동기)");
            System.out.println("  → 실제 전송은 Sender 스레드가 담당");
            System.out.println("  → 버퍼 부족 시에만 max.block.ms만큼 블로킹");
        }
    }
}
