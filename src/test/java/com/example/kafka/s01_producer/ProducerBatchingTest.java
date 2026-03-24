package com.example.kafka.s01_producer;

import com.example.kafka.KafkaTestBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Producer의 배치(batching) 동작을 증명한다.
 *
 * 사고 시나리오:
 *   메시지를 건건이 보내니 처리량이 낮다. linger.ms와 batch.size를 조정하면
 *   여러 메시지를 하나의 배치로 묶어 네트워크 호출을 줄일 수 있다.
 *
 * yml 대응:
 *   spring.kafka.producer.properties.linger.ms=5
 *   spring.kafka.producer.properties.batch.size=16384
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class ProducerBatchingTest extends KafkaTestBase {

    /**
     * linger_ms가_0이면_메시지를_즉시_전송한다.
     *
     * linger.ms=0 (기본값)이면 send()할 때마다 즉시 브로커로 전송한다.
     * 배치 효과가 없어 네트워크 요청이 많아진다.
     *
     * yml 대응: spring.kafka.producer.properties.linger.ms=0 (기본값)
     */
    @Test
    void linger_ms가_0이면_메시지를_즉시_전송한다() throws Exception {
        String testTopic = topic();
        createTopic(testTopic, 1);

        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of(
                ProducerConfig.LINGER_MS_CONFIG, 0,
                ProducerConfig.BATCH_SIZE_CONFIG, 16384
        )))) {
            // 동기 전송: 건건이 즉시 전송됨
            long start = System.currentTimeMillis();
            for (int i = 0; i < 20; i++) {
                sendSync(producer, testTopic, null, "msg-" + i);
            }
            long elapsed = System.currentTimeMillis() - start;

            System.out.printf("  linger.ms=0: 20건 동기 전송 → %dms%n", elapsed);
            System.out.println("  → 건건이 즉시 전송, 배치 효과 없음");
        }
    }

    /**
     * linger_ms를_설정하면_배치로_묶어_전송한다.
     *
     * linger.ms > 0이면 지정 시간만큼 대기하며 메시지를 모아서 한 배치로 전송한다.
     * 처리량(throughput)이 증가하지만 지연(latency)이 linger.ms만큼 늘어난다.
     *
     * yml 대응: spring.kafka.producer.properties.linger.ms=50
     */
    @Test
    void linger_ms를_설정하면_배치로_묶어_전송한다() throws Exception {
        String testTopic = topic();
        createTopic(testTopic, 1);

        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of(
                ProducerConfig.LINGER_MS_CONFIG, 50,
                ProducerConfig.BATCH_SIZE_CONFIG, 16384
        )))) {
            // 비동기 전송: linger.ms 동안 모아서 배치 전송
            List<Future<RecordMetadata>> futures = new ArrayList<>();
            long start = System.currentTimeMillis();
            for (int i = 0; i < 20; i++) {
                futures.add(producer.send(new ProducerRecord<>(testTopic, null, "msg-" + i)));
            }

            // 모든 전송 완료 대기
            for (var future : futures) {
                future.get();
            }
            long elapsed = System.currentTimeMillis() - start;

            System.out.printf("  linger.ms=50: 20건 비동기 전송 → %dms%n", elapsed);
            System.out.println("  → 메시지가 배치로 묶여 네트워크 호출 감소");
            System.out.println("  → 처리량 ↑, 지연 ↑ (최대 linger.ms만큼)");
        }
    }

    /**
     * flush를_호출하면_linger_ms를_무시하고_즉시_전송한다.
     *
     * linger.ms가 크더라도 flush()를 호출하면 버퍼에 쌓인 메시지를
     * 즉시 전송하고 완료될 때까지 블로킹한다.
     * Spring Kafka의 KafkaTemplate.flush()와 동일한 동작이다.
     *
     * yml 대응: KafkaTemplate에서 autoFlush=true 설정
     */
    @Test
    void flush를_호출하면_linger_ms를_무시하고_즉시_전송한다() throws Exception {
        String testTopic = topic();
        createTopic(testTopic, 1);

        // 큰 linger.ms(5초)여도 flush()하면 즉시 전송
        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of(
                ProducerConfig.LINGER_MS_CONFIG, 5000,      // 5초 대기 설정
                ProducerConfig.BATCH_SIZE_CONFIG, 1048576    // 1MB → 배치 안 참
        )))) {
            // 워밍업
            sendSync(producer, testTopic, null, "warmup");

            // send()만 호출 (비동기, 버퍼에만 쌓임)
            List<Future<RecordMetadata>> futures = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                futures.add(producer.send(new ProducerRecord<>(testTopic, null, "msg-" + i)));
            }

            // flush() → linger.ms 무시하고 즉시 전송
            long start = System.currentTimeMillis();
            producer.flush();
            long elapsed = System.currentTimeMillis() - start;

            // 모든 future가 완료됨
            for (var future : futures) {
                assertThat(future.isDone()).isTrue();
            }

            System.out.printf("  linger.ms=5000이지만 flush() → %dms에 전송 완료%n", elapsed);
            assertThat(elapsed).isLessThan(3000);

            System.out.println("  → flush(): 버퍼의 모든 메시지를 즉시 전송 + 완료 대기");
            System.out.println("  → linger.ms/batch.size 무시하고 강제 전송");
            System.out.println("  → 처리량보다 지연이 중요한 경우에 사용");
        }
    }
}
