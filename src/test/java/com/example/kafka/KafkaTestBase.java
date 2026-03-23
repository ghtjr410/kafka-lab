package com.example.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.Future;

/**
 * 모든 학습 테스트의 베이스 클래스.
 *
 * - 테스트마다 고유한 토픽명과 Consumer Group ID를 자동 생성하여 테스트 간 간섭을 방지한다.
 * - yml 기반 테스트와 프로그래밍 방식 오버라이드 테스트 모두를 지원한다.
 */
@SpringBootTest
public abstract class KafkaTestBase {

    protected static final String BOOTSTRAP_SERVERS = "localhost:9092";

    private String uniqueId;

    @BeforeEach
    void setUp(TestInfo testInfo) {
        uniqueId = UUID.randomUUID().toString().substring(0, 8);
        String displayName = testInfo.getDisplayName().replace("_", " ");
        System.out.printf("%n═══ %s ═══%n", displayName);
    }

    /** 테스트 전용 고유 토픽명 생성 */
    protected String topic() {
        return "test-topic-" + uniqueId;
    }

    /** 테스트 전용 고유 Consumer Group ID 생성 */
    protected String groupId() {
        return "test-group-" + uniqueId;
    }

    /** 지정 파티션 수로 토픽을 명시적 생성 */
    protected void createTopic(String topicName, int partitions) {
        try (AdminClient admin = AdminClient.create(Map.of(
                "bootstrap.servers", BOOTSTRAP_SERVERS))) {
            admin.createTopics(List.of(new NewTopic(topicName, partitions, (short) 1)))
                    .all().get();
        } catch (Exception e) {
            throw new RuntimeException("토픽 생성 실패: " + topicName, e);
        }
    }

    /** 기본 Producer 속성. 테스트에서 오버라이드할 속성을 추가로 넣을 수 있다. */
    protected Map<String, Object> producerProps(Map<String, Object> overrides) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.putAll(overrides);
        return props;
    }

    /** 기본 Consumer 속성. 테스트에서 오버라이드할 속성을 추가로 넣을 수 있다. */
    protected Map<String, Object> consumerProps(String groupId, Map<String, Object> overrides) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.putAll(overrides);
        return props;
    }

    /** 메시지를 발행하고 메타데이터를 반환한다 (동기). */
    protected RecordMetadata sendSync(KafkaProducer<String, String> producer,
                                       String topic, String key, String value) {
        try {
            Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, key, value));
            return future.get();
        } catch (Exception e) {
            throw new RuntimeException("메시지 발행 실패", e);
        }
    }

    /** Consumer로 메시지를 폴링한다. timeout 내에 도착한 레코드를 모두 반환한다. */
    protected List<String> pollValues(KafkaConsumer<String, String> consumer,
                                       int expectedCount, Duration timeout) {
        List<String> values = new ArrayList<>();
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (values.size() < expectedCount && System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            records.forEach(r -> values.add(r.value()));
        }
        return values;
    }
}
