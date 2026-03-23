package com.example.kafka.monitoring;

import com.example.kafka.KafkaTestBase;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Kafka 클라이언트 메트릭과 AdminClient 클러스터 정보 조회를 증명한다.
 *
 * 사고 시나리오:
 *   Producer의 send() 지연이 간헐적으로 발생하는데 원인을 모른다.
 *   KafkaProducer.metrics()를 통해 record-send-rate, request-latency 등을
 *   확인하면 병목 지점을 찾을 수 있다.
 *
 * 운영 도구 대응:
 *   JMX: kafka.producer:type=producer-metrics,client-id=*
 *   JMX: kafka.consumer:type=consumer-metrics,client-id=*
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class BrokerMetricsTest extends KafkaTestBase {

    /**
     * AdminClient로_클러스터_정보를_조회할_수_있다.
     *
     * describeCluster()로 브로커 목록, 컨트롤러 노드, 클러스터 ID를 확인한다.
     * 운영 중 브로커 수, 컨트롤러 위치를 프로그래밍 방식으로 모니터링할 수 있다.
     *
     * 운영 도구: kafka-broker-api-versions.sh, kafka-metadata.sh
     */
    @Test
    void AdminClient로_클러스터_정보를_조회할_수_있다() throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of(
                "bootstrap.servers", BOOTSTRAP_SERVERS))) {

            DescribeClusterResult cluster = admin.describeCluster();

            String clusterId = cluster.clusterId().get();
            Node controller = cluster.controller().get();
            var nodes = cluster.nodes().get();

            System.out.printf("  Cluster ID: %s%n", clusterId);
            System.out.printf("  Controller: Node %d (%s:%d)%n",
                    controller.id(), controller.host(), controller.port());
            System.out.printf("  Broker 수: %d%n", nodes.size());
            for (Node node : nodes) {
                System.out.printf("    - Node %d: %s:%d%n", node.id(), node.host(), node.port());
            }

            assertThat(clusterId).isNotEmpty();
            assertThat(controller).isNotNull();
            assertThat(nodes).isNotEmpty();

            System.out.println();
            System.out.println("  → describeCluster(): 클러스터 상태를 프로그래밍 방식으로 확인");
            System.out.println("  → 브로커 수 감소 감지 → 알림 발생 가능");
        }
    }

    /**
     * AdminClient로_토픽의_파티션_리더와_ISR을_확인할_수_있다.
     *
     * describeTopics()로 파티션별 리더 노드와 ISR(In-Sync Replicas) 목록을 확인한다.
     * ISR이 줄어들면 데이터 유실 위험이 있으므로 모니터링이 필수다.
     *
     * 운영 도구: kafka-topics.sh --describe --topic {topic}
     */
    @Test
    void AdminClient로_토픽의_파티션_리더와_ISR을_확인할_수_있다() throws Exception {
        String testTopic = topic();
        createTopic(testTopic, 3);

        // 메시지 발행하여 파티션 활성화
        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of()))) {
            for (int i = 0; i < 6; i++) {
                sendSync(producer, testTopic, "key-" + i, "msg-" + i);
            }
        }

        try (AdminClient admin = AdminClient.create(Map.of(
                "bootstrap.servers", BOOTSTRAP_SERVERS))) {

            TopicDescription desc = admin.describeTopics(List.of(testTopic))
                    .allTopicNames().get().get(testTopic);

            System.out.printf("  Topic: %s (파티션 %d개)%n", desc.name(), desc.partitions().size());
            assertThat(desc.partitions()).hasSize(3);

            for (TopicPartitionInfo partition : desc.partitions()) {
                Node leader = partition.leader();
                List<Node> isr = partition.isr();
                List<Node> replicas = partition.replicas();

                System.out.printf("  P%d: Leader=Node%d, ISR=%s, Replicas=%s%n",
                        partition.partition(),
                        leader.id(),
                        isr.stream().map(n -> "Node" + n.id()).toList(),
                        replicas.stream().map(n -> "Node" + n.id()).toList());

                assertThat(leader).isNotNull();
                // 단일 브로커: ISR = Replicas = [Node1]
                assertThat(isr).isNotEmpty();
            }

            System.out.println();
            System.out.println("  → 단일 브로커: 모든 파티션의 Leader = ISR = 같은 노드");
            System.out.println("  → 멀티 브로커에서 ISR < Replicas면 under-replicated → 알림!");
            System.out.println("  → 모니터링: kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions");
        }
    }

    /**
     * Producer_클라이언트_메트릭으로_발행_성능을_확인할_수_있다.
     *
     * KafkaProducer.metrics()는 record-send-rate, request-latency-avg 등
     * 클라이언트 레벨 메트릭을 제공한다. 이를 통해 Producer 병목을 진단할 수 있다.
     *
     * JMX 대응: kafka.producer:type=producer-metrics,client-id=*
     */
    @Test
    void Producer_클라이언트_메트릭으로_발행_성능을_확인할_수_있다() {
        String testTopic = topic();
        createTopic(testTopic, 1);

        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of(
                ProducerConfig.CLIENT_ID_CONFIG, "metrics-test-producer"
        )))) {
            // 메시지 발행
            for (int i = 0; i < 50; i++) {
                sendSync(producer, testTopic, null, "msg-" + i);
            }

            // Producer 메트릭 조회
            Map<MetricName, ? extends Metric> metrics = producer.metrics();

            System.out.println("  === Producer 주요 메트릭 ===");
            for (var entry : metrics.entrySet()) {
                MetricName name = entry.getKey();
                double value = entry.getValue().metricValue() instanceof Double d ? d : 0.0;

                // 주요 메트릭만 출력
                if (name.group().equals("producer-metrics")) {
                    switch (name.name()) {
                        case "record-send-rate" ->
                                System.out.printf("  record-send-rate: %.2f records/sec%n", value);
                        case "record-send-total" ->
                                System.out.printf("  record-send-total: %.0f records%n", value);
                        case "request-latency-avg" ->
                                System.out.printf("  request-latency-avg: %.2f ms%n", value);
                        case "request-latency-max" ->
                                System.out.printf("  request-latency-max: %.2f ms%n", value);
                        case "batch-size-avg" ->
                                System.out.printf("  batch-size-avg: %.0f bytes%n", value);
                        case "record-error-rate" ->
                                System.out.printf("  record-error-rate: %.2f errors/sec%n", value);
                    }
                }
            }

            // 메트릭이 존재하는지 확인
            boolean hasRecordSendRate = metrics.keySet().stream()
                    .anyMatch(m -> m.name().equals("record-send-rate"));
            assertThat(hasRecordSendRate).isTrue();

            System.out.println();
            System.out.println("  → KafkaProducer.metrics()로 클라이언트 성능 진단");
            System.out.println("  → record-send-rate 감소 → 브로커 병목 or 네트워크 문제");
            System.out.println("  → request-latency 증가 → 브로커 과부하 의심");
        }
    }

    /**
     * Consumer_클라이언트_메트릭으로_소비_성능을_확인할_수_있다.
     *
     * KafkaConsumer.metrics()는 fetch-rate, records-consumed-rate,
     * records-lag 등 Consumer 레벨 메트릭을 제공한다.
     *
     * JMX 대응: kafka.consumer:type=consumer-fetch-manager-metrics,client-id=*
     */
    @Test
    void Consumer_클라이언트_메트릭으로_소비_성능을_확인할_수_있다() {
        String testTopic = topic();
        String testGroupId = groupId();
        createTopic(testTopic, 1);

        // 메시지 발행
        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of()))) {
            for (int i = 0; i < 30; i++) {
                sendSync(producer, testTopic, null, "msg-" + i);
            }
        }

        try (var consumer = new KafkaConsumer<String, String>(
                consumerProps(testGroupId, Map.of(
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                        ConsumerConfig.CLIENT_ID_CONFIG, "metrics-test-consumer"
                )))) {
            consumer.subscribe(List.of(testTopic));

            // 소비
            pollValues(consumer, 30, Duration.ofSeconds(10));

            // Consumer 메트릭 조회
            Map<MetricName, ? extends Metric> metrics = consumer.metrics();

            System.out.println("  === Consumer 주요 메트릭 ===");
            for (var entry : metrics.entrySet()) {
                MetricName name = entry.getKey();
                double value = entry.getValue().metricValue() instanceof Double d ? d : 0.0;

                if (name.group().equals("consumer-fetch-manager-metrics") && name.tags().isEmpty()) {
                    switch (name.name()) {
                        case "fetch-rate" ->
                                System.out.printf("  fetch-rate: %.2f fetches/sec%n", value);
                        case "records-consumed-rate" ->
                                System.out.printf("  records-consumed-rate: %.2f records/sec%n", value);
                        case "bytes-consumed-rate" ->
                                System.out.printf("  bytes-consumed-rate: %.0f bytes/sec%n", value);
                        case "fetch-latency-avg" ->
                                System.out.printf("  fetch-latency-avg: %.2f ms%n", value);
                        case "records-lag-max" ->
                                System.out.printf("  records-lag-max: %.0f records%n", value);
                    }
                }
            }

            boolean hasConsumedRate = metrics.keySet().stream()
                    .anyMatch(m -> m.name().equals("records-consumed-rate"));
            assertThat(hasConsumedRate).isTrue();

            System.out.println();
            System.out.println("  → KafkaConsumer.metrics()로 소비 성능 진단");
            System.out.println("  → records-lag-max 증가 → Consumer 처리 속도 부족");
            System.out.println("  → fetch-latency 증가 → 브로커 응답 지연");
        }
    }
}
