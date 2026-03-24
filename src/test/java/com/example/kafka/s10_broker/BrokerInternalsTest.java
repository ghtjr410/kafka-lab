package com.example.kafka.s10_broker;

import com.example.kafka.KafkaTestBase;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Kafka 브로커 내부 동작과 KRaft 메타데이터를 AdminClient로 증명한다.
 *
 * 사고 시나리오:
 *   토픽의 retention.ms를 잘못 설정하여 데이터가 조기 삭제되었다.
 *   AdminClient로 토픽 설정을 조회/변경하는 방법을 알아야 한다.
 *
 * 운영 도구 대응:
 *   kafka-configs.sh --describe --topic {topic}
 *   kafka-configs.sh --alter --topic {topic} --add-config retention.ms=86400000
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class BrokerInternalsTest extends KafkaTestBase {

    /**
     * AdminClient로_토픽_설정을_조회할_수_있다.
     *
     * ConfigResource로 토픽의 설정값(retention.ms, cleanup.policy 등)을 조회한다.
     * 잘못된 설정을 사전에 발견할 수 있다.
     *
     * 운영 도구: kafka-configs.sh --describe --entity-type topics --entity-name {topic}
     */
    @Test
    void AdminClient로_토픽_설정을_조회할_수_있다() throws Exception {
        String testTopic = topic();
        createTopic(testTopic, 1);

        try (AdminClient admin = AdminClient.create(Map.of(
                "bootstrap.servers", BOOTSTRAP_SERVERS))) {

            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, testTopic);
            Config config = admin.describeConfigs(List.of(resource))
                    .all().get().get(resource);

            System.out.printf("  Topic: %s%n", testTopic);
            System.out.println("  === 주요 설정값 ===");

            // 주요 설정 출력
            String[] importantConfigs = {
                    TopicConfig.RETENTION_MS_CONFIG,
                    TopicConfig.CLEANUP_POLICY_CONFIG,
                    TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG,
                    TopicConfig.SEGMENT_BYTES_CONFIG,
                    TopicConfig.MAX_MESSAGE_BYTES_CONFIG
            };

            for (String configName : importantConfigs) {
                ConfigEntry entry = config.get(configName);
                if (entry != null) {
                    System.out.printf("  %s = %s (source: %s)%n",
                            entry.name(), entry.value(), entry.source());
                }
            }

            // retention.ms와 cleanup.policy가 존재하는지 확인
            assertThat(config.get(TopicConfig.RETENTION_MS_CONFIG)).isNotNull();
            assertThat(config.get(TopicConfig.CLEANUP_POLICY_CONFIG)).isNotNull();
            assertThat(config.get(TopicConfig.CLEANUP_POLICY_CONFIG).value()).isEqualTo("delete");

            System.out.println();
            System.out.println("  → describeConfigs()로 토픽 설정 프로그래밍 방식 조회");
            System.out.println("  → source: DEFAULT_CONFIG = 브로커 기본값, DYNAMIC_TOPIC_CONFIG = 명시적 설정");
        }
    }

    /**
     * AdminClient로_토픽_설정을_동적으로_변경할_수_있다.
     *
     * retention.ms를 변경하여 메시지 보존 기간을 조정할 수 있다.
     * 브로커 재시작 없이 동적으로 반영된다.
     *
     * 운영 도구: kafka-configs.sh --alter --topic {topic} --add-config retention.ms=3600000
     */
    @Test
    void AdminClient로_토픽_설정을_동적으로_변경할_수_있다() throws Exception {
        String testTopic = topic();
        createTopic(testTopic, 1);

        try (AdminClient admin = AdminClient.create(Map.of(
                "bootstrap.servers", BOOTSTRAP_SERVERS))) {

            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, testTopic);

            // 변경 전: 기본 retention 확인
            Config beforeConfig = admin.describeConfigs(List.of(resource))
                    .all().get().get(resource);
            String beforeRetention = beforeConfig.get(TopicConfig.RETENTION_MS_CONFIG).value();
            System.out.printf("  변경 전 retention.ms: %s%n", beforeRetention);

            // retention.ms를 1시간(3600000ms)으로 변경
            ConfigEntry newRetention = new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "3600000");
            AlterConfigOp op = new AlterConfigOp(newRetention, AlterConfigOp.OpType.SET);
            admin.incrementalAlterConfigs(Map.of(resource, List.of(op))).all().get();

            // 변경 후 확인
            Config afterConfig = admin.describeConfigs(List.of(resource))
                    .all().get().get(resource);
            String afterRetention = afterConfig.get(TopicConfig.RETENTION_MS_CONFIG).value();
            String afterSource = afterConfig.get(TopicConfig.RETENTION_MS_CONFIG).source().name();

            System.out.printf("  변경 후 retention.ms: %s (source: %s)%n", afterRetention, afterSource);

            assertThat(afterRetention).isEqualTo("3600000");

            System.out.println();
            System.out.println("  → incrementalAlterConfigs()로 브로커 재시작 없이 설정 변경");
            System.out.println("  → 운영 중 retention 조정, compression 변경 등에 활용");
        }
    }

    /**
     * KRaft_모드에서_컨트롤러_노드를_확인할_수_있다.
     *
     * KRaft(Kafka Raft) 모드에서는 ZooKeeper 없이 Kafka 자체가 메타데이터를 관리한다.
     * 컨트롤러 노드가 리더 선출, 토픽 관리 등을 담당한다.
     *
     * 운영 도구: kafka-metadata.sh --snapshot /path/to/metadata
     */
    @Test
    void KRaft_모드에서_컨트롤러_노드를_확인할_수_있다() throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of(
                "bootstrap.servers", BOOTSTRAP_SERVERS))) {

            DescribeClusterResult cluster = admin.describeCluster();

            String clusterId = cluster.clusterId().get();
            Node controller = cluster.controller().get();
            Collection<Node> nodes = cluster.nodes().get();

            System.out.printf("  Cluster ID: %s%n", clusterId);
            System.out.printf("  Controller Node: %d (%s:%d)%n",
                    controller.id(), controller.host(), controller.port());
            System.out.printf("  Total Nodes: %d%n", nodes.size());

            // KRaft 모드 확인: docker-compose에서 KAFKA_PROCESS_ROLES=broker,controller
            assertThat(controller.id()).isEqualTo(1);
            assertThat(clusterId).contains("kafka-lab-cluster-id");

            System.out.println();
            System.out.println("  === KRaft vs ZooKeeper ===");
            System.out.println("  KRaft: KAFKA_PROCESS_ROLES=broker,controller (단일 프로세스)");
            System.out.println("  KRaft: KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093");
            System.out.println("  KRaft: ZooKeeper 없이 메타데이터를 Raft 프로토콜로 관리");
            System.out.println("  → ZooKeeper 의존성 제거 → 운영 복잡도 감소");
        }
    }

    /**
     * 토픽의_Earliest_Offset과_Latest_Offset으로_데이터_범위를_확인한다.
     *
     * Earliest Offset: 보존 정책에 따라 삭제되지 않은 가장 오래된 메시지의 offset
     * Latest Offset (LEO): 다음에 쓰일 offset (= 현재 저장된 메시지 수)
     * 이 두 값의 차이가 현재 토픽에 남아있는 메시지 수다.
     *
     * 운영 도구: kafka-run-class.sh kafka.tools.GetOffsetShell
     */
    @Test
    void 토픽의_Earliest와_Latest_Offset으로_데이터_범위를_확인한다() throws Exception {
        String testTopic = topic();
        createTopic(testTopic, 1);

        // 메시지 발행
        try (var producer = new KafkaProducer<String, String>(producerProps(Map.of()))) {
            for (int i = 0; i < 20; i++) {
                sendSync(producer, testTopic, null, "msg-" + i);
            }
        }

        try (AdminClient admin = AdminClient.create(Map.of(
                "bootstrap.servers", BOOTSTRAP_SERVERS))) {

            TopicPartition tp = new TopicPartition(testTopic, 0);

            // Earliest Offset
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> earliest =
                    admin.listOffsets(Map.of(tp, OffsetSpec.earliest())).all().get();
            long earliestOffset = earliest.get(tp).offset();

            // Latest Offset (LEO)
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latest =
                    admin.listOffsets(Map.of(tp, OffsetSpec.latest())).all().get();
            long latestOffset = latest.get(tp).offset();

            long availableMessages = latestOffset - earliestOffset;

            System.out.printf("  Earliest Offset: %d%n", earliestOffset);
            System.out.printf("  Latest Offset (LEO): %d%n", latestOffset);
            System.out.printf("  Available Messages: %d (= %d - %d)%n",
                    availableMessages, latestOffset, earliestOffset);

            assertThat(earliestOffset).isEqualTo(0);
            assertThat(latestOffset).isEqualTo(20);
            assertThat(availableMessages).isEqualTo(20);

            System.out.println();
            System.out.println("  → Earliest: retention으로 삭제된 후 남은 시작점");
            System.out.println("  → Latest (LEO): 다음에 쓰일 위치");
            System.out.println("  → 차이 = 현재 토픽에 남아있는 읽을 수 있는 메시지 수");
        }
    }

    /**
     * AdminClient로_토픽_목록을_조회하고_관리할_수_있다.
     *
     * listTopics()로 브로커의 전체 토픽 목록을 조회하고,
     * deleteTopics()로 불필요한 토픽을 삭제할 수 있다.
     *
     * 운영 도구: kafka-topics.sh --list, kafka-topics.sh --delete --topic {topic}
     */
    @Test
    void AdminClient로_토픽_목록을_조회하고_삭제할_수_있다() throws Exception {
        String testTopic = topic();
        createTopic(testTopic, 2);

        try (AdminClient admin = AdminClient.create(Map.of(
                "bootstrap.servers", BOOTSTRAP_SERVERS))) {

            // 토픽 목록 조회
            var topicNames = admin.listTopics().names().get();
            System.out.printf("  전체 토픽 수: %d%n", topicNames.size());
            System.out.printf("  테스트 토픽 존재: %s%n", topicNames.contains(testTopic));
            assertThat(topicNames).contains(testTopic);

            // 토픽 삭제
            admin.deleteTopics(List.of(testTopic)).all().get();
            System.out.printf("  토픽 삭제: %s%n", testTopic);

            // 삭제 확인
            var afterDelete = admin.listTopics().names().get();
            assertThat(afterDelete).doesNotContain(testTopic);

            System.out.println();
            System.out.println("  → listTopics(): 클러스터의 전체 토픽 조회");
            System.out.println("  → deleteTopics(): 불필요한 토픽 정리 (auto.create 주의!)");
            System.out.println("  → 운영: 테스트 토픽, 만료된 토픽 정리 자동화에 활용");
        }
    }
}
