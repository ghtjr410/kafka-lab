package com.example.kafka.connect;

import com.example.kafka.KafkaTestBase;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Kafka Connect REST API를 통한 Source/Sink Connector 동작을 증명한다.
 *
 * 사고 시나리오:
 *   DB 변경사항을 Kafka로 보내야 하는데 직접 Producer를 짜면
 *   장애 시 offset 관리, 재시작 등을 모두 직접 구현해야 한다.
 *   Kafka Connect를 사용하면 설정만으로 안정적인 데이터 파이프라인을 만들 수 있다.
 *
 * 이 테스트는 FileStreamSourceConnector/SinkConnector를 사용한다.
 * 실무에서는 Debezium(DB CDC), S3 Sink, JDBC Source 등을 사용한다.
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class KafkaConnectTest extends KafkaTestBase {

    private static final String CONNECT_URL = "http://localhost:8083";
    private static final HttpClient httpClient = HttpClient.newHttpClient();
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Connect_REST_API로_클러스터_정보를_조회할_수_있다.
     *
     * Kafka Connect는 REST API를 통해 관리된다.
     * GET / 로 Connect 워커의 버전과 연결된 Kafka 클러스터를 확인할 수 있다.
     */
    @Test
    void Connect_REST_API로_클러스터_정보를_조회할_수_있다() throws Exception {
        HttpResponse<String> response = httpClient.send(
                HttpRequest.newBuilder().uri(URI.create(CONNECT_URL + "/")).GET().build(),
                HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(200);

        JsonNode body = objectMapper.readTree(response.body());
        String version = body.get("version").asText();
        String clusterId = body.get("kafka_cluster_id").asText();

        System.out.printf("  Connect Version: %s%n", version);
        System.out.printf("  Kafka Cluster: %s%n", clusterId);

        assertThat(version).isNotEmpty();
        assertThat(clusterId).contains("kafka-lab");

        // 사용 가능한 플러그인 조회
        HttpResponse<String> pluginsResponse = httpClient.send(
                HttpRequest.newBuilder().uri(URI.create(CONNECT_URL + "/connector-plugins")).GET().build(),
                HttpResponse.BodyHandlers.ofString());

        JsonNode plugins = objectMapper.readTree(pluginsResponse.body());
        System.out.printf("  Available Plugins: %d개%n", plugins.size());
        for (JsonNode plugin : plugins) {
            System.out.printf("    - %s (%s)%n",
                    plugin.get("class").asText().replaceAll(".*\\.", ""),
                    plugin.get("type").asText());
        }

        System.out.println();
        System.out.println("  → Connect REST API: GET /, GET /connector-plugins");
        System.out.println("  → 실무: Debezium, S3 Sink, JDBC Source 등 플러그인 추가");
    }

    /**
     * Source_Connector로_파일에서_Kafka_토픽으로_데이터를_보낼_수_있다.
     *
     * FileStreamSourceConnector는 파일을 읽어 Kafka 토픽으로 발행한다.
     * Connect가 offset을 관리하므로 재시작 시 이어서 읽는다.
     *
     * 실무 대응: Debezium CDC Source → DB 변경사항을 Kafka로
     */
    @Test
    void Source_Connector로_파일에서_Kafka_토픽으로_데이터를_보낼_수_있다() throws Exception {
        String connectorName = "file-source-" + topic().replace("test-topic-", "");
        String topicName = "connect-source-test-" + connectorName;

        try {
            // 1) 소스 파일 생성 (Connect 컨테이너 내부)
            dockerExec("sh -c 'echo \"line-1\" > /tmp/connect-data/source-test.txt'");
            dockerExec("sh -c 'echo \"line-2\" >> /tmp/connect-data/source-test.txt'");
            dockerExec("sh -c 'echo \"line-3\" >> /tmp/connect-data/source-test.txt'");
            System.out.println("  [1] 소스 파일 생성: 3줄");

            // 2) Source Connector 생성
            String config = """
                    {
                      "name": "%s",
                      "config": {
                        "connector.class": "org.apache.kafka.connect.file.FileStreamSourceConnector",
                        "tasks.max": "1",
                        "file": "/tmp/connect-data/source-test.txt",
                        "topic": "%s"
                      }
                    }
                    """.formatted(connectorName, topicName);

            HttpResponse<String> createResponse = httpClient.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create(CONNECT_URL + "/connectors"))
                            .header("Content-Type", "application/json")
                            .POST(HttpRequest.BodyPublishers.ofString(config))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());

            assertThat(createResponse.statusCode()).isIn(201, 409);
            System.out.printf("  [2] Source Connector 생성: %s (status=%d)%n",
                    connectorName, createResponse.statusCode());

            // 3) 데이터가 토픽에 도착할 때까지 대기
            Thread.sleep(5000);

            // 4) Consumer로 토픽에서 메시지 확인
            try (var consumer = new KafkaConsumer<String, String>(
                    consumerProps(groupId(), Map.of(
                            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false
                    )))) {
                consumer.subscribe(List.of(topicName));
                List<String> values = pollValues(consumer, 3, Duration.ofSeconds(15));

                System.out.printf("  [3] 토픽에서 수신: %d건 → %s%n", values.size(), values);
                assertThat(values).containsExactly("line-1", "line-2", "line-3");
            }

            // 5) Connector 상태 확인
            HttpResponse<String> statusResponse = httpClient.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create(CONNECT_URL + "/connectors/" + connectorName + "/status"))
                            .GET().build(),
                    HttpResponse.BodyHandlers.ofString());

            JsonNode status = objectMapper.readTree(statusResponse.body());
            String connectorState = status.get("connector").get("state").asText();
            System.out.printf("  [4] Connector 상태: %s%n", connectorState);
            assertThat(connectorState).isEqualTo("RUNNING");

            System.out.println();
            System.out.println("  → Source Connector: 외부 시스템 → Kafka 토픽");
            System.out.println("  → 실무: Debezium(DB CDC), JDBC Source, MQ Source");
        } finally {
            deleteConnector(connectorName);
        }
    }

    /**
     * Sink_Connector로_Kafka_토픽에서_파일로_데이터를_내보낼_수_있다.
     *
     * FileStreamSinkConnector는 Kafka 토픽의 메시지를 파일에 기록한다.
     * Consumer offset은 Connect가 자동 관리한다.
     *
     * 실무 대응: S3 Sink, Elasticsearch Sink, JDBC Sink
     */
    @Test
    void Sink_Connector로_Kafka_토픽에서_파일로_데이터를_내보낼_수_있다() throws Exception {
        String connectorName = "file-sink-" + topic().replace("test-topic-", "");
        String topicName = "connect-sink-test-" + connectorName;
        String sinkFile = "/tmp/connect-data/sink-output-" + connectorName + ".txt";

        try {
            createTopic(topicName, 1);

            // 1) 토픽에 메시지 발행
            try (var producer = new KafkaProducer<String, String>(producerProps(Map.of()))) {
                sendSync(producer, topicName, null, "event-A");
                sendSync(producer, topicName, null, "event-B");
                sendSync(producer, topicName, null, "event-C");
            }
            System.out.println("  [1] 토픽에 3건 발행");

            // 2) Sink Connector 생성
            String config = """
                    {
                      "name": "%s",
                      "config": {
                        "connector.class": "org.apache.kafka.connect.file.FileStreamSinkConnector",
                        "tasks.max": "1",
                        "topics": "%s",
                        "file": "%s"
                      }
                    }
                    """.formatted(connectorName, topicName, sinkFile);

            HttpResponse<String> createResponse = httpClient.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create(CONNECT_URL + "/connectors"))
                            .header("Content-Type", "application/json")
                            .POST(HttpRequest.BodyPublishers.ofString(config))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());

            assertThat(createResponse.statusCode()).isIn(201, 409);
            System.out.printf("  [2] Sink Connector 생성: %s%n", connectorName);

            // 3) 파일에 기록될 때까지 대기
            Thread.sleep(10000);

            // 4) 출력 파일 확인
            String fileContent = dockerExec("cat " + sinkFile).trim();
            System.out.printf("  [3] 출력 파일 내용:%n");
            for (String line : fileContent.split("\n")) {
                System.out.printf("    %s%n", line);
            }

            assertThat(fileContent).contains("event-A");
            assertThat(fileContent).contains("event-B");
            assertThat(fileContent).contains("event-C");

            System.out.println();
            System.out.println("  → Sink Connector: Kafka 토픽 → 외부 시스템");
            System.out.println("  → 실무: S3 Sink, Elasticsearch Sink, JDBC Sink");
        } finally {
            deleteConnector(connectorName);
        }
    }

    /**
     * Connector를_일시정지하고_재개할_수_있다.
     *
     * PUT /connectors/{name}/pause로 일시정지,
     * PUT /connectors/{name}/resume으로 재개할 수 있다.
     * 운영 중 유지보수 시 Connector를 안전하게 중단할 수 있다.
     */
    @Test
    void Connector를_일시정지하고_재개할_수_있다() throws Exception {
        String connectorName = "pause-test-" + topic().replace("test-topic-", "");
        String topicName = "connect-pause-test-" + connectorName;

        try {
            createTopic(topicName, 1);

            // Connector 생성
            String config = """
                    {
                      "name": "%s",
                      "config": {
                        "connector.class": "org.apache.kafka.connect.file.FileStreamSinkConnector",
                        "tasks.max": "1",
                        "topics": "%s",
                        "file": "/tmp/connect-data/pause-test.txt"
                      }
                    }
                    """.formatted(connectorName, topicName);

            httpClient.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create(CONNECT_URL + "/connectors"))
                            .header("Content-Type", "application/json")
                            .POST(HttpRequest.BodyPublishers.ofString(config))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());

            Thread.sleep(3000);

            // RUNNING 상태 확인
            String state1 = getConnectorState(connectorName);
            System.out.printf("  [1] 초기 상태: %s%n", state1);
            assertThat(state1).isEqualTo("RUNNING");

            // PAUSE
            httpClient.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create(CONNECT_URL + "/connectors/" + connectorName + "/pause"))
                            .PUT(HttpRequest.BodyPublishers.noBody())
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            Thread.sleep(2000);

            String state2 = getConnectorState(connectorName);
            System.out.printf("  [2] 일시정지 후: %s%n", state2);
            assertThat(state2).isEqualTo("PAUSED");

            // RESUME
            httpClient.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create(CONNECT_URL + "/connectors/" + connectorName + "/resume"))
                            .PUT(HttpRequest.BodyPublishers.noBody())
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            Thread.sleep(2000);

            String state3 = getConnectorState(connectorName);
            System.out.printf("  [3] 재개 후: %s%n", state3);
            assertThat(state3).isEqualTo("RUNNING");

            System.out.println();
            System.out.println("  → PUT /connectors/{name}/pause: 일시정지");
            System.out.println("  → PUT /connectors/{name}/resume: 재개");
            System.out.println("  → 운영: 유지보수 시 안전한 중단/재개");
        } finally {
            deleteConnector(connectorName);
        }
    }

    // === 유틸 메서드 ===

    private String getConnectorState(String name) throws Exception {
        HttpResponse<String> response = httpClient.send(
                HttpRequest.newBuilder()
                        .uri(URI.create(CONNECT_URL + "/connectors/" + name + "/status"))
                        .GET().build(),
                HttpResponse.BodyHandlers.ofString());
        return objectMapper.readTree(response.body()).get("connector").get("state").asText();
    }

    private void deleteConnector(String name) {
        try {
            httpClient.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create(CONNECT_URL + "/connectors/" + name))
                            .DELETE().build(),
                    HttpResponse.BodyHandlers.ofString());
        } catch (Exception ignored) {}
    }

    private String dockerExec(String command) {
        try {
            Process process = new ProcessBuilder("docker", "exec",
                    "kafka-lab-kafka-connect-1", "sh", "-c", command)
                    .redirectErrorStream(true)
                    .start();
            String output = new String(process.getInputStream().readAllBytes());
            process.waitFor();
            return output;
        } catch (Exception e) {
            throw new RuntimeException("docker exec 실패: " + command, e);
        }
    }
}
