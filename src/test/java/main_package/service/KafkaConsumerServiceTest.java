package main_package.service;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.ChannelOutboundBuffer;
import main_package.config.CassandraConfig;
import main_package.model.UserAction;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import org.springframework.boot.testcontainers.service.connection.ServiceConnection;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;


@SpringBootTest(
    classes = {
        KafkaConsumerService.class,
        UserActionService.class,
        CassandraConfig.class
    },
    properties = {
        "topic-to-consume-message=my-topic",
        "spring.kafka.consumer.group-id=my-topic-group"
    }
)
@EmbeddedKafka(topics = "${topic-to-consume-message}", bootstrapServersProperty = "spring.kafka.bootstrap-servers")
@TestPropertySource(properties = {
    "topic-to-consume-message=my-topic",
    "spring.kafka.consumer.group-id=my-topic-group",
    "spring.kafka.bootstrap-servers=localhost:9092"
})
@Import({KafkaAutoConfiguration.class, KafkaConsumerServiceTest.ObjectMapperTestConfig.class})
@Testcontainers
class KafkaConsumerServiceTest {
  @MockBean
  public UserActionService userActionService;

  @Container
  private static final CassandraContainer<?> cassandraContainer =
      new CassandraContainer<>("cassandra:5.0.4")
          .withExposedPorts(9042)
          .withStartupTimeout(Duration.ofMinutes(5))
          .withEnv("HEAP_NEWSIZE", "128M")
          .withEnv("MAX_HEAP_SIZE", "512M")
          .withEnv("JVM_EXTRA_OPTS",
              "-Dcassandra.skip_wait_for_gossip_to_settle=0 " +
                  "-Dcassandra.initial_token=0")
          .withInitScript("init.cql")
          .waitingFor(new LogMessageWaitStrategy()
              .withRegEx(".*Startup complete.*\\s")
              .withTimes(1)
              .withStartupTimeout(Duration.ofMinutes(3)));


  @DynamicPropertySource
  static void cassandraProperties(DynamicPropertyRegistry registry) {
    registry.add("spring.data.cassandra.contact-points",
        () -> cassandraContainer.getHost() + ":" + cassandraContainer.getMappedPort(9042));
//    registry.add("spring.data.cassandra.port", () -> cassandraContainer.getMappedPort(9042));
//    registry.add("spring.cassandra.contact-points",
//        () -> cassandraContainer.getHost() + ":" + cassandraContainer.getMappedPort(9042));
    registry.add("spring.data.cassandra.local-datacenter", () -> "datacenter1");
    registry.add("spring.data.cassandra.keyspace-name", () -> "my_keyspace");
    registry.add("spring.data.cassandra.schema-action",
        () -> "CREATE_IF_NOT_EXISTS");
  }

  @BeforeAll
  static void checkContainer() {
    if (!cassandraContainer.isRunning()) {
      cassandraContainer.start();
    }
    System.out.println("Cassandra port: " + cassandraContainer.getMappedPort(9042));
  }

  @BeforeAll
  static void setup() {
    // Инициализация keyspace и таблицы
    try (CqlSession session = CqlSession.builder()
        .addContactPoint(cassandraContainer.getContactPoint())
        .withLocalDatacenter("datacenter1")
        .build()) {

      session.execute("CREATE KEYSPACE IF NOT EXISTS my_keyspace WITH "
          + "replication = {'class':'SimpleStrategy', 'replication_factor':1};");

      session.execute("CREATE TABLE IF NOT EXISTS my_keyspace.user_actions ("
          + "id UUID PRIMARY KEY, "
          + "timestamp TIMESTAMP, "
          + "action_type TEXT);");
    }
  }

  @TestConfiguration
  static class ObjectMapperTestConfig {
    @Bean
    public ObjectMapper objectMapper() {
      return new ObjectMapper();
    }
  }

  @Container
  @ServiceConnection
  public static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.0.1"));

  @Autowired
  private KafkaTemplate<String, String> kafkaTemplate;

  @Autowired
  private KafkaConsumerService kafkaConsumerService;

  @Autowired
  private ObjectMapper objectMapper;

  @MockBean
  private ChannelOutboundBuffer.MessageProcessor messageProcessor;

  @DynamicPropertySource
  static void overrideProperties(DynamicPropertyRegistry registry) {
    registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
  }

  @BeforeEach
  void setUp() {
    await().atMost(Duration.ofSeconds(2))
        .until(() -> kafka.isRunning() && cassandraContainer.isRunning());
  }

  @Test
  public void consumeMessage() throws JsonProcessingException {
    String validMessage = "{\"id\":\"11111111-1111-1111-1111-111111111111\",\"eventTime\":\"2025-04-06T12:00:00Z\",\"eventType\":\"CREATE\",\"eventDetails\":\"Created user\"}";
    kafkaConsumerService.consumeMessage(validMessage);
  }

  @Test
  public void consumeInvalidMessage() {
    String invalidMessage = null;
    assertThrows(Exception.class, () -> {
      kafkaConsumerService.consumeMessage(invalidMessage);
    });
  }

  @Test
  void shouldSendMessageToKafkaSuccessfully() {
    kafkaTemplate.send("my-topic", String.valueOf(new UserAction(UUID.randomUUID(), Instant.now(), "CREATE")));

    await().atMost(Duration.ofSeconds(5))
        .pollDelay(Duration.ofSeconds(1))
        .untilAsserted(() -> Mockito.verify(
                messageProcessor, times(1))
            .processMessage(eq(String.valueOf(new UserAction(UUID.randomUUID(), Instant.now(), "CREATE"))))
        );
  }
}