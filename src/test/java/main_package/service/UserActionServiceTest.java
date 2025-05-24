package main_package.service;

import static org.junit.jupiter.api.Assertions.*;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import main_package.Application;
import main_package.model.UserAction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SpringBootTest(classes = Application.class)
@Testcontainers
class UserActionServiceTest {
  @Autowired
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


  @Test
  public void testCreateUserSuccess() {
    UserAction action = new UserAction(UUID.randomUUID(), Instant.now(), "CREATE");
    boolean response = userActionService.insertAction(action);

    assert(response);
  }

  @Test
  public void testCreateUserFail() {
    UserAction invalidAction = null;
    assertThrows(RuntimeException.class, () -> userActionService.insertAction(invalidAction));
  }

  @Test
  public void testGetUserByIdSuccess() {
    UserAction action = new UserAction(UUID.randomUUID(), Instant.now(), "CREATE");
    userActionService.insertAction(action);
    List<Row> userAudit = userActionService.getActionById(action.getId());

    assertEquals(1, userAudit.size());
  }

  @Test
  public void testGetUserByIdFail() {
    List<Row> userAudit = userActionService.getActionById(UUID.randomUUID());
    assertEquals(0, userAudit.size());
  }
}