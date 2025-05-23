package main_package.service;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import main_package.model.UserAction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class UserActionService {

  @Autowired
  private CqlSession session;

  public List<Row> getActionById(UUID id) {
    PreparedStatement selectStatement;

    selectStatement = session.prepare(
        "SELECT * FROM my_keyspace.user_action WHERE id = ?"
    );

    BoundStatement boundStatement = selectStatement.bind(id);
    ResultSet resultSet = session.execute(boundStatement);
    List<Row> result = new ArrayList<>();
    for (Row row : resultSet) {
      result.add(row);
    }
    return result;
  }

  public boolean insertAction(UserAction userAction) {
    PreparedStatement insertStatement;

    insertStatement = session.prepare(
        "INSERT INTO my_keyspace.user_action (id, event_time, event_type) " +
            "VALUES (?, ?, ?)"
    );

    BoundStatement boundStatement = insertStatement.bind(
        userAction.getId(),
        userAction.getEventTime(),
        userAction.getEventType()
    );
    session.execute(boundStatement);

    return true;
  }
}