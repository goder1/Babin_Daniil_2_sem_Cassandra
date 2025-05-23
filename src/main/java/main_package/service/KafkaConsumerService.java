package main_package.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import main_package.model.UserAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaListener.class);

  @Autowired
  private UserActionService userAuditService;
  @Autowired
  private ObjectMapper objectMapper;

  @KafkaListener(topics = {"${topic-to-consume-message}"}, groupId = "my-topic-group")
  public void consumeMessage(String message) throws JsonProcessingException {
    UserAction parsedMessage = objectMapper.readValue(message, UserAction.class);
    userAuditService.insertAction(
      new UserAction(
        parsedMessage.getId(),
        parsedMessage.getEventTime(),
        parsedMessage.getEventType()
      )
    );
    LOGGER.info("Retrieved message {}", message);
  }
}