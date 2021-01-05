package hu.szbuli.smarthome.lora;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class LoraServer {
  
  private static final Logger logger = LoggerFactory.getLogger(LoraServer.class);

  private ObjectMapper objectMapper;

  public LoraServer() {
    this.objectMapper = new ObjectMapper();
  }

  public void processMessage(String line) {
    try {
      objectMapper.readValue(line, LoraMessage.class);
    } catch (JsonProcessingException e) {
      logger.error("lora server, invalid message", e);
    }
  }
}
