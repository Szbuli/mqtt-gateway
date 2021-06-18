package hu.szbuli.smarthome.rpi.mqtt.proxy;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.text.StringSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;

import hu.szbuli.smarthome.can.CanMessage;
import hu.szbuli.smarthome.gateway.converter.CanMqttPayloadConverter;
import hu.szbuli.smarthome.gateway.converter.CanMqttTopicConverter;
import hu.szbuli.smarthome.gateway.converter.GatewayConverter;
import hu.szbuli.smarthome.gateway.heartbeat.HeartBeatService;
import hu.szbuli.smarthome.gateway.homeassistant.DiscoveryManager;
import hu.szbuli.smarthome.mqtt.MqttListener;
import hu.szbuli.smarthome.mqtt.MqttManager;
import hu.szbuli.smarthome.mqtt.MqttTopic;

public class Gateway {

  private static final Logger logger = LoggerFactory.getLogger(Gateway.class);

  private static final String[] HEADERS = { "topic", "mqtt-topic", "converter" };
  private MqttManager mqttManager;
  private BlockingQueue<CanMessage> sendCanQueue;

  private Map<Integer, ConversionConfig> can2Mqtt = new HashMap<>();
  private Map<String, ConversionConfig> mqtt2Can = new HashMap<>();
  private GatewayConverter gatewayConverter = new CanMqttPayloadConverter();
  private CanMqttTopicConverter canMqttTopicConverter = new CanMqttTopicConverter();
  private HeartBeatService heartBeatService;
  private DiscoveryManager discoveryManager;

  public Gateway(String configFile, MqttManager mqttManager, DiscoveryManager discoveryManager,
      HeartBeatService heartBeatService, BlockingQueue<CanMessage> sendCanQueue) throws IOException {
    this.mqttManager = mqttManager;
    this.sendCanQueue = sendCanQueue;

    this.discoveryManager = discoveryManager;
    this.heartBeatService = heartBeatService;

    Reader in = new FileReader(new File(configFile).getAbsolutePath());
    Iterable<CSVRecord> records = CSVFormat.DEFAULT.withHeader(HEADERS).withFirstRecordAsHeader().parse(in);

    for (CSVRecord record : records) {
      Integer canId = Integer.parseInt(record.get("can-id"));
      String mqttTopic = record.get("mqtt-topic");
      String converter = record.get("converter");
      ConversionConfig conversionConfig = new ConversionConfig();
      conversionConfig.setCanTopic(canId);
      conversionConfig.setMqttTopic(mqttTopic);
      conversionConfig.setConverter(converter);

      can2Mqtt.put(canId, conversionConfig);
      mqtt2Can.put(mqttTopic, conversionConfig);
    }

    mqtt2Can.entrySet().stream()
        .filter(config -> {
          return !config.getValue().getConverter().startsWith("config");
        })
        .forEach(config -> {
          MqttListener mqttListener = new MqttListener(mqttManager.getMqttClient(), this, config.getKey());

          Map<String, String> valuesMap = new HashMap<>();
          valuesMap.put("deviceId", "+");
          valuesMap.put("sensorId", "+");
          StringSubstitutor s = new StringSubstitutor(valuesMap);
          mqttListener.subscribe(s.replace(config.getKey()));
        });
  }

  public void processIncomingCanMessage(CanMessage canMessage) throws JsonProcessingException {
    logger.debug("Incoming can message:  deviceId {}, topcId {}", canMessage.getDeviceId(), canMessage.getTopicId());
    ConversionConfig conversionConfig = can2Mqtt.get(canMessage.getTopicId());
    if (conversionConfig == null) {
      logger.warn("conversion config not found for can message");
      return;
    }

    MqttTopic mqttTopic = new MqttTopic(conversionConfig.getMqttTopic());
    mqttTopic.injectValues("deviceId", canMessage.getDeviceId());

    String converter = conversionConfig.getConverter();

    if (converter.equals("heartbeat")) {
      heartBeatService.refreshDeviceTimestamp(Instant.now(), mqttTopic.getTopic());
    } else if (converter.startsWith("config")) {
      discoveryManager.configure(converter.substring(converter.indexOf("/") + 1), can2Mqtt, canMessage);
    } else {
      byte[] payload = toMqttPayload(canMessage, converter);

      logger.debug("can message publishing to {}", mqttTopic.getTopic());

      mqttManager.publishMqttMessage(mqttTopic.getTopic(), payload);
    }

  }

  public void processIncomingMqttMessage(Mqtt5Publish mqttMessage, String originalTopic) {
    logger.debug("Incoming mqtt message:  topic {}", mqttMessage.getTopic());
    ConversionConfig conversionConfig = mqtt2Can.get(originalTopic);
    if (conversionConfig == null) {
      logger.warn("conversion config not found for mqtt message");
      return;
    }
    if (!conversionConfig.getConverter().equals("heartbeat")) {
      CanMessage canMessage = toCan(mqttMessage, conversionConfig.getConverter());
      if (canMessage == null) {
        return;
      }
      try {
        int topicId = conversionConfig.getCanTopic();
        Integer deviceId = canMqttTopicConverter.getDeviceId(originalTopic, mqttMessage.getTopic().toString());
        if (deviceId == null) {
          deviceId = 0;
        }

        canMessage.setTopicId(topicId);
        canMessage.setDeviceId(deviceId);

        logger.debug("mqtt message publishing to deviceId {}, topicId {}", deviceId, topicId);

        this.sendCanQueue.put(canMessage);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  private CanMessage toCan(Mqtt5Publish mqttMessage, String converter) {
    CanMessage canMessage = new CanMessage();
    String mqttPayloadString = new String(mqttMessage.getPayloadAsBytes(), StandardCharsets.UTF_8);
    byte[] payload;
    switch (converter) {
    case "uint8ToOnOff":
      payload = gatewayConverter.onOffToUint8(mqttPayloadString);
      break;
    case "uint8ToNumber":
      payload = gatewayConverter.numberToUint8(mqttPayloadString);
      break;
    case "uint16ToNumber":
      payload = gatewayConverter.numberToUint16(mqttPayloadString);
      break;
    case "config/binary_sensor":
    case "config/switch":
      return null;
    default:
      throw new UnsupportedOperationException(converter);
    }
    canMessage.setData(payload);
    return canMessage;
  }

  private byte[] toMqttPayload(CanMessage canMessage, String converter) {
    byte[] payload;
    switch (converter) {
    case "uint8ToOnOff":
      payload = gatewayConverter.uint8ToOnOff(canMessage.getData());
      break;
    case "uint8ToNumber":
      payload = gatewayConverter.uint8ToNumber(canMessage.getData());
      break;
    case "uint16ToNumber":
      payload = gatewayConverter.uint16ToNumber(canMessage.getData());
      break;
    default:
      throw new UnsupportedOperationException(converter);
    }
    return payload;
  }

}
