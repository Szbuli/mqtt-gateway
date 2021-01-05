package hu.szbuli.smarthome.gateway.homeassistant;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import hu.szbuli.smarthome.can.CanMessage;
import hu.szbuli.smarthome.gateway.util.NumberUtils;
import hu.szbuli.smarthome.mqtt.MqttManager;
import hu.szbuli.smarthome.mqtt.MqttTopic;
import hu.szbuli.smarthome.rpi.mqtt.proxy.ConversionConfig;

public class DiscoveryManager {

  private static final Logger logger = LoggerFactory.getLogger(DiscoveryManager.class);

  private ObjectMapper objectMapper;
  private MqttManager mqttManager;
  private String gatewayName;
  private Map<Integer, DeviceType> deviceTypeMap;

  public DiscoveryManager(MqttManager mqttManager, DeviceType[] deviceTypes, String gatewayName) {
    this.objectMapper = new ObjectMapper();
    this.mqttManager = mqttManager;
    this.gatewayName = gatewayName;
    this.deviceTypeMap = Arrays.stream(deviceTypes).collect(Collectors.toMap(DeviceType::getDeviceTypeId, dt -> dt));
  }

  public void configure(Map<Integer, ConversionConfig> can2Mqtt, CanMessage canMessage) throws JsonProcessingException {
    byte[] data = canMessage.getData();
    int deviceId = canMessage.getDeviceId();
    int canStateTopicId = NumberUtils.uint16ToInteger(Arrays.copyOfRange(data, 4, 6));
    int canAvailabilityTopicId = NumberUtils.uint16ToInteger(Arrays.copyOfRange(data, 6, 8));

    MqttTopic haDiscoveryTopic = new MqttTopic(can2Mqtt.get(canMessage.getTopicId()).getMqttTopic());
    haDiscoveryTopic.injectValues("deviceId", deviceId);
    haDiscoveryTopic.injectValues("sensorId", canStateTopicId);

    DeviceType deviceType = getDeviceType(NumberUtils.uint8ToInteger(data[3]));
    String version = Integer.toString(NumberUtils.uint8ToInteger(data[0])) + "." + Integer.toString(NumberUtils.uint8ToInteger(data[1]))
        + "." + Integer.toString(NumberUtils.uint8ToInteger(data[2]));
    MqttTopic stateTopic = new MqttTopic(can2Mqtt.get(canStateTopicId).getMqttTopic());
    stateTopic.injectValues("deviceId", deviceId);
    MqttTopic availabilityTopic = new MqttTopic(can2Mqtt.get(canAvailabilityTopicId).getMqttTopic());
    availabilityTopic.injectValues("deviceId", deviceId);

    HADeviceConfig deviceConfig = new HADeviceConfig();
    deviceConfig.setSwVersion(version);
    deviceConfig.setModel(deviceType.getModel());
    deviceConfig.setManufacturer(deviceType.getManufacturer());
    deviceConfig.setIdentifiers(Integer.toString(deviceId));
    deviceConfig.setViaDevice(gatewayName);
    deviceConfig.setName(deviceType.getModel() + " - " + deviceId);

    HAEntityConfig entityConfig = new HAEntityConfig();
    entityConfig.setDevice(deviceConfig);
    entityConfig.setPlatform("mqtt");
    entityConfig.setStateTopic(stateTopic.getTopic());
    entityConfig.setAvailabilityTopic(availabilityTopic.getTopic());
    entityConfig.setUniqueId(stateTopic.getTopic());
    entityConfig.setName(stateTopic.getTopic());

    String configString = objectMapper.writeValueAsString(entityConfig);
    mqttManager.publishMqttMessage(haDiscoveryTopic.getTopic(), configString.getBytes(), true);

  }

  private DeviceType getDeviceType(int deviceTypeId) {
    DeviceType deviceType = deviceTypeMap.get(deviceTypeId);
    if (deviceType == null) {
      logger.warn("device type '{}' not found", deviceTypeId);
      return new DeviceType();
    }
    return deviceType;
  }

}
