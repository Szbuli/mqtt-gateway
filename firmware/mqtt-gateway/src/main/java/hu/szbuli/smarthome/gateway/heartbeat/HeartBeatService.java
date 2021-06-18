package hu.szbuli.smarthome.gateway.heartbeat;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import hu.szbuli.smarthome.can.SafeThread;
import hu.szbuli.smarthome.mqtt.MqttManager;
import hu.szbuli.smarthome.rpi.mqtt.proxy.MqttConfiguration;

public class HeartBeatService extends SafeThread {

  private static final Logger logger = LoggerFactory.getLogger(HeartBeatService.class);

  private static final int MAX_IDLE_TIME_SECONDS = 70;

  public static final byte[] ONLINE_PAYLOAD = "online".getBytes();
  public static final byte[] OFFLINE_PAYLOAD = "offline".getBytes();

  private Map<String, Instant> heartbeatMap;
  private Map<String, MqttManager> gatewayClientMap;
  private MqttConfiguration mqttConfiguration;

  public HeartBeatService(MqttConfiguration mqttConfiguration) {
    super();
    this.heartbeatMap = new HashMap<>();
    this.gatewayClientMap = new HashMap<>();
    this.mqttConfiguration = mqttConfiguration;
  }

  @Override
  public void doRun() {
    try {
      while (true) {
        Instant currentTime = Instant.now();

        Iterator<Entry<String, Instant>> it = heartbeatMap.entrySet().iterator();
        while (it.hasNext()) {
          Entry<String, Instant> entry = it.next();
          if (Duration.between(entry.getValue(), currentTime).compareTo(Duration.ofSeconds(MAX_IDLE_TIME_SECONDS)) > 0) {
            logger.debug("connected device went offline: {}", entry.getKey());
            it.remove();
            MqttManager mqttManager = gatewayClientMap.remove(entry.getKey());
            mqttManager.disconnect();
          }
        }

        TimeUnit.SECONDS.sleep(15);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  @Override
  public void setThreadName() {
    this.threadName = "HEARTBEAT";
  }

  synchronized public void refreshDeviceTimestamp(Instant time, String mqttTopic) {
    if (!heartbeatMap.containsKey(mqttTopic)) {
      MqttManager mqttManager = new MqttManager(this.mqttConfiguration, mqttTopic);
      mqttManager.connect(true, false);
      gatewayClientMap.put(mqttTopic, mqttManager);
    }
    heartbeatMap.put(mqttTopic, time);
  }

}
