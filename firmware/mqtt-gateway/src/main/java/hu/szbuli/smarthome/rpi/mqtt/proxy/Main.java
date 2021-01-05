package hu.szbuli.smarthome.rpi.mqtt.proxy;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Instant;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;

import hu.szbuli.smarthome.can.CanReceiveThread;
import hu.szbuli.smarthome.can.CanSendThread;
import hu.szbuli.smarthome.gateway.heartbeat.HeartBeatService;
import hu.szbuli.smarthome.lora.SocketManager;

public class Main {

  private static final Logger logger = LoggerFactory.getLogger(Main.class);
  private static Mqtt5AsyncClient client = null;

  public static void main(String[] args) throws IOException, ParseException {
    Options options = new Options();
    options.addOption("m", "mqtt", true, "mqtt config file");
    options.addOption("g", "gateway", true, "gateway config file");

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    final String mqttConfigFile = cmd.getOptionValue("m", "./mqtt.properties");
    final String gatewayConfigFile = cmd.getOptionValue("g", "./config.csv");

    Mqtt5AsyncClient mqttClient = initMqttClient(mqttConfigFile);

    Gateway gateway = new Gateway(gatewayConfigFile, mqttClient, CanSendThread.sendCanQueue);

    CanSendThread canSendThread = new CanSendThread();
    canSendThread.start();

    CanReceiveThread canRecieveThread = new CanReceiveThread(gateway);
    canRecieveThread.start();

    SocketManager loraSocketManager = new SocketManager();
    loraSocketManager.start();
  }

  private static Mqtt5AsyncClient initMqttClient(String mqttConfigFile) throws FileNotFoundException, IOException {
    Properties prop = new Properties();
    prop.load(new FileInputStream(mqttConfigFile));

    String statusTopic = prop.getProperty("statusTopic");

    client = MqttClient.builder()
        .automaticReconnectWithDefaultConfig()
        .addDisconnectedListener(context -> {
          logger.error("disconected from mqtt ({})", Instant.now(), context.getCause());
        })
        .addConnectedListener(context -> {
          logger.info("connected to mqtt ({})", Instant.now());
          client.publishWith()
              .topic(statusTopic)
              .payload(HeartBeatService.ONLINE_PAYLOAD)
              .qos(MqttQos.AT_MOST_ONCE)
              .retain(true)
              .send();
        })
        .useMqttVersion5()
        .willPublish()
        .topic(statusTopic).retain(true)
        .payload(HeartBeatService.OFFLINE_PAYLOAD)
        .applyWillPublish()
        .simpleAuth()
        .username(prop.getProperty("username"))
        .password(prop.getProperty("password").getBytes())
        .applySimpleAuth()
        .identifier(UUID.randomUUID().toString())
        .serverHost(prop.getProperty("host"))
        .serverPort(Integer.parseInt(prop.getProperty("port")))
        // .sslWithDefaultConfig()
        .buildAsync();

    client.connectWith()
        .send();

    return client;
  }

}
