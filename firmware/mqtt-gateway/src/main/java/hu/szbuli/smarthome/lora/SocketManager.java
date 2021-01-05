package hu.szbuli.smarthome.lora;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import hu.szbuli.smarthome.can.SafeThread;

public class SocketManager extends SafeThread {

  private static final Logger logger = LoggerFactory.getLogger(SocketManager.class);

  @Override
  public void doRun() {
    LoraServer loraServer = new LoraServer();
    
    try (ServerSocket server = new ServerSocket(5009, 0, InetAddress.getLoopbackAddress())) {
      logger.info("lora socket started at port 5009");

      while (true) {
        logger.info("waiting for lora client");
        Socket client = server.accept();
        logger.info("client joined to lora socket");
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(client.getInputStream()));
        String line;
        while ((line = bufferedReader.readLine()) != null) {
          logger.info("lora client sent: {}", line);
          loraServer.processMessage(line);
        }
        logger.info("lora client disconnected");
      }
    } catch (IOException e) {
      logger.error("lora socket error", e);
    }
  }

  @Override
  public void setThreadName() {
    this.threadName = "LORA_SOCKET_SERVER_THREAD";

  }

}
