package hu.szbuli.smarthome.gateway.util;

public class NumberUtils {

  public static int uint16ToInteger(byte[] uint16) {
    return Byte.toUnsignedInt(uint16[0]) | Byte.toUnsignedInt(uint16[1]) << 8;
  }

  public static int uint8ToInteger(byte uint8) {
    return Byte.toUnsignedInt(uint8);
  }

}
