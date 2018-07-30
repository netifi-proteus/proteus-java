package io.netifi.proteus;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Gets current default configuration for {@link Proteus.Builder}. Can be overriden with System
 * properties, or if the application provides a config file. The builder will over-ride these values
 * if they are based directly in to the builder. Otherwise it will these values a default.
 */
final class DefaultBuilderConfig {

  private DefaultBuilderConfig() {}

  static boolean getKeepAlive() {
    return Boolean.getBoolean("proteus.client.keepalive.enable");
  }

  static long getTickPeriodSeconds() {
    return Long.getLong("proteus.client.keepalive.tickPeriodSeconds", 60);
  }

  static long getAckTimeoutSeconds() {
    return Long.getLong("proteus.client.keepalive.ackTimeoutSeconds", 120);
  }

  static int getMissedAcks() {
    return Integer.getInteger("proteus.client.keepalive.missedAcks", 3);
  }

  static String getHost() {
    return System.getProperty("proteus.client.host", "localhost");
  }

  static int getPort() {
    return Integer.getInteger("proteus.client.port", 8001);
  }

  static String getGroup() {
    return System.getProperty("proteus.client.group");
  }

  static String getDestination() {
    return System.getProperty("proteus.client.destination", UUID.randomUUID().toString());
  }

  static Long getAccessKey() {
    return Long.getLong("proteus.client.accessKey");
  }

  static String getAccessToken() {
    return System.getProperty("proteus.client.accessToken");
  }

  static int getPoolSize() {
    int poolSize = Math.min(4, Runtime.getRuntime().availableProcessors());
    return Integer.getInteger("proteus.client.poolSize", poolSize);
  }

  static List<SocketAddress> getSeedAddress() {
    List<SocketAddress> seedAddresses = null;
    String s = System.getProperty("proteus.client.seedAddresses");
    if (s != null) {
      seedAddresses = new ArrayList<>();
      String[] split = s.split(",");
      for (String a : split) {
        String[] split1 = a.split(":");
        if (split1.length == 2) {
          String host = split1[0];
          try {
            int port = Integer.parseInt(split1[1]);
            seedAddresses.add(InetSocketAddress.createUnresolved(host, port));
          } catch (NumberFormatException fe) {
            throw new IllegalStateException("invalid seed address: " + a);
          }
        } else {
          throw new IllegalStateException("invalid seed address: " + a);
        }
      }
    }

    return seedAddresses;
  }
}
