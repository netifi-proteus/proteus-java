package io.netifi.proteus;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Gets current default configuration for {@link io.netifi.proteus.Netifi.Builder}. Can be overriden
 * with System properties, or if the application provides a config file. The builder will over-ride
 * these values if they are based directly in to the builder. Otherwise it will these values a
 * default.
 */
final class DefaultBuilderConfig {

  private DefaultBuilderConfig() {}

  static boolean getKeepAlive() {
    boolean keepalive = false;
    try {
      keepalive = ConfigHolder.conf.getBoolean("proteus.client.keepalive.enable");
    } catch (ConfigException.Missing m) {

    }

    return keepalive;
  }

  static long getTickPeriodSeconds() {
    long tickPeriodSeconds = 60;
    try {
      tickPeriodSeconds = ConfigHolder.conf.getLong("proteus.client.keepalive.tickPeriodSeconds");
    } catch (ConfigException.Missing m) {

    }

    return tickPeriodSeconds;
  }

  static long getAckTimeoutSeconds() {
    long ackTimeoutSeconds = 120;
    try {
      ackTimeoutSeconds = ConfigHolder.conf.getLong("proteus.client.keepalive.ackTimeoutSeconds");
    } catch (ConfigException.Missing m) {

    }

    return ackTimeoutSeconds;
  }

  static int getMissedAcks() {
    int missedAcks = 3;
    try {
      missedAcks = ConfigHolder.conf.getInt("proteus.client.keepalive.missedAcks");
    } catch (ConfigException.Missing m) {
    }

    return missedAcks;
  }

  static String getHost() {
    String host = null;
    try {
      host = ConfigHolder.conf.getString("proteus.client.host");
    } catch (ConfigException.Missing m) {

    }

    return host;
  }

  static int getPort() {
    int port = 8001;
    try {
      port = ConfigHolder.conf.getInt("proteus.client.port");
    } catch (ConfigException.Missing m) {

    }

    return port;
  }

  static String getGroup() {
    String group = null;
    try {
      group = ConfigHolder.conf.getString("proteus.client.group");
    } catch (ConfigException.Missing m) {

    }

    return group;
  }

  static String getDestination() {
    String destination = null;
    try {
      destination = ConfigHolder.conf.getString("proteus.client.destination");
    } catch (ConfigException.Missing m) {

    }

    return destination;
  }

  static Long getAccountId() {
    Long accountId = null;

    try {
      accountId = ConfigHolder.conf.getLong("proteus.client.accountId");
    } catch (ConfigException.Missing m) {

    }

    return accountId;
  }

  static Long getAccessKey() {
    Long accessKey = null;

    try {
      accessKey = ConfigHolder.conf.getLong("proteus.client.accessKey");
    } catch (ConfigException.Missing m) {

    }

    return accessKey;
  }

  static String getAccessToken() {
    String accessToken = null;

    try {
      accessToken = ConfigHolder.conf.getString("proteus.client.accessToken");
    } catch (ConfigException.Missing m) {

    }

    return accessToken;
  }

  static int getPoolSize() {
    int poolSize = Math.min(4, Runtime.getRuntime().availableProcessors());
    try {
      poolSize = ConfigHolder.conf.getInt("proteus.client.poolSize");
    } catch (ConfigException.Missing m) {
    }
    return poolSize;
  }

  static int getMinHostsAtStartup() {
    int minHostsAtStartup = 3;
    try {
      minHostsAtStartup = ConfigHolder.conf.getInt("proteus.client.minHostsAtStartup");
    } catch (ConfigException.Missing m) {
    }
    return minHostsAtStartup;
  }

  static long getMinHostsAtStartupTimeoutSeconds() {
    long minHostsAtStartupTimeout = 5;

    try {
      minHostsAtStartupTimeout =
          ConfigHolder.conf.getLong("proteus.client.minHostsAtStartupTimeout");
    } catch (ConfigException.Missing m) {
    }
    return minHostsAtStartupTimeout;
  }

  static List<SocketAddress> getSeedAddress() {
    List<SocketAddress> seedAddresses = null;
    try {
      String s = ConfigHolder.conf.getString("proteus.client.seedAddresses");
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
    } catch (ConfigException.Missing m) {

    }

    return seedAddresses;
  }

  private static class ConfigHolder {
    private static final Config conf;

    static {
      conf = ConfigFactory.load();
    }
  }
}
