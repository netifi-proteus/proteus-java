package io.netifi.proteus.rsocket.transport;

import io.netifi.proteus.broker.info.Broker;
import java.net.InetSocketAddress;
import java.util.function.Function;

public class BrokerAddressSelectors {

  public static Function<Broker, InetSocketAddress> TCP_ADDRESS =
      broker -> InetSocketAddress.createUnresolved(broker.getIpAddress(), broker.getPort());
  public static Function<Broker, InetSocketAddress> WEBSOCKET_ADDRESS =
      broker ->
          InetSocketAddress.createUnresolved(
              broker.getWebSocketAddress(), broker.getWebSocketPort());
}
