/*
 *    Copyright 2019 The Proteus Authors
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package io.netifi.proteus.rsocket.transport;

import io.netifi.proteus.broker.info.Broker;
import java.net.InetSocketAddress;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BrokerAddressSelectors {
  private static final Logger logger = LoggerFactory.getLogger(BrokerAddressSelectors.class);

  public static Function<Broker, InetSocketAddress> TCP_ADDRESS =
      broker -> InetSocketAddress.createUnresolved(broker.getTcpAddress(), broker.getTcpPort());
  public static Function<Broker, InetSocketAddress> WEBSOCKET_ADDRESS =
      broker ->
          InetSocketAddress.createUnresolved(
              broker.getWebSocketAddress(), broker.getWebSocketPort());
}
