package io.netifi.proteus.discovery;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import reactor.core.publisher.Flux;

/** Used to generate SocketAddresses to the Netifi Proteus Platform */
public interface SocketAddressFactory extends Supplier<Flux<DiscoveryEvent>> {

  /**
   * Generates a static SocketAddressFactory using the a host name and port
   *
   * @param host DNS name or IP address
   * @param port port
   * @return SocketAddressFactory implementation
   */
  static SocketAddressFactory from(String host, int port) {
    InetSocketAddress address = InetSocketAddress.createUnresolved(host, port);
    return from(address);
  }

  /**
   * Generates a static SocketAddressFactory from one or more SocketAddresses
   *
   * @param address
   * @param addresses
   * @return SocketFactory implementation
   */
  static SocketAddressFactory from(SocketAddress address, SocketAddress... addresses) {
    List<SocketAddress> list = new ArrayList<>();
    list.add(address);

    if (addresses != null) {
      list.addAll(Arrays.asList(addresses));
    }

    return from(list);
  }

  /**
   * Genereates a static SocketAddressFactory from a list of SocketAddresses
   *
   * @param addresses the list of SocketAddresses
   * @return SocketFactory implementation
   */
  static SocketAddressFactory from(List<? extends SocketAddress> addresses) {
    return () -> Flux.fromIterable(addresses).map(DiscoveryEvent::add);
  }
}
