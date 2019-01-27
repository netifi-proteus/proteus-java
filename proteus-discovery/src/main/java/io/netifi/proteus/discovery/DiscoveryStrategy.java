package io.netifi.proteus.discovery;

import io.netifi.proteus.common.net.HostAndPort;
import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Objects;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;

public interface DiscoveryStrategy {

  static DiscoveryStrategy getInstance(DiscoveryConfig discoveryConfig) {
    Objects.requireNonNull(discoveryConfig);
    try {
      Class discoveryStrategyClass = discoveryConfig.getDiscoveryStrategyClass();
      Constructor discoveryStrategyClassConstructor =
          discoveryStrategyClass.getConstructor(discoveryConfig.getClass());
      return (DiscoveryStrategy) discoveryStrategyClassConstructor.newInstance(discoveryConfig);

    } catch (Exception e) {
      throw Exceptions.propagate(e);
    }
  }

  Mono<? extends Collection<HostAndPort>> discoverNodes();
}
