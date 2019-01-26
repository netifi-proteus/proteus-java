package io.netifi.proteus.discovery;

import com.google.common.net.HostAndPort;
import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Objects;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;

public interface DiscoveryStrategy {

  // TODO: pull static list into this version
  // TODO: pull in HostAndPort to this project
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
