package io.netifi.proteus.discovery;

import com.google.common.net.HostAndPort;
import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class StaticListDiscoveryStrategy implements DiscoveryStrategy {
  private static final Logger logger = LoggerFactory.getLogger(StaticListDiscoveryStrategy.class);

  private Mono<? extends Collection<HostAndPort>> nodes;

  public StaticListDiscoveryStrategy(String addresses) {
    this.nodes =
        Mono.defer(
                () -> {
                  if (addresses.isEmpty()) {
                    return Mono.empty();
                  } else {
                    logger.debug("seeding cluster with {}", addresses);
                    return Flux.fromArray(addresses.split(","))
                        .map(
                            hostPortString ->
                                HostAndPort.fromString(hostPortString).withDefaultPort(7001))
                        .collectList();
                  }
                })
            .cache();
  }

  @Override
  public Mono<? extends Collection<HostAndPort>> discoverNodes() {
    return nodes;
  }
}
