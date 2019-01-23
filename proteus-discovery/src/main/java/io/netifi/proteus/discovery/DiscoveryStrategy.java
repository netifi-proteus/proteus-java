package io.netifi.proteus.discovery;

import com.google.common.net.HostAndPort;
import java.util.Collection;
import reactor.core.publisher.Mono;

public interface DiscoveryStrategy {
  Mono<? extends Collection<HostAndPort>> discoverNodes();
}
