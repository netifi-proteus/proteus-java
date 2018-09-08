package io.netifi.proteus.vizceral.service;

import io.netifi.proteus.vizceral.Node;
import reactor.core.publisher.Flux;

public interface Vizceral {

  Flux<Node> visualisations();
}
