package io.netifi.proteus.discovery;

import com.google.common.net.HostAndPort;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.Configuration;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.models.V1EndpointPort;
import io.kubernetes.client.models.V1Endpoints;
import io.kubernetes.client.util.Config;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;

public class KubernetesDiscoveryStrategy implements DiscoveryStrategy {
  private static final Logger logger = LoggerFactory.getLogger(KubernetesDiscoveryStrategy.class);

  private final CoreV1Api client;
  private final String namespace;
  private final String deploymentName;
  private final String portName;
  private final HostAndPort currentAddress;
  private Set<HostAndPort> knownBrokers;

  public KubernetesDiscoveryStrategy(
      String namespace, String deploymentName, String portName, HostAndPort currentAddress) {

    this.namespace = namespace;
    this.deploymentName = deploymentName;
    this.portName = portName;
    this.currentAddress = currentAddress;
    this.knownBrokers = new HashSet<>();
    ApiClient apiClient = null;
    try {
      apiClient = Config.defaultClient();
    } catch (IOException e) {
      throw Exceptions.propagate(e);
    }
    Configuration.setDefaultApiClient(apiClient);

    this.client = new CoreV1Api();

    logger.info(
        "netifi.discovery.kubernetes -> searching in namespace {} for endpoint {} with portName {}",
        namespace,
        deploymentName,
        portName);
  }

  @Override
  public Mono<Collection<HostAndPort>> discoverNodes() {
    return Mono.fromSupplier(this::getEndpoints);
  }

  private Collection<HostAndPort> getEndpoints() {
    try {

      V1Endpoints endpoints =
          client.readNamespacedEndpoints(deploymentName, namespace, null, true, true);

      Set<HostAndPort> incomingNodes =
          endpoints
              .getSubsets()
              .stream()
              .flatMap(
                  subset -> {
                    logger.debug("got subset: {}", subset.toString());
                    V1EndpointPort endpointPort =
                        subset
                            .getPorts()
                            .stream()
                            .filter(p -> p.getName().equals(portName))
                            .findFirst()
                            .orElse(new V1EndpointPort().name(portName).port(7001).protocol("tcp"));
                    return subset
                        .getAddresses()
                        .stream()
                        .filter(addr -> !addr.getIp().equals(currentAddress.getHost()))
                        .map(
                            address -> {
                              HostAndPort hostAndPort =
                                  HostAndPort.fromParts(address.getIp(), endpointPort.getPort());

                              if (!knownBrokers.contains(hostAndPort)) {
                                logger.info(
                                    "kubernetes discovery found broker seed node {}:{}",
                                    hostAndPort.getHost(),
                                    hostAndPort.getPort());
                              }
                              return hostAndPort;
                            });
                  })
              .collect(Collectors.toSet());

      Set<HostAndPort> diff = new HashSet<>(incomingNodes);
      synchronized (this) {
        diff.removeAll(knownBrokers);
        knownBrokers = incomingNodes;
      }

      return diff;

    } catch (Exception e) {
      throw Exceptions.propagate(e);
    }
  }
}
