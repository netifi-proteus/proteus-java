package io.netifi.proteus.discovery;

import io.netifi.proteus.common.net.HostAndPort;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.ec2.Ec2AsyncClient;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Filter;

public class EC2TagsDiscoveryStrategy implements DiscoveryStrategy {
  private static final Logger logger = LoggerFactory.getLogger(EC2TagsDiscoveryStrategy.class);

  private Ec2AsyncClient client;
  private final EC2TagsDiscoveryConfig ec2TagsDiscoveryConfig;

  private Set<HostAndPort> knownBrokers;

  public EC2TagsDiscoveryStrategy(EC2TagsDiscoveryConfig ec2TagsDiscoveryConfig) {
    // TODO: when should we: client.close(); ?
    this.client = Ec2AsyncClient.builder().build();
    this.ec2TagsDiscoveryConfig = ec2TagsDiscoveryConfig;
  }

  @Override
  public Mono<? extends Collection<HostAndPort>> discoverNodes() {
    logger.debug(
        "using tag name {} and tag value {}",
        this.ec2TagsDiscoveryConfig.getTagName(),
        this.ec2TagsDiscoveryConfig.getTagValue());
    final CompletableFuture<DescribeInstancesResponse> future =
        client.describeInstances(
            DescribeInstancesRequest.builder()
                .filters(
                    Filter.builder()
                        .name("tag:" + this.ec2TagsDiscoveryConfig.getTagName())
                        .values(this.ec2TagsDiscoveryConfig.getTagValue())
                        .build())
                .build());
    return Mono.fromFuture(future)
        .map(
            resp -> {
              Set<HostAndPort> incomingNodes =
                  resp.reservations()
                      .stream()
                      .flatMap(
                          reservation ->
                              reservation
                                  .instances()
                                  .stream()
                                  .map(
                                      instance -> {
                                        String instanceIP = instance.privateIpAddress();
                                        int port = this.ec2TagsDiscoveryConfig.getPort();
                                        logger.debug(
                                            "found instance {} with private ip {} and port {}",
                                            instance.instanceId(),
                                            instanceIP,
                                            port);
                                        return HostAndPort.fromParts(instanceIP, port);
                                      }))
                      .collect(Collectors.toSet());

              Set<HostAndPort> diff = new HashSet<>(incomingNodes);
              synchronized (this) {
                diff.removeAll(knownBrokers);
                knownBrokers = incomingNodes;
              }
              logger.debug("returning these nodes {}", diff);
              return diff;
            });
  }
}
