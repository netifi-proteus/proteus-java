package io.netifi.proteus.discovery;

import com.google.common.net.HostAndPort;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.ec2.Ec2AsyncClient;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Filter;

public class EC2TagsDiscoveryStrategy implements DiscoveryStrategy {

  private Ec2AsyncClient client;
  private final String tagName;
  private final String tagValue;
  private final int staticPort;

  private Set<HostAndPort> knownBrokers;

  public EC2TagsDiscoveryStrategy(String tagName, String tagValue, int staticPort) {
    // TODO: when should we: client.close(); ?
    this.client = Ec2AsyncClient.builder().build();
    this.tagName = tagName;
    this.tagValue = tagValue;
    this.staticPort = staticPort;
  }

  @Override
  public Mono<? extends Collection<HostAndPort>> discoverNodes() {
    return Mono.fromSupplier(this::getInstances);
  }

  private Collection<HostAndPort> getInstances() {
    final CompletableFuture<DescribeInstancesResponse> future =
        client.describeInstances(
            DescribeInstancesRequest.builder()
                .filters(Filter.builder().name("tag:" + tagName).values(tagValue).build())
                .build());
    Set<HostAndPort> incomingNodes =
        future
            .handle(
                (resp, err) -> {
                  try {
                    if (resp != null) {
                      return resp.reservations()
                          .stream()
                          .flatMap(
                              reservation ->
                                  reservation
                                      .instances()
                                      .stream()
                                      .map(
                                          instance ->
                                              HostAndPort.fromParts(
                                                  instance.privateIpAddress(), staticPort)))
                          .collect(Collectors.toSet());
                    } else {
                      throw Exceptions.propagate(err);
                    }
                  } catch (Exception e) {
                    throw Exceptions.propagate(e);
                  }
                })
            .join();

    Set<HostAndPort> diff = new HashSet<>(incomingNodes);
    synchronized (this) {
      diff.removeAll(knownBrokers);
      knownBrokers = incomingNodes;
    }
    return diff;
  }
}
