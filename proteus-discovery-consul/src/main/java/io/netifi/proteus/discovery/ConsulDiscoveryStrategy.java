package io.netifi.proteus.discovery;

import com.google.common.net.HostAndPort;
import com.orbitz.consul.Consul;
import com.orbitz.consul.async.ConsulResponseCallback;
import com.orbitz.consul.model.ConsulResponse;
import com.orbitz.consul.model.health.Service;
import com.orbitz.consul.model.health.ServiceHealth;
import com.orbitz.consul.option.QueryOptions;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class ConsulDiscoveryStrategy implements DiscoveryStrategy {
  private static final Logger logger = LoggerFactory.getLogger(ConsulDiscoveryStrategy.class);

  private final Consul consul;
  private final ConsulDiscoveryConfig consulDiscoveryConfig;
  private Set<HostAndPort> knownBrokers;

  public ConsulDiscoveryStrategy(ConsulDiscoveryConfig consulDiscoveryConfig) {
    this.consulDiscoveryConfig = consulDiscoveryConfig;
    logger.debug("Using consul url {}", this.consulDiscoveryConfig.getConsulURL());
    logger.debug("Using consul service name {}", this.consulDiscoveryConfig.getServiceName());
    this.consul = Consul.builder().withUrl(this.consulDiscoveryConfig.getConsulURL()).build();
    this.knownBrokers = new HashSet<>();
  }

  @Override
  public Mono<Collection<HostAndPort>> discoverNodes() {
    return Mono.create(
        sink -> {
          consul
              .healthClient()
              .getHealthyServiceInstances(
                  this.consulDiscoveryConfig.getServiceName(),
                  QueryOptions.BLANK,
                  new ConsulResponseCallback<List<ServiceHealth>>() {
                    @Override
                    public void onComplete(ConsulResponse<List<ServiceHealth>> consulResponse) {

                      List<ServiceHealth> response = consulResponse.getResponse();

                      if (response.isEmpty()) {
                        logger.debug(
                            "no brokers found in consul for {}",
                            consulDiscoveryConfig.getServiceName());
                        knownBrokers.clear();
                        sink.success();
                        return;
                      }

                      Set<HostAndPort> incomingNodes =
                          response
                              .stream()
                              .map(
                                  serviceHealth -> {
                                    Service service = serviceHealth.getService();
                                    return HostAndPort.fromParts(
                                        service.getAddress(), service.getPort());
                                  })
                              .collect(Collectors.toSet());

                      if (logger.isDebugEnabled()) {
                        for (HostAndPort port : incomingNodes) {
                          logger.debug("broker address returned from consul {}", port);
                        }
                      }

                      HashSet<HostAndPort> newBrokerSet = new HashSet<>(incomingNodes);

                      synchronized (ConsulDiscoveryStrategy.this) {
                        incomingNodes.removeAll(knownBrokers);
                        knownBrokers = newBrokerSet;
                      }

                      if (incomingNodes.isEmpty()) {
                        logger.debug("no new broker found");
                        sink.success();
                      } else {
                        if (logger.isDebugEnabled()) {
                          for (HostAndPort port : incomingNodes) {
                            logger.debug("found new broker from consul with address {}", port);
                          }
                        }

                        sink.success(incomingNodes);
                      }
                    }

                    @Override
                    public void onFailure(Throwable throwable) {
                      sink.error(throwable);
                    }
                  });
        });
  }
}
