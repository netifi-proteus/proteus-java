package io.netifi.proteus.admin.connection;

import io.netifi.proteus.frames.admin.AdminRouterNodeInfoEventType;
import io.netifi.proteus.frames.admin.AdminRouterNodeInfoFlyweight;
import io.netifi.proteus.frames.admin.AdminRouterNodeInfoResultFlyweight;
import io.netifi.proteus.discovery.DiscoveryEvent;
import io.netifi.proteus.discovery.SocketAddressFactory;
import io.netifi.proteus.util.TimebasedIdGenerator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.rsocket.Payload;
import io.rsocket.util.ByteBufPayload;
import java.net.InetSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

class RouterInfoSocketAddressFactory implements SocketAddressFactory {
  private static final Logger logger =
      LoggerFactory.getLogger(RouterInfoSocketAddressFactory.class);

  private ConnectionManager connectionManager;
  private TimebasedIdGenerator idGenerator;

  public RouterInfoSocketAddressFactory(
      ConnectionManager connectionManager, TimebasedIdGenerator idGenerator) {
    this.connectionManager = connectionManager;
    this.idGenerator = idGenerator;
  }

  @Override
  public Flux<DiscoveryEvent> get() {
    return Flux.defer(
            () -> {
              return connectionManager
                  .getRSocket()
                  .flatMapMany(
                      rSocket -> {
                        logger.debug(
                            "streaming connection information from router id {} address {}",
                            rSocket.getRouterId(),
                            rSocket.getSocketAddress());
                        int length = AdminRouterNodeInfoFlyweight.computeLength();
                        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(length);
                        AdminRouterNodeInfoFlyweight.encode(byteBuf, idGenerator.nextId());
                        Payload payload = ByteBufPayload.create(Unpooled.EMPTY_BUFFER, byteBuf);
                        return rSocket
                            .requestStream(payload)
                            .map(
                                result -> {
                                  ByteBuf metadata = result.sliceMetadata();
                                  AdminRouterNodeInfoEventType type =
                                      AdminRouterNodeInfoResultFlyweight.eventType(metadata);
                                  String routerId =
                                      AdminRouterNodeInfoResultFlyweight.routerId(metadata);
                                  String host =
                                      AdminRouterNodeInfoResultFlyweight.adminAddress(metadata);
                                  int port = AdminRouterNodeInfoResultFlyweight.adminPort(metadata);

                                  switch (type) {
                                    case JOIN:
                                      logger.debug(
                                          "router with host {} and port {} joining", host, port);
                                      return DiscoveryEvent.add(
                                          routerId, InetSocketAddress.createUnresolved(host, port));
                                    case LEAVE:
                                      logger.debug(
                                          "router with host {} and port {} leaving", host, port);
                                      return DiscoveryEvent.remove(
                                          routerId, InetSocketAddress.createUnresolved(host, port));
                                    default:
                                      throw new IllegalStateException(
                                          "unknown router node info event type");
                                  }
                                });
                      });
            })
        .repeat()
        .retry()
        .publish()
        .refCount();
  }
}
