package io.netifi.proteus.admin;

import io.netifi.proteus.admin.connection.ConnectionManager;
import io.netifi.proteus.admin.connection.DefaultConnectionManager;
import io.netifi.proteus.frames.admin.AdminSetupFlyweight;
import io.netifi.proteus.admin.rs.AdminRSocket;
import io.netifi.proteus.admin.tracing.AdminTraceService;
import io.netifi.proteus.admin.tracing.DefaultAdminTraceService;
import io.netifi.proteus.util.TimebasedIdGenerator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.rsocket.*;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.ByteBufPayload;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.*;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

public class NetifiAdmin implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(NetifiAdmin.class);

  private static final int id = UUID.randomUUID().toString().hashCode();

  private MonoProcessor<Void> onClose;

  private AdminTraceService adminTraceService;

  private TimebasedIdGenerator idGenerator;

  private NetifiAdmin(List<SocketAddress> socketAddresses) {
    this.onClose = MonoProcessor.create();
    this.idGenerator = new TimebasedIdGenerator(id);

    Function<SocketAddress, Mono<RSocket>> rSocketFactory =
        address ->
            RSocketFactory.connect()
                .frameDecoder(Frame::retain)
                .setupPayload(createPayload())
                .transport(() -> TcpClientTransport.create((InetSocketAddress) address))
                .start();

    Function<SocketAddress, Mono<AdminRSocket>> adminSocketFactory =
        address -> Mono.fromSupplier(() -> new AdminRSocket(address, rSocketFactory, idGenerator));

    ConnectionManager connectionManager =
        new DefaultConnectionManager(idGenerator, adminSocketFactory, socketAddresses);

    this.adminTraceService = new DefaultAdminTraceService(idGenerator, connectionManager);
  }

  public static Builder builder() {
    return new Builder();
  }

  private Payload createPayload() {
    int length = AdminSetupFlyweight.computeLength();
    ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(length);
    AdminSetupFlyweight.encode(byteBuf);
    return ByteBufPayload.create(Unpooled.EMPTY_BUFFER, byteBuf);
  }

  public AdminTraceService adminTraceService() {
    return adminTraceService;
  }

  @Override
  public void dispose() {
    onClose.onComplete();
  }

  @Override
  public boolean isDisposed() {
    return onClose.isDisposed();
  }

  @Override
  public Mono<Void> onClose() {
    return onClose;
  }

  public static class Builder {
    private List<SocketAddress> addresses;
    private String host;
    private int port;

    private Builder() {}

    public Builder host(String host) {
      this.host = host;

      return this;
    }

    public Builder port(int port) {
      this.port = port;

      return this;
    }

    public Builder socketAddress(Collection<SocketAddress> addresses) {
      if (addresses instanceof List) {
        this.addresses = (List<SocketAddress>) addresses;
      } else {
        List<SocketAddress> list = new ArrayList<>();
        list.addAll(addresses);
        this.addresses = list;
      }

      return this;
    }

    public Builder socketAddress(SocketAddress address, SocketAddress... addresses) {
      List<SocketAddress> list = new ArrayList<>();
      list.add(address);

      if (addresses != null) {
        list.addAll(Arrays.asList(addresses));
      }

      return socketAddress(list);
    }

    public NetifiAdmin build() {
      List<SocketAddress> a;
      if (addresses == null) {
        Objects.requireNonNull(host, "host is required");
        Objects.requireNonNull(port, "port is required");
        a = Arrays.asList(InetSocketAddress.createUnresolved(host, port));
      } else {
        a = addresses;
      }

      return new NetifiAdmin(a);
    }
  }
}
