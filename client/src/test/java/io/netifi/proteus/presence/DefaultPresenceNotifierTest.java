package io.netifi.proteus.presence;

import io.netifi.proteus.Netifi;
import io.netifi.proteus.auth.SessionUtil;
import io.netifi.proteus.balancer.LoadBalancedRSocketSupplier;
import io.netifi.proteus.frames.DestinationSetupFlyweight;
import io.netifi.proteus.rs.SecureRSocket;
import io.netifi.proteus.util.TimebasedIdGenerator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelOption;
import io.rsocket.AbstractRSocket;
import io.rsocket.Frame;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.ByteBufPayload;
import io.rsocket.util.RSocketProxy;
import java.time.Duration;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.ipc.netty.tcp.TcpClient;

@Ignore
public class DefaultPresenceNotifierTest {

  private static final long accessKey = 3855261330795754807L;
  private static final String accessToken = "n9R9042eE1KaLtE56rbWjBIGymo=";

  @Test
  public void testGroupPresence() throws Exception {

    SecureRSocket rSocket = createRSocketConnection("tester", "test.groupPresence", 8001);

    LoadBalancedRSocketSupplier supplier = Mockito.mock(LoadBalancedRSocketSupplier.class);
    Mockito.when(supplier.get()).thenReturn(rSocket);
    Mockito.when(supplier.onClose()).thenReturn(Mono.empty());

    DefaultPresenceNotifier handler =
        new DefaultPresenceNotifier(new TimebasedIdGenerator(1), accessKey, "tester", supplier);

    handler.notify(Long.MAX_VALUE, "test.groupPresence").block();

    Netifi build =
        Netifi.builder()
            .accessKey(accessKey)
            .accountId(Long.MAX_VALUE)
            .accessToken(accessToken)
            .destination("test1")
            .group("anotherGroup")
            .host("127.0.0.1")
            .port(8001)
            .build();

    handler.notify(Long.MAX_VALUE, "anotherGroup").block();

    try {
      handler
          .notify(Long.MAX_VALUE, "anotherGroup2")
          .timeout(Duration.ofSeconds(4), Schedulers.elastic())
          .block();
      Assert.fail();
    } catch (Throwable t) {
      if (!t.getMessage().contains("Timeout")) {
        Assert.fail();
      }
    }

    Netifi build2 =
        Netifi.builder()
            .accessKey(accessKey)
            .accountId(Long.MAX_VALUE)
            .accessToken(accessToken)
            .destination("test1")
            .group("anotherGroup2")
            .host("127.0.0.1")
            .port(8001)
            .build();

    handler.notify(Long.MAX_VALUE, "anotherGroup2").block();

    build.dispose();

    //    try {
    //      handler.notify(Long.MAX_VALUE, "anotherGroup").timeout(Duration.ofSeconds(8)).block();
    //      Assert.fail();
    //    } catch (Throwable t) {
    //      if (!t.getMessage().contains("Timeout")) {
    //        Assert.fail();
    //      }
    //    }
  }

  @Test
  public void testDestinationPresence() {
    SecureRSocket rSocket = createRSocketConnection("tester", "test.destinationPresence", 8001);

    LoadBalancedRSocketSupplier supplier = Mockito.mock(LoadBalancedRSocketSupplier.class);
    Mockito.when(supplier.get()).thenReturn(rSocket);
    Mockito.when(supplier.onClose()).thenReturn(Mono.empty());

    DefaultPresenceNotifier handler =
        new DefaultPresenceNotifier(new TimebasedIdGenerator(1), accessKey, "tester", supplier);

    handler.notify(Long.MAX_VALUE, "tester", "test.destinationPresence").block();
  }

  public SecureRSocket createRSocketConnection(String destination, String group, int port) {
    return createRSocketConnection(destination, group, new AbstractRSocket() {}, port);
  }

  public SecureRSocket createRSocketConnection(
      String destination, String group, RSocket handler, int port) {
    int length = DestinationSetupFlyweight.computeLength(false, destination, group);

    byte[] accessTokenBytes = Base64.getDecoder().decode(accessToken);

    ByteBuf byteBuf = Unpooled.directBuffer(length);
    DestinationSetupFlyweight.encode(
        byteBuf,
        Unpooled.EMPTY_BUFFER,
        Unpooled.wrappedBuffer(accessTokenBytes),
        System.currentTimeMillis(),
        accessKey,
        destination,
        group);

    TcpClient tcpClient =
        TcpClient.create(
            builder -> {
              builder.option(ChannelOption.SO_RCVBUF, 1_048_576);
              builder.option(ChannelOption.SO_SNDBUF, 1_048_576);
              builder.option(ChannelOption.TCP_NODELAY, true);
              builder.option(ChannelOption.SO_KEEPALIVE, true);
              builder.host("127.0.0.1");
              builder.port(port);
              builder.build();
            });

    RSocket client =
        RSocketFactory.connect()
            .frameDecoder(Frame::retain)
            .setupPayload(ByteBufPayload.create(Unpooled.EMPTY_BUFFER, byteBuf))
            .errorConsumer(Throwable::printStackTrace)
            .acceptor(rSocket -> handler)
            .transport(TcpClientTransport.create(tcpClient))
            .start()
            .block();

    return new SecureRSocketWrapper(client);
  }

  class SecureRSocketWrapper extends RSocketProxy implements SecureRSocket {
    public SecureRSocketWrapper(RSocket source) {
      super(source);
    }

    @Override
    public SessionUtil getSessionUtil() {
      return null;
    }

    @Override
    public Mono<AtomicLong> getCurrentSessionCounter() {
      return null;
    }

    @Override
    public Mono<byte[]> getCurrentSessionToken() {
      return null;
    }
  }
}
