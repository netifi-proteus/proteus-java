package io.netifi.proteus.rs;

import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.ByteBufPayload;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class DefaultProteusSocketTest {

  @Test
  public void testRequestResponse() {
    Payload incoming = Mockito.mock(Payload.class);

    Payload transformed = ByteBufPayload.create("transformed");
    Payload outgoing = ByteBufPayload.create("outgoing");

    RSocket mock = Mockito.mock(RSocket.class);
    Mockito.when(mock.requestResponse(Mockito.any(Payload.class))).thenReturn(Mono.just(outgoing));
    DefaultProteusSocket socket = new DefaultProteusSocket(payload -> transformed, () -> mock);
    Payload block = socket.requestResponse(incoming).block();

    Assert.assertTrue(block == outgoing);
    Mockito.verify(incoming, Mockito.times(1)).release();
  }

  @Test
  public void testFireForget() {
    Payload incoming = Mockito.mock(Payload.class);

    Payload transformed = ByteBufPayload.create("transformed");

    RSocket mock = Mockito.mock(RSocket.class);
    Mockito.when(mock.fireAndForget(Mockito.any(Payload.class))).thenReturn(Mono.empty());
    DefaultProteusSocket socket = new DefaultProteusSocket(payload -> transformed, () -> mock);
    socket.fireAndForget(incoming).block();

    Mockito.verify(incoming, Mockito.times(1)).release();
  }

  @Test
  public void testRequestStream() {
    Payload incoming = Mockito.mock(Payload.class);

    Payload transformed = ByteBufPayload.create("transformed");
    Payload outgoing = ByteBufPayload.create("outgoing");

    RSocket mock = Mockito.mock(RSocket.class);
    Mockito.when(mock.requestStream(Mockito.any(Payload.class))).thenReturn(Flux.just(outgoing));
    DefaultProteusSocket socket = new DefaultProteusSocket(payload -> transformed, () -> mock);
    Payload block = socket.requestStream(incoming).blockLast();

    Assert.assertTrue(block == outgoing);
    Mockito.verify(incoming, Mockito.times(1)).release();
  }

  @Test
  public void testRequestChannel() {
    Payload incoming = Mockito.mock(Payload.class);

    Payload transformed = ByteBufPayload.create("transformed");
    Payload outgoing = ByteBufPayload.create("outgoing");

    DefaultProteusSocket socket =
        new DefaultProteusSocket(
            payload -> transformed,
            () ->
                new AbstractRSocket() {
                  @Override
                  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
                    return Flux.from(payloads).thenMany(Flux.just(outgoing));
                  }
                });
    Payload block = socket.requestChannel(Mono.just(incoming)).blockLast();

    Assert.assertTrue(block == outgoing);
    Mockito.verify(incoming, Mockito.times(1)).release();
  }
}
