package io.netifi.proteus.rs;

import io.netifi.proteus.presence.PresenceNotifier;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import org.junit.Test;
import org.mockito.Mockito;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;

public class PresenceAwareRSocketTest {
  @Test
  public void testShouldWaitForPresenceOfDestination() {
    RSocket mock = Mockito.mock(RSocket.class);
    Mockito.when(mock.requestResponse(Mockito.any())).thenReturn(Mono.never());
    Mockito.when(mock.onClose()).thenReturn(Mono.never());
    PresenceNotifier presenceNotifier = Mockito.mock(PresenceNotifier.class);

    Mockito.when(
            presenceNotifier.notify(Mockito.anyLong(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(Mono.never());

    PresenceAwareRSocket rSocket =
        PresenceAwareRSocket.wrap(
            mock,
            Long.MAX_VALUE,
            "testShouldWaitForPresence-dest",
            "testShouldWaitForPresence-group",
            presenceNotifier);

    StepVerifier.create(rSocket.requestResponse(Mockito.mock(Payload.class)))
        .expectNextCount(0)
        .thenCancel()
        .verify(Duration.ofSeconds(1));
  }

  @Test
  public void testShouldWaitForPresenceOfGroup() {
    RSocket mock = Mockito.mock(RSocket.class);
    Mockito.when(mock.requestResponse(Mockito.any())).thenReturn(Mono.never());
    Mockito.when(mock.onClose()).thenReturn(Mono.never());
    PresenceNotifier presenceNotifier = Mockito.mock(PresenceNotifier.class);

    Mockito.when(presenceNotifier.notify(Mockito.anyLong(), Mockito.anyString()))
        .thenReturn(Mono.never());

    PresenceAwareRSocket rSocket =
        PresenceAwareRSocket.wrap(
            mock, Long.MAX_VALUE, null, "testShouldWaitForPresence-group", presenceNotifier);

    StepVerifier.create(rSocket.requestResponse(Mockito.mock(Payload.class)))
        .expectNextCount(0)
        .thenCancel()
        .verify(Duration.ofSeconds(1));
  }
}
