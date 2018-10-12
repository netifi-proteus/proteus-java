package io.netifi.proteus.rsocket;

import io.netifi.proteus.presence.PresenceNotifier;
import io.netifi.proteus.tags.DefaultTags;
import io.netifi.proteus.tags.Tags;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import java.time.Duration;
import org.junit.Test;
import org.mockito.Mockito;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class PresenceAwareRSocketTest {
  @Test
  public void testShouldWaitForPresenceOfDestination() {
    RSocket mock = Mockito.mock(RSocket.class);
    Mockito.when(mock.requestResponse(Mockito.any())).thenReturn(Mono.never());
    Mockito.when(mock.onClose()).thenReturn(Mono.never());
    PresenceNotifier presenceNotifier = Mockito.mock(PresenceNotifier.class);

    Mockito.when(presenceNotifier.notify(Mockito.any(Tags.class))).thenReturn(Mono.never());

    Tags tags =
        new DefaultTags()
            .add("group", "testShouldWaitForPresence-group")
            .add("destination", "testShouldWaitForPresence-dest");
    PresenceAwareRSocket rSocket = PresenceAwareRSocket.wrap(mock, tags, presenceNotifier);

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

    Mockito.when(presenceNotifier.notify(Mockito.any(Tags.class))).thenReturn(Mono.never());

    Tags tags = new DefaultTags().add("group", "testShouldWaitForPresence-group");
    PresenceAwareRSocket rSocket = PresenceAwareRSocket.wrap(mock, tags, presenceNotifier);

    StepVerifier.create(rSocket.requestResponse(Mockito.mock(Payload.class)))
        .expectNextCount(0)
        .thenCancel()
        .verify(Duration.ofSeconds(1));
  }
}
