package io.netifi.proteus.presence;

import io.netifi.proteus.broker.info.BrokerInfoService;
import io.netifi.proteus.broker.info.Destination;
import io.netifi.proteus.broker.info.Event;
import io.netifi.proteus.broker.info.Group;
import io.netty.buffer.ByteBuf;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.mockito.Mockito;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class BrokerInfoPresenceNotifierTest {
  @Rule public Timeout globalTimeout = Timeout.seconds(5);

  @Test
  public void testWatchGroup() throws Exception {
    BrokerInfoService client = Mockito.mock(BrokerInfoService.class);
    Destination destination =
        Destination
            .newBuilder()
            .setGroup("testGroup")
            .setDestination("testDest")
            .build();
    
    Event event = Event
                      .newBuilder()
                      .setDestination(destination)
                      .build();
    
    Mockito.when(client.streamGroupEvents(Mockito.any(Group.class), Mockito.any(ByteBuf.class)))
        .thenReturn(Flux.just(event));

    BrokerInfoPresenceNotifier notifier = new BrokerInfoPresenceNotifier(client);

    CountDownLatch latch = new CountDownLatch(1);
    notifier.joinEvents.subscribe(d -> latch.countDown());

    notifier.watch("testWatchGroup");

    latch.await();
  }

  @Test
  public void testWatchDestination() throws Exception {
    BrokerInfoService client = Mockito.mock(BrokerInfoService.class);
    Destination destination =
        Destination.newBuilder().setGroup("testGroup").setDestination("testDest").build();
    Event event = Event.newBuilder().setDestination(destination).build();
    Mockito.when(
            client.streamDestinationEvents(
                Mockito.any(Destination.class), Mockito.any(ByteBuf.class)))
        .thenReturn(Flux.just(event));

    BrokerInfoPresenceNotifier notifier = new BrokerInfoPresenceNotifier(client);

    CountDownLatch latch = new CountDownLatch(1);
    notifier.joinEvents.subscribe(d -> latch.countDown());

    notifier.watch("tesDest", "testGroup");

    latch.await();
  }

  @Test
  public void testNotifyGroup() throws Exception {
    BrokerInfoService client = Mockito.mock(BrokerInfoService.class);
    Destination destination =
        Destination.newBuilder().setGroup("testGroup").setDestination("testDest").build();
    Event event = Event.newBuilder().setDestination(destination).build();
    DirectProcessor<Event> processor = DirectProcessor.create();
    Mockito.when(client.streamGroupEvents(Mockito.any(Group.class), Mockito.any(ByteBuf.class)))
        .thenReturn(processor);

    BrokerInfoPresenceNotifier notifier = new BrokerInfoPresenceNotifier(client);
    CountDownLatch latch = new CountDownLatch(1);
    notifier.notify("testGroup").doOnSuccess(v -> latch.countDown()).subscribe();
    processor.onNext(event);
    latch.await();
  }

  @Test
  public void testNotifyDestination() throws Exception {
    BrokerInfoService client = Mockito.mock(BrokerInfoService.class);
    Event invalidEvent =
        Event.newBuilder()
            .setDestination(
                Destination.newBuilder()
                    .setGroup("testGroup")
                    .setDestination("testNotFound")
                    .build())
            .build();

    Event validEvent =
        Event.newBuilder()
            .setDestination(
                Destination.newBuilder().setGroup("testGroup").setDestination("testDest").build())
            .build();

    DirectProcessor<Event> processor = DirectProcessor.create();
    Mockito.when(
            client.streamDestinationEvents(
                Mockito.any(Destination.class), Mockito.any(ByteBuf.class)))
        .thenReturn(processor);

    BrokerInfoPresenceNotifier notifier = new BrokerInfoPresenceNotifier(client);
    CountDownLatch latch = new CountDownLatch(1);
    notifier.notify("testDest", "testGroup").doOnSuccess(v -> latch.countDown()).subscribe();
    processor.onNext(invalidEvent);
    processor.onNext(validEvent);
    latch.await();
  }

  @Test
  public void testNotifyExistingGroup() {
    BrokerInfoService client = Mockito.mock(BrokerInfoService.class);
    Mockito.when(client.streamGroupEvents(Mockito.any(Group.class), Mockito.any(ByteBuf.class)))
        .thenReturn(Flux.never());

    BrokerInfoPresenceNotifier notifier = new BrokerInfoPresenceNotifier(client);
    notifier
        .store
        .put(Destination.newBuilder().build(), "testGroup", "testDest")
        .add("group", "testGroup");
    notifier.notify("testGroup").block();
  }

  @Test
  public void testNotifyExistDestination() {
    BrokerInfoService client = Mockito.mock(BrokerInfoService.class);
    Mockito.when(
            client.streamDestinationEvents(
                Mockito.any(Destination.class), Mockito.any(ByteBuf.class)))
        .thenReturn(Flux.never());
  
    BrokerInfoPresenceNotifier notifier = new BrokerInfoPresenceNotifier(client);
    notifier
        .store
        .put(Destination.newBuilder().build(), "testGroup", "testDest");
    notifier.notify("testDest", "testGroup").block();
  }
}
