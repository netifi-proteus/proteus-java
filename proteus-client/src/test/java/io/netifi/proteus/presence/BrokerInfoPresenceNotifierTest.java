package io.netifi.proteus.presence;

import com.google.protobuf.UnsafeByteOperations;
import io.netifi.proteus.broker.info.*;
import io.netifi.proteus.tags.DefaultTags;
import io.netifi.proteus.tags.Tags;
import io.netifi.proteus.tags.TagsCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.util.concurrent.CountDownLatch;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.mockito.Mockito;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;

public class BrokerInfoPresenceNotifierTest {
  @Rule public Timeout globalTimeout = Timeout.seconds(5);

  @Test
  public void testWatchGroup() throws Exception {
    BrokerInfoService brokerInfo = Mockito.mock(BrokerInfoService.class);

    Tags tags = new DefaultTags().add("group", "testGroup").add("destination", "testDest");
    ByteBuf out = TagsCodec.encode(ByteBufAllocator.DEFAULT, tags);
    Client client =
        Client.newBuilder().setTags(UnsafeByteOperations.unsafeWrap(out.nioBuffer())).build();
    Event event = Event.newBuilder().setClient(client).build();
    Mockito.when(
            brokerInfo.streamClientEvents(
                Mockito.any(io.netifi.proteus.broker.info.Tags.class), Mockito.any(ByteBuf.class)))
        .thenReturn(Flux.just(event));

    BrokerInfoPresenceNotifier notifier = new BrokerInfoPresenceNotifier(brokerInfo);

    CountDownLatch latch = new CountDownLatch(1);
    notifier.connections.events().subscribe(d -> latch.countDown());

    Tags watchTags = new DefaultTags().add("group", "testGroup");
    notifier.watch(watchTags);

    latch.await();
  }

  @Test
  public void testWatchDestination() throws Exception {
    BrokerInfoService brokerInfo = Mockito.mock(BrokerInfoService.class);

    Tags tags = new DefaultTags().add("group", "testGroup").add("destination", "testDest");
    ByteBuf out = TagsCodec.encode(ByteBufAllocator.DEFAULT, tags);
    Client client =
        Client.newBuilder().setTags(UnsafeByteOperations.unsafeWrap(out.nioBuffer())).build();
    Event event = Event.newBuilder().setClient(client).build();
    Mockito.when(
            brokerInfo.streamClientEvents(
                Mockito.any(io.netifi.proteus.broker.info.Tags.class), Mockito.any(ByteBuf.class)))
        .thenReturn(Flux.just(event));

    BrokerInfoPresenceNotifier notifier = new BrokerInfoPresenceNotifier(brokerInfo);

    CountDownLatch latch = new CountDownLatch(1);
    notifier.connections.events().subscribe(d -> latch.countDown());

    Tags watchTags = new DefaultTags().add("group", "testGroup").add("destination", "testDest");
    notifier.watch(watchTags);

    latch.await();
  }

  @Test
  public void testNotifyGroup() throws Exception {
    BrokerInfoService brokerInfo = Mockito.mock(BrokerInfoService.class);

    Tags tags = new DefaultTags().add("group", "testGroup").add("destination", "testDest");
    ByteBuf out = TagsCodec.encode(ByteBufAllocator.DEFAULT, tags);
    Client client =
        Client.newBuilder().setTags(UnsafeByteOperations.unsafeWrap(out.nioBuffer())).build();
    Event event = Event.newBuilder().setClient(client).build();
    DirectProcessor<Event> processor = DirectProcessor.create();
    Mockito.when(
            brokerInfo.streamClientEvents(
                Mockito.any(io.netifi.proteus.broker.info.Tags.class), Mockito.any(ByteBuf.class)))
        .thenReturn(processor);

    BrokerInfoPresenceNotifier notifier = new BrokerInfoPresenceNotifier(brokerInfo);
    CountDownLatch latch = new CountDownLatch(1);

    Tags watchTags = new DefaultTags().add("group", "testGroup");
    notifier.watch(watchTags);
    notifier.notify(watchTags).doOnSuccess(v -> latch.countDown()).subscribe();
    processor.onNext(event);
    latch.await();
  }

  @Test
  public void testNotifyDestination() throws Exception {
    BrokerInfoService client = Mockito.mock(BrokerInfoService.class);
    Tags invalidTags =
        new DefaultTags().add("group", "testGroup").add("destination", "testNotFound");
    ByteBuf invalidTagsOut = TagsCodec.encode(ByteBufAllocator.DEFAULT, invalidTags);
    Event invalidEvent =
        Event.newBuilder()
            .setClient(
                Client.newBuilder()
                    .setTags(UnsafeByteOperations.unsafeWrap(invalidTagsOut.nioBuffer()))
                    .build())
            .build();

    Tags validTags = new DefaultTags().add("group", "testGroup").add("destination", "testDest");
    ByteBuf validTagsOut = TagsCodec.encode(ByteBufAllocator.DEFAULT, validTags);
    Event validEvent =
        Event.newBuilder()
            .setClient(
                Client.newBuilder()
                    .setTags(UnsafeByteOperations.unsafeWrap(validTagsOut.nioBuffer()))
                    .build())
            .build();

    DirectProcessor<Event> processor = DirectProcessor.create();
    Mockito.when(
            client.streamClientEvents(
                Mockito.any(io.netifi.proteus.broker.info.Tags.class), Mockito.any(ByteBuf.class)))
        .thenReturn(processor);

    BrokerInfoPresenceNotifier notifier = new BrokerInfoPresenceNotifier(client);
    CountDownLatch latch = new CountDownLatch(1);

    Tags watchTags = new DefaultTags().add("group", "testGroup").add("destination", "testDest");
    notifier.watch(watchTags);
    notifier.notify(watchTags).doOnSuccess(v -> latch.countDown()).subscribe();
    processor.onNext(invalidEvent);
    processor.onNext(validEvent);
    latch.await();
  }

  @Test
  public void testNotifyExistingGroup() {
    BrokerInfoService client = Mockito.mock(BrokerInfoService.class);
    Mockito.when(
            client.streamClientEvents(
                Mockito.any(io.netifi.proteus.broker.info.Tags.class), Mockito.any(ByteBuf.class)))
        .thenReturn(Flux.never());

    Tags tags = new DefaultTags().add("group", "testGroup").add("destination", "testDest");
    BrokerInfoPresenceNotifier notifier = new BrokerInfoPresenceNotifier(client);
    notifier.connections.put(123, 456, Client.newBuilder().build(), tags);

    Tags watchTags = new DefaultTags().add("group", "testGroup");
    notifier.watch(watchTags);
    notifier.notify(watchTags).block();
  }

  @Test
  public void testNotifyExistDestination() {
    BrokerInfoService client = Mockito.mock(BrokerInfoService.class);
    Mockito.when(
            client.streamClientEvents(
                Mockito.any(io.netifi.proteus.broker.info.Tags.class), Mockito.any(ByteBuf.class)))
        .thenReturn(Flux.never());

    Tags tags = new DefaultTags().add("group", "testGroup").add("destination", "testDest");
    BrokerInfoPresenceNotifier notifier = new BrokerInfoPresenceNotifier(client);
    notifier.connections.put(123, 456, Client.newBuilder().build(), tags);

    Tags watchTags = new DefaultTags().add("group", "testGroup").add("destination", "testDest");
    notifier.watch(watchTags);
    notifier.notify(watchTags).block();
  }
}
