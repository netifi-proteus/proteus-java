package io.netifi.proteus.presence;

import io.netifi.proteus.balancer.LoadBalancedRSocketSupplier;
import io.netifi.proteus.frames.DestinationAvailResult;
import io.netifi.proteus.rs.SecureRSocket;
import io.netifi.proteus.util.TimebasedIdGenerator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.rsocket.util.ByteBufPayload;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;
import reactor.test.StepVerifier;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultPresenceNotifierTest {
  private static TimebasedIdGenerator idGenerator = new TimebasedIdGenerator(1);

  @Test
  public void testWatchGroup() {
    SecureRSocket mock = Mockito.mock(SecureRSocket.class);
    LoadBalancedRSocketSupplier supplier = Mockito.mock(LoadBalancedRSocketSupplier.class);
    Mockito.when(supplier.get()).thenReturn(mock);
    Mockito.when(supplier.onClose()).thenReturn(Mono.never());

    UnicastProcessor processor = UnicastProcessor.create();
    Mockito.when(mock.requestStream(Mockito.any())).thenReturn(processor);

    DefaultPresenceNotifier notifier =
        new DefaultPresenceNotifier(idGenerator, Long.MAX_VALUE, "test", supplier);

    notifier.watch(Long.MAX_VALUE, "testWatchGroup-group");

    int length = DestinationAvailResult.computeLength("someDest");
    ByteBuf metadata = ByteBufAllocator.DEFAULT.buffer(length);
    DestinationAvailResult.encode(metadata, "someDest", true, idGenerator.nextId());

    processor.onNext(ByteBufPayload.create(Unpooled.EMPTY_BUFFER, metadata));

    Assert.assertTrue(notifier.contains(Long.MAX_VALUE, "testWatchGroup-group"));

    length = DestinationAvailResult.computeLength("someDest");
    metadata = ByteBufAllocator.DEFAULT.buffer(length);
    DestinationAvailResult.encode(metadata, "someDest", false, idGenerator.nextId());

    processor.onNext(ByteBufPayload.create(Unpooled.EMPTY_BUFFER, metadata));

    Assert.assertFalse(notifier.contains(Long.MAX_VALUE, "testWatchGroup-group"));
  }

  @Test
  public void testWatchDestination() {
    SecureRSocket mock = Mockito.mock(SecureRSocket.class);
    LoadBalancedRSocketSupplier supplier = Mockito.mock(LoadBalancedRSocketSupplier.class);
    Mockito.when(supplier.get()).thenReturn(mock);
    Mockito.when(supplier.onClose()).thenReturn(Mono.never());

    UnicastProcessor processor = UnicastProcessor.create();
    Mockito.when(mock.requestStream(Mockito.any())).thenReturn(processor);

    DefaultPresenceNotifier notifier =
        new DefaultPresenceNotifier(idGenerator, Long.MAX_VALUE, "test", supplier);

    notifier.watch(Long.MAX_VALUE, "testWatchDestination-dest", "testWatchDestination-group");

    int length = DestinationAvailResult.computeLength("testWatchDestination-dest");
    ByteBuf metadata = ByteBufAllocator.DEFAULT.buffer(length);
    DestinationAvailResult.encode(
        metadata, "testWatchDestination-dest", true, idGenerator.nextId());

    processor.onNext(ByteBufPayload.create(Unpooled.EMPTY_BUFFER, metadata));

    Assert.assertTrue(
        notifier.contains(
            Long.MAX_VALUE, "testWatchDestination-dest", "testWatchDestination-group"));

    length = DestinationAvailResult.computeLength("testWatchDestination-dest");
    metadata = ByteBufAllocator.DEFAULT.buffer(length);
    DestinationAvailResult.encode(
        metadata, "testWatchDestination-dest", false, idGenerator.nextId());

    processor.onNext(ByteBufPayload.create(Unpooled.EMPTY_BUFFER, metadata));
  
    Assert.assertFalse(
        notifier.contains(
            Long.MAX_VALUE, "testWatchDestination-dest", "testWatchDestination-group"));
  
  }

  @Test
  public void testNotifyGroup() throws Exception {
    SecureRSocket mock = Mockito.mock(SecureRSocket.class);
    LoadBalancedRSocketSupplier supplier = Mockito.mock(LoadBalancedRSocketSupplier.class);
    Mockito.when(supplier.get()).thenReturn(mock);
    Mockito.when(supplier.onClose()).thenReturn(Mono.never());

    UnicastProcessor processor = UnicastProcessor.create();
    Mockito.when(mock.requestStream(Mockito.any())).thenReturn(processor);

    DefaultPresenceNotifier notifier =
        new DefaultPresenceNotifier(idGenerator, Long.MAX_VALUE, "test", supplier);

    StepVerifier.create(notifier.notify(Long.MAX_VALUE, "testNotifyGroup-group"))
        .then(
            () -> {
              int length = DestinationAvailResult.computeLength("testNotifyGroup-dest");
              ByteBuf metadata = ByteBufAllocator.DEFAULT.buffer(length);
              DestinationAvailResult.encode(
                  metadata, "testNotifyGroup-dest", true, idGenerator.nextId());

              processor.onNext(ByteBufPayload.create(Unpooled.EMPTY_BUFFER, metadata));
            })
        .verifyComplete();
  }

  @Test
  public void testNotifyDestination() throws Exception {
    SecureRSocket mock = Mockito.mock(SecureRSocket.class);
    LoadBalancedRSocketSupplier supplier = Mockito.mock(LoadBalancedRSocketSupplier.class);
    Mockito.when(supplier.get()).thenReturn(mock);
    Mockito.when(supplier.onClose()).thenReturn(Mono.never());

    UnicastProcessor processor = UnicastProcessor.create();
    Mockito.when(mock.requestStream(Mockito.any())).thenReturn(processor);

    DefaultPresenceNotifier notifier =
        new DefaultPresenceNotifier(idGenerator, Long.MAX_VALUE, "test", supplier);

    StepVerifier.create(
            notifier.notify(
                Long.MAX_VALUE, "testNotifyDestination-dest", "testNotifyDestination-group"))
        .then(
            () -> {
              int length = DestinationAvailResult.computeLength("testNotifyDestination-dest");
              ByteBuf metadata = ByteBufAllocator.DEFAULT.buffer(length);
              DestinationAvailResult.encode(
                  metadata, "testNotifyDestination-dest", true, idGenerator.nextId());

              processor.onNext(ByteBufPayload.create(Unpooled.EMPTY_BUFFER, metadata));
            })
        .verifyComplete();
  }

  @Test
  public void testNotifiExistingGroup() throws Exception {
    SecureRSocket mock = Mockito.mock(SecureRSocket.class);
    LoadBalancedRSocketSupplier supplier = Mockito.mock(LoadBalancedRSocketSupplier.class);
    Mockito.when(supplier.get()).thenReturn(mock);
    Mockito.when(supplier.onClose()).thenReturn(Mono.never());

    UnicastProcessor processor = UnicastProcessor.create();
    Mockito.when(mock.requestStream(Mockito.any())).thenReturn(processor);

    DefaultPresenceNotifier notifier =
        new DefaultPresenceNotifier(idGenerator, Long.MAX_VALUE, "test", supplier);

    ConcurrentHashMap<String, Set<String>> groupMap = notifier.getGroupMap(Long.MAX_VALUE);
    groupMap.put("testNotifyGroup-group", ConcurrentHashMap.newKeySet());

    Mono<Void> notify = notifier.notify(Long.MAX_VALUE, "testNotifyGroup-group");
    Assert.assertSame(Mono.empty(), notify);
  }

  @Test
  public void testNotifyExistDestination() throws Exception {
    SecureRSocket mock = Mockito.mock(SecureRSocket.class);
    LoadBalancedRSocketSupplier supplier = Mockito.mock(LoadBalancedRSocketSupplier.class);
    Mockito.when(supplier.get()).thenReturn(mock);
    Mockito.when(supplier.onClose()).thenReturn(Mono.never());

    UnicastProcessor processor = UnicastProcessor.create();
    Mockito.when(mock.requestStream(Mockito.any())).thenReturn(processor);

    DefaultPresenceNotifier notifier =
        new DefaultPresenceNotifier(idGenerator, Long.MAX_VALUE, "test", supplier);

    ConcurrentHashMap<String, Set<String>> groupMap = notifier.getGroupMap(Long.MAX_VALUE);
    Set<String> destinations =
        groupMap.computeIfAbsent("testNotifyDestination-group", k -> ConcurrentHashMap.newKeySet());
    destinations.add("testNotifyDestination-dest");

    Mono<Void> notify =
        notifier.notify(
            Long.MAX_VALUE, "testNotifyDestination-dest", "testNotifyDestination-group");
    Assert.assertSame(Mono.empty(), notify);
  }
}
