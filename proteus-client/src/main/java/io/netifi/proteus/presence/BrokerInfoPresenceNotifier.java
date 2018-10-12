package io.netifi.proteus.presence;

import io.netifi.proteus.broker.info.BrokerInfoService;
import io.netifi.proteus.broker.info.Destination;
import io.netifi.proteus.broker.info.Event;
import io.netifi.proteus.tags.*;
import io.netty.buffer.Unpooled;
import java.time.Duration;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

public class BrokerInfoPresenceNotifier implements PresenceNotifier {
  private static final Logger logger = LoggerFactory.getLogger(BrokerInfoPresenceNotifier.class);
  IndexableMap<String, Destination> destinations;

  private BrokerInfoService client;

  public BrokerInfoPresenceNotifier(BrokerInfoService client) {
    this.destinations = IndexableMapWrapper.wrap(new ConcurrentHashMap<>());
    this.client = client;
  }

  @Override
  public Disposable watch(Tags tags) {
    Objects.requireNonNull(tags);

    io.netifi.proteus.broker.info.Tags.Builder builder =
        io.netifi.proteus.broker.info.Tags.newBuilder();
    for (Entry<CharSequence, CharSequence> entry : tags) {
      builder.putTags(entry.getKey().toString(), entry.getValue().toString());
    }
    return client
        .streamDestinationEvents(builder.build(), Unpooled.EMPTY_BUFFER)
        // .doFinally(
        //    s -> {
        //      synchronized (BrokerInfoPresenceNotifier.class) {
        //        List<String> strings = new ArrayList<>(groups.row(group).keySet());
        //        for (String d : strings) {
        //          remove(group, d);
        //        }
        //      }
        //    })
        .onErrorResume(err -> Mono.delay(Duration.ofMillis(500)).then(Mono.error(err)))
        .retry()
        .subscribe(this::joinEvent);
  }

  private void joinEvent(Event event) {
    Destination destination = event.getDestination();
    logger.info("presence notifier received event {}", event.toString());
    switch (event.getType()) {
      case JOIN:
        Tags tags =
            TagsCodec.decode(Unpooled.wrappedBuffer(destination.getTags().asReadOnlyByteBuffer()));
        destinations.put(destination.getDestinationId(), destination, tags);
        break;
      case LEAVE:
        destinations.remove(destination.getDestinationId());
        break;
      default:
        throw new IllegalStateException("unknown event type " + event.getType());
    }
  }

  @Override
  public Mono<Void> notify(Tags tags) {
    Objects.requireNonNull(tags);

    return Mono.defer(
        () -> {
          if (destinations.contains(tags)) {
            return Mono.empty();
          } else {
            return destinations.events(tags).next().then();
          }
        });
  }
}
