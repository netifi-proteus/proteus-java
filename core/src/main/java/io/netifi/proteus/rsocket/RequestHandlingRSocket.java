package io.netifi.proteus.rsocket;

import io.netifi.proteus.ProteusService;
import io.netifi.proteus.exception.ServiceNotFound;
import io.netifi.proteus.frames.ProteusMetadata;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.internal.SwitchTransform;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class RequestHandlingRSocket extends AbstractRSocket {
  private final ConcurrentMap<String, ProteusService> registeredServices = new ConcurrentHashMap<>();

  public RequestHandlingRSocket(ProteusService... services) {
    for (ProteusService proteusService : services) {
      String service = proteusService.getService();
      registeredServices.put(service, proteusService);
    }
  }

  public void addService(ProteusService proteusService) {
    String service = proteusService.getService();
    registeredServices.put(service, proteusService);
  }

  private ProteusService getService(String service) {
    return registeredServices.get(service);
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    try {
      ByteBuf metadata = Unpooled.wrappedBuffer(payload.getMetadata());
      String service = ProteusMetadata.getService(metadata);

      ProteusService proteusService = getService(service);

      if (proteusService == null) {
        return Mono.error(new ServiceNotFound(service));
      }

      return proteusService.fireAndForget(payload);

    } catch (Throwable t) {
      return Mono.error(t);
    }
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    try {
      ByteBuf metadata = Unpooled.wrappedBuffer(payload.getMetadata());
      String service = ProteusMetadata.getService(metadata);

      ProteusService proteusService = getService(service);

      if (proteusService == null) {
        return Mono.error(new ServiceNotFound(service));
      }

      return proteusService.requestResponse(payload);

    } catch (Throwable t) {
      return Mono.error(t);
    }
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    try {
      ByteBuf metadata = Unpooled.wrappedBuffer(payload.getMetadata());
      String service = ProteusMetadata.getService(metadata);

      ProteusService proteusService = getService(service);

      if (proteusService == null) {
        return Flux.error(new ServiceNotFound(service));
      }

      return proteusService.requestStream(payload);

    } catch (Throwable t) {
      return Flux.error(t);
    }
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    return new SwitchTransform<>(
        payloads,
        (payload, flux) -> {
          ByteBuf metadata = Unpooled.wrappedBuffer(payload.getMetadata());
          String service = ProteusMetadata.getService(metadata);

          ProteusService proteusService = getService(service);

          if (proteusService == null) {
            return Flux.error(new ServiceNotFound(service));
          }

          return proteusService.requestChannel(payload, flux);
        });
  }
}
