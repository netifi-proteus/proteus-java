package io.netifi.proteus.rsocket;

import io.netifi.proteus.ProteusService;
import io.netifi.proteus.exception.ServiceNotFound;
import io.netifi.proteus.frames.ProteusMetadata;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.internal.SwitchTransform;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class RequestHandlingRSocket extends AbstractRSocket {
  private final ConcurrentMap<String, ProteusService> registeredServices;

  public RequestHandlingRSocket(ConcurrentMap<String, ProteusService> registeredServices) {
    this.registeredServices = registeredServices;
  }

  public RequestHandlingRSocket(ProteusService... services) {
    registeredServices = new ConcurrentHashMap<>();

    for (ProteusService proteusService : services) {
      String service = proteusService.getService();
      registeredServices.put(service, proteusService);
    }
  }

  public void addService(ProteusService proteusService) {
    String service = proteusService.getService();
    registeredServices.put(service, proteusService);
  }
    
    public ConcurrentMap<String, ProteusService> getRegisteredServices() {
        return registeredServices;
    }
    
    @Override
  public Mono<Void> fireAndForget(Payload payload) {
    try {
      ByteBuf metadata = payload.sliceMetadata();
      String service = ProteusMetadata.getService(metadata);

      ProteusService proteusService = registeredServices.get(service);

      if (proteusService == null) {
        ReferenceCountUtil.safeRelease(payload);
        return Mono.error(new ServiceNotFound(service));
      }

      return proteusService.fireAndForget(payload);
    } catch (Throwable t) {
      ReferenceCountUtil.safeRelease(payload);
      return Mono.error(t);
    }
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    try {
      ByteBuf metadata = payload.sliceMetadata();
      String service = ProteusMetadata.getService(metadata);

      ProteusService proteusService = registeredServices.get(service);

      if (proteusService == null) {
        ReferenceCountUtil.safeRelease(payload);
        return Mono.error(new ServiceNotFound(service));
      }

      return proteusService.requestResponse(payload);
    } catch (Throwable t) {
      ReferenceCountUtil.safeRelease(payload);
      return Mono.error(t);
    }
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    try {
      ByteBuf metadata = payload.sliceMetadata();
      String service = ProteusMetadata.getService(metadata);

      ProteusService proteusService = registeredServices.get(service);

      if (proteusService == null) {
        ReferenceCountUtil.safeRelease(payload);
        return Flux.error(new ServiceNotFound(service));
      }

      return proteusService.requestStream(payload);
    } catch (Throwable t) {
      ReferenceCountUtil.safeRelease(payload);
      return Flux.error(t);
    }
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    return new SwitchTransform<>(
        payloads,
        (payload, flux) -> {
          try {
            ByteBuf metadata = payload.sliceMetadata();
            String service = ProteusMetadata.getService(metadata);

            ProteusService proteusService = registeredServices.get(service);

            if (proteusService == null) {
              ReferenceCountUtil.safeRelease(payload);
              return Flux.error(new ServiceNotFound(service));
            }

            return proteusService.requestChannel(payload, flux);
          } catch (Throwable t) {
            ReferenceCountUtil.safeRelease(payload);
            return Flux.error(t);
          }
        });
  }
}
