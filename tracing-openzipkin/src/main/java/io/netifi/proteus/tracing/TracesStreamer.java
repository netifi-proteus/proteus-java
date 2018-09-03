package io.netifi.proteus.tracing;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleDeserializers;
import com.hubspot.jackson.datatype.protobuf.ProtobufModule;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.http.client.HttpClient;
import zipkin2.proto3.Span;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class TracesStreamer {

  private final ObjectMapper objectMapper = protoMapper();
  private Function<Integer, Mono<InputStream>> inputSource;

  public TracesStreamer(String zipkinUrl,
                        Mono<HttpClient> client) {
    this.inputSource = zipkinServerStream(zipkinUrl, client);
  }

  public TracesStreamer(Mono<InputStream> inputSource) {
    this.inputSource = v -> inputSource;
  }

  public Flux<Trace> streamTraces(int lookbackSeconds) {
    return streamTraces(inputSource.apply(lookbackSeconds));
  }

  Flux<Trace> streamTraces(Mono<InputStream> input) {
    return input.map(is -> {
      try {
        return objectMapper.<Traces>readValue(
            is,
            new TypeReference<Traces>() {
            });
      } catch (IOException e) {
        throw Exceptions.propagate(e);
      }
    }).flatMapIterable(Traces::getTracesList)
        ;
  }

  private static Function<Integer, Mono<InputStream>> zipkinServerStream(String zipkinUrl,
                                                                         Mono<HttpClient> client) {
    return lookbackSeconds -> client
        .flatMap(c -> c
            .get(zipkinQuery(zipkinUrl, lookbackSeconds))
            .flatMap(resp ->
                resp.receive()
                    .aggregate()
                    .asInputStream()));
  }

  private static String zipkinQuery(String zipkinUrl, int lookbackSeconds) {
    long lookbackMicros = TimeUnit.SECONDS.toMicros(lookbackSeconds);
    return zipkinUrl + "?lookback=" + lookbackMicros + "&limit=100000";
  }

  private ObjectMapper protoMapper() {
    ObjectMapper mapper = new ObjectMapper();
    ProtobufModule module = new CustomProtoModule();
    mapper.registerModule(module);
    return mapper;
  }

  public static class CustomProtoModule extends ProtobufModule {
    @Override
    public void setupModule(SetupContext context) {
      super.setupModule(context);
      SimpleDeserializers deser = new SimpleDeserializers();
      deser.addDeserializer(Traces.class, new TracersDeserializer());
      context.addDeserializers(deser);
    }
  }

  public static class TracersDeserializer extends StdDeserializer<Traces> {

    public TracersDeserializer() {
      this(null);
    }

    protected TracersDeserializer(Class<?> vc) {
      super(vc);
    }

    @Override
    public Traces deserialize(JsonParser p,
                              DeserializationContext ctxt) throws IOException {
      p.nextToken();
      Traces.Builder tracesBuilder = Traces.newBuilder();
      while (p.currentToken() != JsonToken.END_ARRAY) {
        tracesBuilder.addTraces(nextTrace(p, ctxt));
      }
      p.nextToken();
      return tracesBuilder.build();
    }

    private Trace nextTrace(JsonParser p,
                            DeserializationContext ctxt) throws IOException {
      Trace.Builder traceBuilder = Trace.newBuilder();
      p.nextToken();
      while (p.currentToken() != JsonToken.END_ARRAY) {
        traceBuilder.addSpans(ctxt.readValue(p, Span.class));
        p.nextToken();
      }
      p.nextToken();
      return traceBuilder.build();
    }
  }
}
