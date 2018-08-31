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

public class TracesStreamer {

  private final ObjectMapper objectMapper;
  private Mono<InputStream> inputSource;

  public TracesStreamer(String zipkinUrl,
                        Mono<HttpClient> client) {
    this(zipkinServerStream(zipkinUrl, client));
  }

  public TracesStreamer(Mono<InputStream> inputSource) {
    this.inputSource = inputSource;
    this.objectMapper = protoMapper();
  }

  public Flux<Trace> streamTraces() {
    return streamTraces(inputSource);
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
    }).flatMapIterable(Traces::getTracesList);
  }

  private static Mono<InputStream> zipkinServerStream(String zipkinUrl,
                                                      Mono<HttpClient> client) {
    return client
        .flatMap(c -> c
            .get(zipkinUrl + "?lookback=1728000000&limit=10000")
            .flatMap(resp ->
                resp.receive()
                    .aggregate()
                    .asInputStream()));
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
