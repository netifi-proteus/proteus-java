package io.netifi.proteus.rpc.blocking;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netifi.proteus.BlockingIterable;
import io.netifi.proteus.rpc.SimpleRequest;
import io.netifi.proteus.rpc.SimpleResponse;
import io.netifi.proteus.rsocket.RequestHandlingRSocket;
import io.netty.buffer.ByteBuf;
import io.rsocket.Frame;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import reactor.core.publisher.Mono;

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class SimpleServiceTest {

  private static SimpleMeterRegistry registry = new SimpleMeterRegistry();
  private static RSocket rSocket;

  @BeforeClass
  public static void setup() {
    SimpleServiceServer serviceServer =
        new SimpleServiceServer(new DefaultSimpleService(), registry);

    RSocketFactory.receive()
        .frameDecoder(Frame::retain)
        .acceptor((setup, sendingSocket) -> Mono.just(new RequestHandlingRSocket(serviceServer)))
        .transport(TcpServerTransport.create(8801))
        .start()
        .block();

    rSocket =
        RSocketFactory.connect()
            .frameDecoder(Frame::retain)
            .transport(TcpClientTransport.create(8801))
            .start()
            .block();
  }

  @Test
  public void testRequestReply() {
    SimpleServiceClient client = new SimpleServiceClient(rSocket, registry);
    SimpleResponse response =
        client.requestReply(
            SimpleRequest.newBuilder().setRequestMessage("sending a message").build());

    String responseMessage = response.getResponseMessage();

    System.out.println(responseMessage);

    Assert.assertEquals("sending a message", responseMessage);
  }

  @Test(timeout = 50_000)
  public void testStreaming() {
    SimpleServiceClient client = new SimpleServiceClient(rSocket, registry);

    BlockingIterable<SimpleResponse> stream =
        client.requestStream(
            SimpleRequest.newBuilder().setRequestMessage("sending a message").build());

    SimpleResponse response = stream.stream().limit(5).findAny().get();

    String responseMessage = response.getResponseMessage();
    System.out.println(responseMessage);
  }

  @Test(timeout = 50_000)
  public void testStreamingPrintEach() {
    SimpleServiceClient client = new SimpleServiceClient(rSocket, registry);
    client
        .requestStream(SimpleRequest.newBuilder().setRequestMessage("sending a message").build())
        .stream()
        .limit(5)
        .forEach(simpleResponse -> System.out.println(simpleResponse.getResponseMessage()));
  }

  @Test(timeout = 30_000)
  public void testClientStreamingRpc() throws Exception {
    SimpleServiceClient client = new SimpleServiceClient(rSocket, registry);
    Queue<SimpleRequest> strings = new LinkedBlockingQueue<>();

    ForkJoinTask<String> submit =
        ForkJoinPool.commonPool()
            .submit(
                new Callable<String>() {
                  @Override
                  public String call() throws Exception {

                    SimpleResponse response = client.streamingRequestSingleResponse(strings);

                    return response.getResponseMessage();
                  }
                });

    for (int i = 0; i < 11; i++) {
      strings.add(SimpleRequest.newBuilder().setRequestMessage("sending -> " + i).build());
    }

    System.out.println(submit.get());
  }

  @Test(timeout = 150_000)
  public void testBidiStreamingRpc() throws Exception {
    SimpleServiceClient client = new SimpleServiceClient(rSocket, registry);
    Queue<SimpleRequest> strings = new LinkedBlockingQueue<>();

    ForkJoinTask<String> submit =
        ForkJoinPool.commonPool()
            .submit(
                new Callable<String>() {
                  @Override
                  public String call() throws Exception {

                    return client
                        .streamingRequestAndResponse(strings)
                        .stream()
                        .limit(1_000)
                        .reduce((simpleResponse, simpleResponse2) -> simpleResponse2)
                        .get()
                        .getResponseMessage();
                  }
                });

    for (int i = 0; i < 1_000; i++) {
      strings.add(SimpleRequest.newBuilder().setRequestMessage("sending -> " + i).build());
    }

    System.out.println(submit.get());
  }

  @Test
  public void testFireAndForget() throws Exception {
    int count = 1000;
    SimpleServiceClient client = new SimpleServiceClient(rSocket, registry);

    for (int i = 0; i < count; i++) {
      client.fireAndForget(SimpleRequest.newBuilder().setRequestMessage("fire -> " + i).build());
    }
  }

  static class DefaultSimpleService implements SimpleService {
    @Override
    public SimpleResponse requestReply(SimpleRequest message, ByteBuf metadata) {
      return SimpleResponse.newBuilder()
          .setResponseMessageBytes(message.getRequestMessageBytes())
          .build();
    }

    @Override
    public void fireAndForget(SimpleRequest message, ByteBuf metadata) {
      System.out.println("got message -> " + message.getRequestMessage());
    }

    @Override
    public Iterable<SimpleResponse> requestStream(SimpleRequest message, ByteBuf metadata) {
      String requestMessage = message.getRequestMessage();
      AtomicLong count = new AtomicLong();
      return new Iterable<SimpleResponse>() {
        @Override
        public Iterator<SimpleResponse> iterator() {
          return new Iterator<SimpleResponse>() {
            @Override
            public boolean hasNext() {
              return true;
            }

            @Override
            public SimpleResponse next() {
              return SimpleResponse.newBuilder()
                  .setResponseMessage(
                      count.incrementAndGet() + " - got message - " + requestMessage)
                  .build();
            }
          };
        }
      };
    }

    @Override
    public SimpleResponse streamingRequestSingleResponse(
        Iterable<SimpleRequest> messages, ByteBuf metadata) {
      ConcurrentHashMap<Character, AtomicInteger> map = new ConcurrentHashMap<>();
      Iterator<SimpleRequest> iterator = messages.iterator();
      for (int i = 0; i < 10; i++) {
        SimpleRequest s = iterator.next();
        char[] chars = s.getRequestMessage().toCharArray();
        for (char c : chars) {
          map.computeIfAbsent(c, _c -> new AtomicInteger()).incrementAndGet();
        }
      }
      StringBuilder builder = new StringBuilder();
      map.forEach(
          (character, atomicInteger) -> {
            builder
                .append("character -> ")
                .append(character)
                .append(", count -> ")
                .append(atomicInteger.get())
                .append("\n");
          });

      String s = builder.toString();

      return SimpleResponse.newBuilder().setResponseMessage(s).build();
    }

    @Override
    public Iterable<SimpleResponse> streamingRequestAndResponse(
        Iterable<SimpleRequest> messages, ByteBuf metadata) {
      Iterator<SimpleRequest> iterator = messages.iterator();
      return new Iterable<SimpleResponse>() {
        @Override
        public Iterator<SimpleResponse> iterator() {
          return new Iterator<SimpleResponse>() {
            @Override
            public boolean hasNext() {
              return iterator.hasNext();
            }

            @Override
            public SimpleResponse next() {
              SimpleRequest next = iterator.next();
              return requestReply(next, metadata);
            }
          };
        }
      };
    }
  }
}
