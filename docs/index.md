#![image](images/proteus.png) Netifi Proteus Java
## Introduction
Proteus is an easy to use RPC layer that sits on top of [RSocket](http://rsocket.io). RSocket is a binary protocol for use on byte stream transports such as TCP, WebSockets, and [Aeron](https://github.com/real-logic/aeron). Proteus is able to leverage RSockets unique features allowing you develop complex realtime, streaming applications without needing Kafka, Spark, etc. It is completely non-blocking and reactive making it responsive and high performance.

### Protobuf
Proteus use [Protobuf 3](https://developers.google.com/protocol-buffers/docs/proto3) as its IDL. Protobuf works well with RSocket because they are both binary. Using Protobuf makes it easy to switch from gRPC. You only need to replace the gRPC compiler with the Proteus compiler to start using Proteus.

### Full Duplex
Proteus is built with a full duplex protocol- RSocket. An application that initiates a connection (traditionally a client) can actually serve requests from the application it is connecting with (traditionally the server).  In other words a client can connect to a server, and the server can call a Proteus service located on the client. The server does not have to wait for the client to initiate a call, only the connection is necessary.

### Reactive Streams Compatible
Proteus's Java implementation is built using a Reactive Streams compliant library [Reactor-core](https://github.com/reactor/reactor-core). Proteus generated code returns either a Flux which is a stream of many, or a Mono which is a stream of one. Proteus is compatible with any [Reactive Streams](http://www.reactive-streams.org/) implementation including RXJava 2, and Akka Streams. 

### Back-pressure
Proteus respects both RSocket and Reactive Stream back-pressure. Back-pressure is where the consumer of a stream sends stream's producer a message indicating how many messages it can handle. This makes Proteus services very reliable, and  not be overwhelmed with demand.

## Interaction Models
Proteus has five interaction models. They are modeled using a Protobuf 3 IDL. This section details the interaction models, and gives an example Protobuf IDL definition. The interaction models are request/response, request/stream, fire-and-forget, streaming request/one response, and streaming request/streaming response.

### Request / Response
Request response is analogous to a HTTP Rest call. A major difference is because this is non-blocking the caller can wait for the response for a long time without blocking other requests on the same connection.

#### Protobuf
```
rpc RequestReply (SimpleRequest) returns (SimpleResponse) {}
```

### Fire-and-Forget
Fire and forget sends a request without a response. This is not just ignoring the response; the underlying protocol does not send anything back to the caller. To make a request fire-and-forget with Proteus you need to return google.Protobuf.Empty in your IDL. This will generate fire and forget code.

#### Protobuf
```
import "google/Protobuf/empty.proto";
...
rpc FireAndForget (SimpleRequest) returns (google.Protobuf.Empty) {}
```

### Single Request / Stream Response
Sends a single request and then receives multiple responses. This can be used to model subscriptions to topics and queues. It can be also used to do things like page from a database. To generate this mark the response with the stream keyword.

#### Protobuf
```
rpc RequestStream (SimpleRequest) returns (stream SimpleResponse) {}
```

### Streaming Request / Single Response
Sends a stream of requests, and receives a single response.  This can be used to model transactions. You can send multiple requests over a logic channel, and then receive a single response if they were processed correctly or not. Mark the request with the stream keyword to create this interaction model.


#### Protobuf
```
rpc StreamingRequestSingleResponse (stream SimpleRequest) returns (SimpleResponse) {}
```

### Streaming Request / Streaming Response
Sends request stream and returns response stream. This models a fully duplex interaction. Mark the request and response with the stream keyword to create this interaction model.

#### Protobuf
```
rpc StreamingRequestAndResponse (stream SimpleRequest) returns (stream SimpleResponse) {}
```

## Getting Started
### Prerequisites
* JDK 1.8
* Gradle 2.x
* Protobuf Compiler 3.x

#### Installing Protobuf
##### Mac OS
For Mac users you can easily install the Protobuf compiler using Homebrew:
```
$ brew install Protobuf
```
##### Ubuntu
Ubuntu users can install Protobuf using apt-get:
```
$ sudo apt-get install libProtobuf-java Protobuf-compiler
```

### Configuring Gradle
Proteus Java uses a Protobuf plugin to generate application code. Add the following code to your project's Gradle file so that it will generate code from your Protobuf IDL when your application is compiled.

```
Protobuf {
    protoc {
        artifact = 'com.google.Protobuf:protoc:3.6.1'
    }
    plugins {
        rsocketRpc {
            artifact = 'io.rsocket.rpc:rsocket-rpc-protobuf:0.2.x'
        }
    }
    generateProtoTasks {
        all()*.plugins {
            rsocketRpc {}
        }
    }
}

// If you use Intellij add this so it can find the generated classes
idea {
    module {
        sourceDirs += file("${projectDir}/build/generated/source/rsocketRpc/main/java");
        sourceDirs += file("${projectDir}/build/generated/source/rsocketRpc/main/proteus");
        sourceDirs += file("${projectDir}/build/generated/source/rsocketRpc/test/java");
        sourceDirs += file("${projectDir}/build/generated/source/rsocketRpc/test/proteus");
    }
}
```

### Define a Protobuf
After you have installed Protobuf and configured Gradle you need to create a Protobuf IDL to define your service. The following is a simple Protobuf example that defines all the interaction models:
```
syntax = "proto3";

package io.netifi.testing;

import "google/Protobuf/empty.proto";

option java_package = "io.netifi.testing.Protobuf";
option java_outer_classname = "SimpleServiceProto";
option java_multiple_files = true;

service SimpleService {
  // Request / Response
  rpc RequestReply (SimpleRequest) returns (SimpleResponse) {}

  // Fire-and-Forget
  rpc FireAndForget (SimpleRequest) returns (google.Protobuf.Empty) {}

  // Single Request / Streaming Response
  rpc RequestStream (SimpleRequest) returns (stream SimpleResponse) {}

  // Streaming Request / Single Response
  rpc StreamingRequestSingleResponse (stream SimpleRequest) returns (SimpleResponse) {}

  // Streaming Request / Streaming Response
  rpc StreamingRequestAndResponse (stream SimpleRequest) returns (stream SimpleResponse) {}
}

message SimpleRequest {
  string requestMessage = 1;
}

message SimpleResponse {
  string responseMessage = 1;
}
```

## Generate Files
After you have configured your Protobuf IDL, use the Protobuf compiler via the Gradle plugin to generate your files:
```
$ gradlew generateProto
```

The Protobuf compiler will generate Protobuf builders for the message, and then use the Proteus compiler to generate an interface for the service defined in the IDL. It will also generate a client that implements the interface, and a server takes an implementation of the interface. This is what servers the requests. 

Using the above IDL as an example Proteus will generate several class from the above example, but the ones that you need to concern yourself are:
* SimpleRequest
* SimpleResponse
* SimpleService
* SimpleServiceClient
* SimpleServiceServer

### Proteus IDL Example
Here is what the simple `SimpleService` is actually and interface, and represent the service contract from the IDL. The `SimpleService` interface needs to be implemented in order to handle requests. Here is an example implementation:
```
class DefaultSimpleService implements SimpleService {
    @Override
    public Mono<Void> fireAndForget(SimpleRequest message) {
      System.out.println("got message -> " + message.getRequestMessage());
      return Mono.empty();
    }
    
    @Override
    public Mono<SimpleResponse> requestReply(SimpleRequest message) {
      return Mono.fromCallable(
          () ->
              SimpleResponse.newBuilder()
                  .setResponseMessage("we got the message -> " + message.getRequestMessage())
                  .build());
    }
    
    @Override
    public Mono<SimpleResponse> streamingRequestSingleResponse(Publisher<SimpleRequest> messages) {
      return Flux.from(messages)
          .windowTimeout(10, Duration.ofSeconds(500))
          .take(1)
          .flatMap(Function.identity())
          .reduce(
              new ConcurrentHashMap<Character, AtomicInteger>(),
              (map, s) -> {
                char[] chars = s.getRequestMessage().toCharArray();
                for (char c : chars) {
                  map.computeIfAbsent(c, _c -> new AtomicInteger()).incrementAndGet();
                }
    
                return map;
              })
          .map(
              map -> {
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
              });
    }
    
    @Override
    public Flux<SimpleResponse> requestStream(SimpleRequest message) {
      String requestMessage = message.getRequestMessage();
      return Flux.interval(Duration.ofMillis(200))
          .onBackpressureDrop()
          .map(i -> i + " - got message - " + requestMessage)
          .map(s -> SimpleResponse.newBuilder().setResponseMessage(s).build());
    }
    
    @Override
    public Flux<SimpleResponse> streamingRequestAndResponse(Publisher<SimpleRequest> messages) {
      return Flux.from(messages).flatMap(this::requestReply);
    }
}
```

### Proteus Server Configuration
Each generated service has a client and server implementation generated for you. After you have implemented the generated interface you need to hand the implementation to the  server. See the below example:
```
SimpleServiceServer serviceServer = new SimpleServiceServer(new DefaultSimpleService());
```

Once you have created an instance of the the server you need to configure RSocket. The server can either be configured the RSocket that receives the connection or the RSocket that makes the connection. The following examples uses TCP, but can use other transports as well. The example also uses the optional `RequestHandlingRSocket`. The `RequestHandlingRSocket` is a special RSocket that will allow you to support more than one Proteus Server implementation on the same RSocket connection. If you don't need to support more than one socket you don't need to use it. Just return the generated server directly.

#### RSocket Server Configuration
This configures the receiver of a connection, typically a server, to handle requests to the `SimeplService` implementation.
```
SimpleServiceServer serviceServer = new SimpleServiceServer(new DefaultSimpleService());
  
    RSocketFactory.receive()
        .acceptor(
            (setup, sendingSocket) ->
                Mono.just(new RequestHandlingRSocket(serviceServer)))
        .transport(TcpServerTransport.create(8801))
        .start()
        .block();
```

#### RSocket Client Configuration
This configures a initiator of a connection, typically a client, to handle requests to the `SimpleService` implementation.
```
SimpleServiceServer serviceServer = new SimpleServiceServer(new DefaultSimpleService());

RSocketFactory
        .connect()
        .acceptor(rSocket -> new RequestHandlingRSocket(serviceServer))
        .transport(TcpClientTransport.create(8801))
        .start()
        .block();
```

### Proteus Client Configuration
The Proteus compiler generates a client as well as a server. The client implements the generated interface. You can configure the client either from an RSocket client connection, or server connection.

#### RSocket Server Configuration
This configures the receiver of a connection, typically a server, to call the remote  `SimpleService`. Notice that the client is created inside the closure in the acceptor method. The method passes in a variable called `sendingSocket`. This is the RSocket that is the connection to the client. You  can make calls to the client *without* receiving requests first, or ever.
```
RSocketFactory.receive()
        .acceptor(
            (setup, sendingSocket) -> {
              SimpleServiceClient client = new SimpleServiceClient(sendingSocket);

              // Trivial example - this could also be an Proteus service.
              return Mono.just(
                  new AbstractRSocket() {
                    @Override
                    public Mono<Void> fireAndForget(Payload payload) {
                      return client.fireAndForget(
                          SimpleRequest.newBuilder()
                              .setRequestMessage(payload.getDataUtf8())
                              .build());
                    }
                  });
            })
        .transport(TcpServerTransport.create(8801))
        .start()
        .block();
```

#### RSocket Client Configuration
This configures an initiator of a connection, typically a client, to call the remote  `SimpleService`.
```
RSocket rSocket = RSocketFactory.connect().transport(TcpClientTransport.create(8801)).start().block();

SimpleServiceClient client = new SimpleServiceClient(rSocket);

```

### Calling the client
One the client is created, it can be called like any other method. Here is an example call the `streamingRequestSingleResponse` method.
```
SimpleServiceClient client = new SimpleServiceClient(rSocket);

Flux<SimpleRequest> requests =
    Flux.range(1, 11)
        .map(i -> "sending -> " + i)
        .map(s -> SimpleRequest.newBuilder().setRequestMessage(s).build());

SimpleResponse response = client.streamingRequestSingleResponse(requests).block();

System.out.println(response.getResponseMessage());
```

The above example streams in 11 items to a server. The server receives the stream, counts the most common words, and then returns a message detailing the data received. 

## Working example
To see a working example of the code described here please view the [SimpleServiceTest](https://github.com/netifi/proteus-java/blob/master/testing-proto/src/test/java/io/netifi/testing/protobuf/SimpleServiceTest.java) class.


## Release Notes

Please find release notes at [https://github.com/netifi/proteus-java/releases](https://github.com/netifi/proteus-java/releases).

## Bugs and Feedback

For bugs, questions, and discussions please use the [Github Issues](https://github.com/netifi/proteus-java/issues).

## License
Copyright 2017 Netifi Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
