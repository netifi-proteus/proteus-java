package io.netifi.proteus.admin.tracing;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.netifi.proteus.admin.om.Node;
import reactor.core.publisher.Flux;

public interface AdminTraceService {
  Flux<Node> streamData();

  default Flux<String> streamDataJson() {
    return streamData()
        .map(
            node -> {
              String s;
              Node.Builder builder = node.toBuilder();
              try {
                s = JsonFormat.printer().print(builder);
              } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
              }
              return s;
            });
  }
}
