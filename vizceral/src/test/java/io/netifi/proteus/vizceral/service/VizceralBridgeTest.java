package io.netifi.proteus.vizceral.service;

import com.google.protobuf.Empty;
import io.netifi.proteus.tracing.TracesStreamer;
import io.netifi.proteus.viz.Connection;
import io.netifi.proteus.viz.Metrics;
import io.netifi.proteus.viz.Node;
import io.netifi.proteus.viz.VisualisationRequest;
import io.netty.buffer.Unpooled;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Mono;

import java.io.InputStream;
import java.time.Duration;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class VizceralBridgeTest {

  private VizceralBridge vizceralBridge;

  @Before
  public void setUp() {
    vizceralBridge = new VizceralBridge(
        req -> new TracesStreamer(zipkinMockSource())
            .streamTraces(req.getLookbackSeconds()));
  }

  @Test
  public void vizSource() {

    VisualisationRequest vizRequest =
        VisualisationRequest
            .newBuilder()
            .setDataLookbackSeconds(42)
            .build();

    Node root = vizceralBridge
        .visualisations(vizRequest, Unpooled.EMPTY_BUFFER)
        .blockFirst(Duration.ofSeconds(5));

    assertNotNull(root);

    List<Connection> connectionsList = root.getConnectionsList();
    assertNotNull(connectionsList);
    assertEquals(1, connectionsList.size());

    Connection conn = connectionsList.iterator().next();
    assertEquals(
        "quickstart.clients-client1-io.netifi.proteus.quickstart.service.HelloService",
        conn.getSource());
    assertEquals(
        "quickstart.services.helloservices-helloservice-3fa7b9dc-7afd-4767-a781-b7265a9fa02d-io.netifi.proteus.quickstart.service.HelloService",
        conn.getTarget());

    Metrics metrics = conn.getMetrics();
    assertEquals(1.0d, metrics.getNormal(), 1e-7);
    assertEquals(0.0d, metrics.getDanger(), 1e-7);

    List<Node> nodeList = root.getNodesList();
    assertNotNull(nodeList);
    assertEquals(2, nodeList.size());
  }

  private Mono<InputStream> zipkinMockSource() {
    return Mono.fromCallable(() ->
        getClass().getClassLoader().getResourceAsStream("zipkin.json"));
  }
}
