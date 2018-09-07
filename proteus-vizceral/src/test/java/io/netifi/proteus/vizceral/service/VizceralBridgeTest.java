package io.netifi.proteus.vizceral.service;

import static org.junit.Assert.*;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import io.netifi.proteus.tracing.TracesStreamer;
import io.netifi.proteus.vizceral.*;
import io.netty.buffer.Unpooled;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

public class VizceralBridgeTest {

  @Test
  public void vizSource() {

    VizceralBridge vizceralBridge =
        new VizceralBridge(
            req -> new TracesStreamer(zipkinSource()).streamTraces(req.getLookbackSeconds()));

    VisualisationRequest vizRequest =
        VisualisationRequest.newBuilder().setDataLookbackSeconds(42).build();

    Node root =
        vizceralBridge
            .visualisations(vizRequest, Unpooled.EMPTY_BUFFER)
            .blockFirst(Duration.ofSeconds(5));

    assertNotNull(root);

    List<Connection> connectionsList = root.getConnectionsList();
    assertNotNull(connectionsList);
    assertEquals(1, connectionsList.size());

    Connection conn = connectionsList.iterator().next();
    assertEquals("quickstart.clients-client1", conn.getSource());
    assertEquals(
        "quickstart.services.helloservices-helloservice-f0ada6e3-60fa-42b0-b6fd-e5e065bed989",
        conn.getTarget());

    Metrics metrics = conn.getMetrics();
    assertEquals(1.0d, metrics.getNormal(), 1e-7);
    assertEquals(0.0d, metrics.getDanger(), 1e-7);

    List<Notice> services = conn.getNoticesList();
    assertEquals(1, services.size());
    Notice notice = services.iterator().next();
    String title = notice.getTitle();
    assertEquals("io.netifi.proteus.quickstart.service.HelloService", title);

    List<Node> nodeList = root.getNodesList();
    assertNotNull(nodeList);
    assertEquals(2, nodeList.size());

    Predicate<Node> hasRequester =
        node ->
            "quickstart.clients-client1".equals(node.getName())
                && node.getConnectionsList().isEmpty();

    Predicate<Node> hasResponder =
        node ->
            "quickstart.services.helloservices-helloservice-f0ada6e3-60fa-42b0-b6fd-e5e065bed989"
                    .equals(node.getName())
                && node.getConnectionsList().isEmpty();

    assertTrue(allMatch(nodeList, hasRequester, hasResponder));
  }

  @Test
  public void vizSourceError() {

    VizceralBridge vizceralBridge =
        new VizceralBridge(
            req -> new TracesStreamer(errorSource()).streamTraces(req.getLookbackSeconds()));

    VisualisationRequest vizRequest =
        VisualisationRequest.newBuilder().setDataLookbackSeconds(42).build();

    StepVerifier.create(vizceralBridge.visualisations(vizRequest, Unpooled.EMPTY_BUFFER))
        .expectErrorMatches(
            err ->
                err instanceof IllegalStateException
                    && "Error reading traces source stream".equals(err.getMessage())
                    && err.getCause() instanceof RuntimeException)
        .verify(Duration.ofSeconds(5));
  }

  private static <T> boolean oneMatches(Collection<T> col, Predicate<T> assertion) {
    for (T t : col) {
      if (assertion.test(t)) {
        return true;
      }
    }
    return false;
  }

  @SafeVarargs
  private static <T> boolean allMatch(Collection<T> col, Predicate<T>... assertions) {
    for (Predicate<T> assertion : assertions) {
      if (!oneMatches(col, assertion)) {
        return false;
      }
    }
    return true;
  }

  private Flux<InputStream> errorSource() {
    return Flux.error(new RuntimeException());
  }

  private Flux<InputStream> zipkinSource() {
    return Flux.create(
        sink -> {
          try (Reader reader =
              new InputStreamReader(
                  getClass().getClassLoader().getResourceAsStream("zipkin.json"), "UTF-8") {}) {
            JsonFactory f = new MappingJsonFactory();
            JsonParser jp = f.createParser(reader);
            jp.nextToken();
            while (jp.nextToken() != JsonToken.END_ARRAY) {
              String trace = jp.readValueAsTree().toString();
              sink.next(new ByteArrayInputStream(trace.getBytes(StandardCharsets.UTF_8)));
            }
            sink.complete();
          } catch (Exception e) {
            sink.error(e);
          }
        });
  }
}
