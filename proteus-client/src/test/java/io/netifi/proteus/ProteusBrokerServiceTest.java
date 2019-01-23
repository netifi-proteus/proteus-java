package io.netifi.proteus;

import io.netifi.proteus.frames.DestinationSetupFlyweight;
import io.netty.buffer.*;
import io.rsocket.Payload;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.function.Function;
import org.junit.*;

public class ProteusBrokerServiceTest {

  @Test
  public void setupPayloadLeakTest() {
    ByteBuf token = Unpooled.wrappedBuffer("token".getBytes(Charset.defaultCharset()));
    PooledByteBufAllocator alloc = nonCachingAllocator();

    for (int i = 0; i < 100000; i++) {
      Payload payload =
          DefaultProteusBrokerService.getSetupPayload(alloc, "foo", "bar", 123L, token);
      payload.release();
    }
    Assert.assertEquals(0, directBuffersCount(alloc));
    Assert.assertEquals(0, heapBuffersCount(alloc));
  }

  @Test
  public void setupDecodeTest() {
    String expectedToken = "token";
    String expectedDest = "foo";
    String expectedGroup = "bar";
    long expectedKey = 123L;

    ByteBuf token = Unpooled.wrappedBuffer(expectedToken.getBytes(Charset.defaultCharset()));

    Payload payload =
        DefaultProteusBrokerService.getSetupPayload(
            ByteBufAllocator.DEFAULT, expectedDest, expectedGroup, expectedKey, token);
    ByteBuf metadata = payload.sliceMetadata();
    String actualDest = DestinationSetupFlyweight.destination(metadata);
    String actualGroup = DestinationSetupFlyweight.group(metadata);
    long actualAccessKey = DestinationSetupFlyweight.accessKey(metadata);
    String actualAccessToken =
        DestinationSetupFlyweight.accessToken(metadata).toString(Charset.defaultCharset());

    Assert.assertEquals(expectedToken, actualAccessToken);
    Assert.assertEquals(expectedDest, actualDest);
    Assert.assertEquals(expectedGroup, actualGroup);
    Assert.assertEquals(expectedKey, actualAccessKey);
  }

  private static PooledByteBufAllocator nonCachingAllocator() {
    return new PooledByteBufAllocator(true, 1, 1, 8192, 11, 0, 0, 0);
  }

  private static long directBuffersCount(PooledByteBufAllocator alloc) {
    return count(alloc, PooledByteBufAllocator::directArenas);
  }

  private static long heapBuffersCount(PooledByteBufAllocator alloc) {
    return count(alloc, PooledByteBufAllocator::heapArenas);
  }

  private static long count(
      PooledByteBufAllocator alloc,
      Function<PooledByteBufAllocator, Collection<PoolArenaMetric>> f) {
    return f.apply(alloc).stream().mapToLong(PoolArenaMetric::numActiveAllocations).sum();
  }
}
