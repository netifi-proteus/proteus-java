package io.netifi.proteus;

import io.netifi.proteus.frames.ClientSetupFlyweight;
import io.netifi.proteus.tags.DefaultTags;
import io.netifi.proteus.tags.Tags;
import io.netifi.proteus.tags.TagsCodec;
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
    Tags tags = new DefaultTags();
    tags.add("group", "foo");
    tags.add("destination", "bar");
    ByteBuf fromTags = TagsCodec.encode(ByteBufAllocator.DEFAULT, tags);

    PooledByteBufAllocator alloc = nonCachingAllocator();

    for (int i = 0; i < 100000; i++) {
      Payload payload = DefaultProteusBrokerService.getSetupPayload(alloc, 123L, token, fromTags);
    }
    Assert.assertEquals(0, directBuffersCount(alloc));
    Assert.assertEquals(0, heapBuffersCount(alloc));
  }

  @Test
  public void setupDecodeTest() {
    String expectedToken = "token";
    String expectedGroup = "foo";
    String expectedDest = "bar";
    long expectedKey = 123L;

    ByteBuf token = Unpooled.wrappedBuffer(expectedToken.getBytes(Charset.defaultCharset()));

    Tags tags = new DefaultTags();
    tags.add("group", expectedGroup);
    tags.add("destination", expectedDest);
    ByteBuf fromTags = TagsCodec.encode(ByteBufAllocator.DEFAULT, tags);

    Payload payload =
        DefaultProteusBrokerService.getSetupPayload(
            ByteBufAllocator.DEFAULT, expectedKey, token, fromTags);
    ByteBuf metadata = payload.sliceMetadata();
    Tags actualTags = TagsCodec.decode(ClientSetupFlyweight.tags(metadata));
    String actualGroup = actualTags.get("group").toString();
    String actualDest = actualTags.get("destination").toString();
    long actualAccessKey = ClientSetupFlyweight.accessKey(metadata);
    String actualAccessToken =
        ClientSetupFlyweight.accessToken(metadata).toString(Charset.defaultCharset());

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
