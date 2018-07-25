package io.netifi.proteus.frames;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

public class BroadcastServiceFlyweightTest {
    @Test
    public void testEncodeAndDecode() {
        ByteBuf metadata = Unpooled.wrappedBuffer("metadata".getBytes());
        ByteBuf byteBuf =
            BroadcastServiceFlyweight.encode(
                ByteBufAllocator.DEFAULT, "汉字", "fromGroup", "service", metadata);
        
        Assert.assertEquals(FrameType.BROADCAST_SERVICE, FrameHeaderFlyweight.frameType(byteBuf));
        Assert.assertEquals("汉字", BroadcastServiceFlyweight.fromDestination(byteBuf));
        Assert.assertEquals("fromGroup", BroadcastServiceFlyweight.fromGroup(byteBuf));
        Assert.assertEquals("service", BroadcastServiceFlyweight.service(byteBuf));
        metadata.resetReaderIndex();
        Assert.assertTrue(ByteBufUtil.equals(metadata, BroadcastServiceFlyweight.metadata(byteBuf)));
    }
}