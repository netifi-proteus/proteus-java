package io.netifi.proteus.frames;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

public class BroadcastServiceGroupFlyweightTest {
    
    @Test
    public void testEncodeAndDecode() {
        ByteBuf metadata = Unpooled.wrappedBuffer("metadata".getBytes());
        ByteBuf byteBuf =
            BroadcastServiceGroupFlyweight.encode(
                ByteBufAllocator.DEFAULT, "汉字", "fromGroup", "service", "toGroup", metadata);
        
        Assert.assertEquals(FrameType.BROADCAST_SERVICE_GROUP, FrameHeaderFlyweight.frameType(byteBuf));
        Assert.assertEquals("汉字", BroadcastServiceGroupFlyweight.fromDestination(byteBuf));
        Assert.assertEquals("fromGroup", BroadcastServiceGroupFlyweight.fromGroup(byteBuf));
        Assert.assertEquals("service", BroadcastServiceGroupFlyweight.service(byteBuf));
        Assert.assertEquals("toGroup", BroadcastServiceGroupFlyweight.toGroup(byteBuf));
        metadata.resetReaderIndex();
        Assert.assertTrue(ByteBufUtil.equals(metadata, BroadcastServiceGroupFlyweight.metadata(byteBuf)));
    }
}