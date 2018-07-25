package io.netifi.proteus.frames;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

public class ServiceDestinationFlyweightTest {
    @Test
    public void testEncodeAndDecode() {
        ByteBuf metadata = Unpooled.wrappedBuffer("metadata".getBytes());
        ByteBuf byteBuf =
            ServiceDestinationFlyweight.encode(
                ByteBufAllocator.DEFAULT,
                "汉字",
                "fromGroup",
                "service",
                "toDestination",
                "toGroup",
                metadata);
        
        Assert.assertEquals(FrameType.SERVICE_DESTINATION, FrameHeaderFlyweight.frameType(byteBuf));
        Assert.assertEquals("汉字", ServiceDestinationFlyweight.fromDestination(byteBuf));
        Assert.assertEquals("fromGroup", ServiceDestinationFlyweight.fromGroup(byteBuf));
        Assert.assertEquals("service", ServiceDestinationFlyweight.service(byteBuf));
        Assert.assertEquals("toDestination", ServiceDestinationFlyweight.toDestination(byteBuf));
        Assert.assertEquals("toGroup", ServiceDestinationFlyweight.toGroup(byteBuf));
        metadata.resetReaderIndex();
        Assert.assertTrue(ByteBufUtil.equals(metadata, ServiceDestinationFlyweight.metadata(byteBuf)));
    }
}