/*
 *    Copyright 2019 The Proteus Authors
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package io.netifi.proteus;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

@Ignore // can't reload system properties
public class DefaultBuilderConfigTest {
  @Test
  public void testShouldFindSingleSeedAddress() {
    System.setProperty("proteus.client.seedAddresses", "localhost:8001");
    List<SocketAddress> seedAddress = DefaultBuilderConfig.getSeedAddress();
    Assert.assertNotNull(seedAddress);
    Assert.assertEquals(1, seedAddress.size());

    InetSocketAddress address = (InetSocketAddress) seedAddress.get(0);
    Assert.assertEquals(8001, address.getPort());
  }

  @Test
  public void testShouldFindMultipleSeedAddresses() {
    System.setProperty(
        "proteus.client.seedAddresses", "localhost:8001,localhost:8002,localhost:8003");
    List<SocketAddress> seedAddress = DefaultBuilderConfig.getSeedAddress();
    Assert.assertNotNull(seedAddress);
    Assert.assertEquals(3, seedAddress.size());

    InetSocketAddress address = (InetSocketAddress) seedAddress.get(0);
    Assert.assertEquals(8001, address.getPort());
  }

  @Test(expected = IllegalStateException.class)
  public void testShouldThrowExceptionForAddressMissingPort() {
    System.setProperty("proteus.client.seedAddresses", "localhost:8001,localhost,localhost:8003");
    List<SocketAddress> seedAddress = DefaultBuilderConfig.getSeedAddress();
  }

  @Test(expected = IllegalStateException.class)
  public void testShouldThrowExceptionForInvalidAddress() {
    System.setProperty("proteus.client.seedAddresses", "no way im valid");
    List<SocketAddress> seedAddress = DefaultBuilderConfig.getSeedAddress();
  }

  @Test
  public void testShouldReturnNull() {
    List<SocketAddress> seedAddress = DefaultBuilderConfig.getSeedAddress();

    Assert.assertNull(seedAddress);
  }
}
