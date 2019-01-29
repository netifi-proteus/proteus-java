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
package io.netifi.proteus.discovery;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

public class StaticListDiscoveryConfigTest {

  @Before
  public void clearProperties() {
    System.clearProperty(
        StaticListDiscoveryConfig.DISCOVERY_CONFIG_SYSTEM_PROPERTY_STATIC_LIST_ADDRESSES);
    System.clearProperty(
        StaticListDiscoveryConfig.DISCOVERY_CONFIG_SYSTEM_PROPERTY_STATIC_LIST_PORT);
  }

  @Test
  public void withDiscoveryStrategy() {
    DiscoveryStrategy discoveryStrategy =
        DiscoveryStrategy.getInstance(new StaticListDiscoveryConfig());
    assertEquals(discoveryStrategy.getClass(), StaticListDiscoveryStrategy.class);
  }

  @Test
  public void propertyAddressConfig() {
    System.setProperty(
        StaticListDiscoveryConfig.DISCOVERY_CONFIG_SYSTEM_PROPERTY_STATIC_LIST_ADDRESSES,
        "1.1.1.1,2.2.2.2,3.3.3.3");
    StaticListDiscoveryConfig staticListDiscoveryConfig = new StaticListDiscoveryConfig();
    List<String> addresses = staticListDiscoveryConfig.getAddresses();
    assertTrue(addresses.stream().anyMatch("1.1.1.1"::equals));
    assertTrue(addresses.stream().anyMatch("2.2.2.2"::equals));
    assertTrue(addresses.stream().anyMatch("3.3.3.3"::equals));
    assertEquals(3, addresses.size());
    assertEquals(7001, staticListDiscoveryConfig.getPort());
  }

  @Test
  public void variadicAddressConfig() {
    StaticListDiscoveryConfig staticListDiscoveryConfig =
        new StaticListDiscoveryConfig(9000, "1.1.1.1", "2.2.2.2", "3.3.3.3");
    List<String> addresses = staticListDiscoveryConfig.getAddresses();
    assertTrue(addresses.stream().anyMatch("1.1.1.1"::equals));
    assertTrue(addresses.stream().anyMatch("2.2.2.2"::equals));
    assertTrue(addresses.stream().anyMatch("3.3.3.3"::equals));
    assertEquals(3, addresses.size());
    assertEquals(9000, staticListDiscoveryConfig.getPort());
  }

  @Test
  public void stringListAddressConfig() {
    StaticListDiscoveryConfig staticListDiscoveryConfig =
        new StaticListDiscoveryConfig(9000, "1.1.1.1,2.2.2.2,3.3.3.3");
    List<String> addresses = staticListDiscoveryConfig.getAddresses();
    assertTrue(addresses.stream().anyMatch("1.1.1.1"::equals));
    assertTrue(addresses.stream().anyMatch("2.2.2.2"::equals));
    assertTrue(addresses.stream().anyMatch("3.3.3.3"::equals));
    assertEquals(3, addresses.size());
    assertEquals(9000, staticListDiscoveryConfig.getPort());
  }

  @Test
  public void listAddressConfig() {
    List<String> hostList = Arrays.asList("1.1.1.1", "2.2.2.2", "3.3.3.3");
    StaticListDiscoveryConfig staticListDiscoveryConfig =
        new StaticListDiscoveryConfig(9000, hostList);
    List<String> addresses = staticListDiscoveryConfig.getAddresses();
    assertTrue(addresses.stream().anyMatch("1.1.1.1"::equals));
    assertTrue(addresses.stream().anyMatch("2.2.2.2"::equals));
    assertTrue(addresses.stream().anyMatch("3.3.3.3"::equals));
    assertEquals(3, addresses.size());
    assertEquals(9000, staticListDiscoveryConfig.getPort());
  }

  @Test
  public void providedEmptyList() {
    List<String> address = new StaticListDiscoveryConfig(7001, "").getAddresses();
    assertEquals(Collections.emptyList(), address);
  }
}
