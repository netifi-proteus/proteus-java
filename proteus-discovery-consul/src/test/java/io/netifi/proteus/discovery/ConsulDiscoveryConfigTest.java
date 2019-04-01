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

import static org.junit.Assert.assertEquals;

import java.net.MalformedURLException;
import java.net.URL;
import org.junit.Before;
import org.junit.Test;

public class ConsulDiscoveryConfigTest {

  @Before
  public void clearProperties() {
    System.clearProperty(ConsulDiscoveryConfig.DISCOVERY_CONFIG_SYSTEM_PROPERTY_CONSUL_URL);
    System.clearProperty(
        ConsulDiscoveryConfig.DISCOVERY_CONFIG_SYSTEM_PROPERTY_CONSUL_SERVICE_NAME);
  }

  @Test
  public void propertyConfig() throws MalformedURLException {
    URL testURL = new URL("http://foobar.com:9090");
    String testServiceName = "foobar";

    System.setProperty(
        ConsulDiscoveryConfig.DISCOVERY_CONFIG_SYSTEM_PROPERTY_CONSUL_URL, testURL.toString());
    System.setProperty(
        ConsulDiscoveryConfig.DISCOVERY_CONFIG_SYSTEM_PROPERTY_CONSUL_SERVICE_NAME,
        testServiceName);
    ConsulDiscoveryConfig consulDiscoveryConfig = new ConsulDiscoveryConfig();
    assertEquals(testURL, consulDiscoveryConfig.getConsulURL());
    assertEquals(testServiceName, consulDiscoveryConfig.getServiceName());
  }

  @Test
  public void normalConfig() throws MalformedURLException {
    URL testURL = new URL("http://foobar.com:9090");
    String testServiceName = "foobar";

    ConsulDiscoveryConfig consulDiscoveryConfig =
        new ConsulDiscoveryConfig(testURL, testServiceName);
    assertEquals(testURL, consulDiscoveryConfig.getConsulURL());
    assertEquals(testServiceName, consulDiscoveryConfig.getServiceName());
  }

  @Test
  public void overrideConfig() throws MalformedURLException {
    URL testURL = new URL("http://foobar.com:9090");
    URL testOverrideURL = new URL("http://google.com");
    String testServiceName = "foobar";
    String testOverrideServiceName = "binbaz";

    System.setProperty(
        ConsulDiscoveryConfig.DISCOVERY_CONFIG_SYSTEM_PROPERTY_CONSUL_URL,
        testOverrideURL.toString());
    System.setProperty(
        ConsulDiscoveryConfig.DISCOVERY_CONFIG_SYSTEM_PROPERTY_CONSUL_SERVICE_NAME,
        testOverrideServiceName);
    ConsulDiscoveryConfig consulDiscoveryConfig =
        new ConsulDiscoveryConfig(testURL, testServiceName);
    assertEquals(testOverrideURL, consulDiscoveryConfig.getConsulURL());
    assertEquals(testOverrideServiceName, consulDiscoveryConfig.getServiceName());
  }

  @Test
  public void defaultConfig() {
    ConsulDiscoveryConfig consulDiscoveryConfig = new ConsulDiscoveryConfig();
    assertEquals(
        ConsulDiscoveryConfig.DISCOVERY_CONFIG_SYSTEM_PROPERTY_CONSUL_DEFAULT_URL,
        consulDiscoveryConfig.getConsulURL());
    assertEquals(
        ConsulDiscoveryConfig.DISCOVERY_CONFIG_SYSTEM_PROPERTY_CONSUL_DEFAULT_SERVICE_NAME,
        consulDiscoveryConfig.getServiceName());
  }
}
