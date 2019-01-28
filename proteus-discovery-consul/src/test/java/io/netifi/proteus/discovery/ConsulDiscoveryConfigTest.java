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
