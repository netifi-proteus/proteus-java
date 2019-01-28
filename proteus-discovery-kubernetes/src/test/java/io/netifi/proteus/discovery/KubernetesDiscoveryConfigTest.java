package io.netifi.proteus.discovery;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

public class KubernetesDiscoveryConfigTest {

  @Before
  public void clearProperties() {
    System.clearProperty(
        KubernetesDiscoveryConfig.DISCOVERY_CONFIG_SYSTEM_PROPERTY_KUBERNETES_NAMESPACE);
    System.clearProperty(
        KubernetesDiscoveryConfig.DISCOVERY_CONFIG_SYSTEM_PROPERTY_KUBERNETES_DEPLOYMENT_NAME);
    System.clearProperty(
        KubernetesDiscoveryConfig.DISCOVERY_CONFIG_SYSTEM_PROPERTY_KUBERNETES_PORT_NAME);
  }

  @Test
  public void propertyConfig() {
    String testNamespace = "namespace";
    String testDeploymentName = "foobar";
    String testPortName = "cluster-magic";

    System.setProperty(
        KubernetesDiscoveryConfig.DISCOVERY_CONFIG_SYSTEM_PROPERTY_KUBERNETES_NAMESPACE,
        testNamespace);
    System.setProperty(
        KubernetesDiscoveryConfig.DISCOVERY_CONFIG_SYSTEM_PROPERTY_KUBERNETES_DEPLOYMENT_NAME,
        testDeploymentName);
    System.setProperty(
        KubernetesDiscoveryConfig.DISCOVERY_CONFIG_SYSTEM_PROPERTY_KUBERNETES_PORT_NAME,
        testPortName);

    KubernetesDiscoveryConfig kubernetesDiscoveryConfig = new KubernetesDiscoveryConfig();
    assertEquals(testNamespace, kubernetesDiscoveryConfig.getNamespace());
    assertEquals(testDeploymentName, kubernetesDiscoveryConfig.getDeploymentName());
    assertEquals(testPortName, kubernetesDiscoveryConfig.getPortName());
  }

  @Test
  public void overrideConfig() {
    String testNamespace = "namespace";
    String testOverrideNamespace = "magic-ops";
    String testDeploymentName = "foobar";
    String testOverrideDeploymentName = "netifi-best-cluster";
    String testPortName = "cluster-magic";
    String testOverridePortName = "secret-port";

    System.setProperty(
        KubernetesDiscoveryConfig.DISCOVERY_CONFIG_SYSTEM_PROPERTY_KUBERNETES_NAMESPACE,
        testOverrideNamespace);
    System.setProperty(
        KubernetesDiscoveryConfig.DISCOVERY_CONFIG_SYSTEM_PROPERTY_KUBERNETES_DEPLOYMENT_NAME,
        testOverrideDeploymentName);
    System.setProperty(
        KubernetesDiscoveryConfig.DISCOVERY_CONFIG_SYSTEM_PROPERTY_KUBERNETES_PORT_NAME,
        testOverridePortName);

    KubernetesDiscoveryConfig kubernetesDiscoveryConfig =
        new KubernetesDiscoveryConfig(testNamespace, testDeploymentName, testPortName);
    assertEquals(testOverrideNamespace, kubernetesDiscoveryConfig.getNamespace());
    assertEquals(testOverrideDeploymentName, kubernetesDiscoveryConfig.getDeploymentName());
    assertEquals(testOverridePortName, kubernetesDiscoveryConfig.getPortName());
  }

  @Test
  public void defaultConfig() {
    KubernetesDiscoveryConfig kubernetesDiscoveryConfig = new KubernetesDiscoveryConfig();
    assertEquals(
        KubernetesDiscoveryConfig.DISCOVERY_CONFIG_SYSTEM_PROPERTY_KUBERNETES_DEFAULT_NAMESPACE,
        kubernetesDiscoveryConfig.getNamespace());
    assertEquals(
        KubernetesDiscoveryConfig
            .DISCOVERY_CONFIG_SYSTEM_PROPERTY_KUBERNETES_DEFAULT_DEPLOYMENT_NAME,
        kubernetesDiscoveryConfig.getDeploymentName());
    assertEquals(
        KubernetesDiscoveryConfig.DISCOVERY_CONFIG_SYSTEM_PROPERTY_KUBERNETES_DEFAULT_PORT_NAME,
        kubernetesDiscoveryConfig.getPortName());
  }
}
