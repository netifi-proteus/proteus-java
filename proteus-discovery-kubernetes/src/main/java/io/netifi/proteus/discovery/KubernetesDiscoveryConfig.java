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

public class KubernetesDiscoveryConfig implements DiscoveryConfig {
  public static final String DISCOVERY_CONFIG_SYSTEM_PROPERTY_KUBERNETES_NAMESPACE =
      "netifi.proteus.discovery.kubernetes.namespace";
  public static final String DISCOVERY_CONFIG_SYSTEM_PROPERTY_KUBERNETES_DEPLOYMENT_NAME =
      "netifi.proteus.discovery.kubernetes.deploymentName";
  public static final String DISCOVERY_CONFIG_SYSTEM_PROPERTY_KUBERNETES_PORT_NAME =
      "netifi.proteus.discovery.kubernetes.portName";
  public static final String DISCOVERY_CONFIG_SYSTEM_PROPERTY_KUBERNETES_DEFAULT_NAMESPACE =
      "default";
  public static final String DISCOVERY_CONFIG_SYSTEM_PROPERTY_KUBERNETES_DEFAULT_DEPLOYMENT_NAME =
      "netifi-proteus-broker";
  public static final String DISCOVERY_CONFIG_SYSTEM_PROPERTY_KUBERNETES_DEFAULT_PORT_NAME =
      "cluster";
  public static final int DISCOVERY_CONFIG_SYSTEM_PROPERTY_KUBERNETES_DEFAULT_PORT = 7001;
  public static final String DISCOVERY_CONFIG_SYSTEM_PROPERTY_KUBERNETES_DEFAULT_PROTOCOL = "tcp";

  private final String namespace;
  private final String deploymentName;
  private final String portName;

  public KubernetesDiscoveryConfig(String namespace, String deploymentName, String portName) {
    this.namespace = defaultNamespaceProvider(namespace);
    this.deploymentName = defaultDeploymentNameProvider(deploymentName);
    this.portName = defaultPortNameProvider(portName);
  }

  public KubernetesDiscoveryConfig() {
    this.namespace = defaultNamespaceProvider(null);
    this.deploymentName = defaultDeploymentNameProvider(null);
    this.portName = defaultPortNameProvider(null);
  }

  public String getNamespace() {
    return namespace;
  }

  public String getDeploymentName() {
    return deploymentName;
  }

  public String getPortName() {
    return portName;
  }

  @Override
  public Class getDiscoveryStrategyClass() {
    return KubernetesDiscoveryStrategy.class;
  }

  private String defaultNamespaceProvider(String providedNamespace) {
    String propertyNamespace = getPropertyNamespace();
    if (propertyNamespace != null && !propertyNamespace.isEmpty()) {
      return propertyNamespace;
    }
    if (providedNamespace != null && !providedNamespace.isEmpty()) {
      return providedNamespace;
    }
    return DISCOVERY_CONFIG_SYSTEM_PROPERTY_KUBERNETES_DEFAULT_NAMESPACE;
  }

  private String defaultDeploymentNameProvider(String providedDeploymentName) {
    String propertyDeploymentName = getPropertyDeploymentName();
    if (propertyDeploymentName != null && !propertyDeploymentName.isEmpty()) {
      return propertyDeploymentName;
    }
    if (providedDeploymentName != null && !providedDeploymentName.isEmpty()) {
      return providedDeploymentName;
    }
    return DISCOVERY_CONFIG_SYSTEM_PROPERTY_KUBERNETES_DEFAULT_DEPLOYMENT_NAME;
  }

  private String defaultPortNameProvider(String providedPortName) {
    String propertyPortName = getPropertyPortName();
    if (propertyPortName != null && !propertyPortName.isEmpty()) {
      return propertyPortName;
    }
    if (providedPortName != null && !providedPortName.isEmpty()) {
      return providedPortName;
    }
    return DISCOVERY_CONFIG_SYSTEM_PROPERTY_KUBERNETES_DEFAULT_PORT_NAME;
  }

  private String getPropertyNamespace() {
    String propertyNamespace =
        System.getProperty(DISCOVERY_CONFIG_SYSTEM_PROPERTY_KUBERNETES_NAMESPACE);
    if (propertyNamespace == null || propertyNamespace.isEmpty()) {
      return null;
    }
    return propertyNamespace;
  }

  private String getPropertyDeploymentName() {
    String propertyDeploymentName =
        System.getProperty(DISCOVERY_CONFIG_SYSTEM_PROPERTY_KUBERNETES_DEPLOYMENT_NAME);
    if (propertyDeploymentName == null || propertyDeploymentName.isEmpty()) {
      return null;
    }
    return propertyDeploymentName;
  }

  private String getPropertyPortName() {
    String propertyPortName =
        System.getProperty(DISCOVERY_CONFIG_SYSTEM_PROPERTY_KUBERNETES_PORT_NAME);
    if (propertyPortName == null || propertyPortName.isEmpty()) {
      return null;
    }
    return propertyPortName;
  }
}
