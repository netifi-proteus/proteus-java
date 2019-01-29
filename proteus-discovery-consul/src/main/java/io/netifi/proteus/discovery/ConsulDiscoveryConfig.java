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

import com.orbitz.consul.Consul;
import java.net.MalformedURLException;
import java.net.URL;
import reactor.core.Exceptions;

public class ConsulDiscoveryConfig implements DiscoveryConfig {
  public static final String DISCOVERY_CONFIG_SYSTEM_PROPERTY_CONSUL_URL =
      "netifi.proteus.discovery.consul.url";
  public static final String DISCOVERY_CONFIG_SYSTEM_PROPERTY_CONSUL_SERVICE_NAME =
      "netifi.proteus.discovery.consul.serviceName";

  public static final URL DISCOVERY_CONFIG_SYSTEM_PROPERTY_CONSUL_DEFAULT_URL;

  static {
    try {
      DISCOVERY_CONFIG_SYSTEM_PROPERTY_CONSUL_DEFAULT_URL =
          new URL("http", Consul.DEFAULT_HTTP_HOST, Consul.DEFAULT_HTTP_PORT, "");
    } catch (MalformedURLException e) {
      throw new RuntimeException("static code of default consul url is broken.");
    }
  }

  public static final String DISCOVERY_CONFIG_SYSTEM_PROPERTY_CONSUL_DEFAULT_SERVICE_NAME =
      "netifi-proteus-broker";

  private final URL consulURL;
  private final String serviceName;

  public ConsulDiscoveryConfig(URL consulURL, String serviceName) {
    this.consulURL = defaultURLProvider(consulURL);
    this.serviceName = defaultServiceNameProvider(serviceName);
  }

  public ConsulDiscoveryConfig() {
    this.consulURL = defaultURLProvider(null);
    this.serviceName = defaultServiceNameProvider(null);
  }

  public URL getConsulURL() {
    return consulURL;
  }

  public String getServiceName() {
    return serviceName;
  }

  @Override
  public Class getDiscoveryStrategyClass() {
    return ConsulDiscoveryStrategy.class;
  }

  private URL defaultURLProvider(URL providedAddress) {
    URL propertyURL = getPropertyURL();
    if (propertyURL != null) {
      return propertyURL;
    }
    if (providedAddress != null) {
      return providedAddress;
    }
    return DISCOVERY_CONFIG_SYSTEM_PROPERTY_CONSUL_DEFAULT_URL;
  }

  private String defaultServiceNameProvider(String providedServiceName) {
    String propertyServiceName = getPropertyServiceName();
    if (propertyServiceName != null && !propertyServiceName.isEmpty()) {
      return propertyServiceName;
    }
    if (providedServiceName != null && !providedServiceName.isEmpty()) {
      return providedServiceName;
    }
    return DISCOVERY_CONFIG_SYSTEM_PROPERTY_CONSUL_DEFAULT_SERVICE_NAME;
  }

  private URL getPropertyURL() {
    String propertyURLString = System.getProperty(DISCOVERY_CONFIG_SYSTEM_PROPERTY_CONSUL_URL);
    if (propertyURLString == null || propertyURLString.isEmpty()) {
      return null;
    }
    try {
      return new URL(propertyURLString);
    } catch (MalformedURLException e) {
      throw Exceptions.propagate(e);
    }
  }

  private String getPropertyServiceName() {
    String propertyServiceName =
        System.getProperty(DISCOVERY_CONFIG_SYSTEM_PROPERTY_CONSUL_SERVICE_NAME);
    if (propertyServiceName == null || propertyServiceName.isEmpty()) {
      return null;
    }
    return propertyServiceName;
  }
}
