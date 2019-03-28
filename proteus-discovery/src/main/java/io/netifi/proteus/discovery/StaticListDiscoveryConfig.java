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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

// TODO: implement a version that parses a comma-delimited list of hostAndPorts
public class StaticListDiscoveryConfig implements DiscoveryConfig {
  public static final String DISCOVERY_CONFIG_SYSTEM_PROPERTY_STATIC_LIST_ADDRESSES =
      "netifi.proteus.discovery.staticlist.addresses";
  public static final String DISCOVERY_CONFIG_SYSTEM_PROPERTY_STATIC_LIST_PORT =
      "netifi.proteus.discovery.staticlist.port";
  private final List<String> addresses;
  private final int port;

  public StaticListDiscoveryConfig(int port, List<String> addresses) {
    this.addresses = defaultAddressListProvider(addresses);
    this.port = defaultPortProvider(port);
  }

  public StaticListDiscoveryConfig(int port, String... addresses) {
    this.addresses = defaultAddressListProvider(Arrays.asList(addresses));
    this.port = defaultPortProvider(port);
  }

  public StaticListDiscoveryConfig(int port, String commaDelimitedList) {
    this.addresses = defaultAddressListProvider(commaDelimitedListToList(commaDelimitedList));
    this.port = defaultPortProvider(port);
  }

  public StaticListDiscoveryConfig() {
    this.addresses = defaultAddressListProvider(Collections.emptyList());
    this.port = defaultPortProvider(-1);
  }

  public List<String> getAddresses() {
    return addresses;
  }

  public int getPort() {
    return port;
  }

  @Override
  public Class getDiscoveryStrategyClass() {
    return StaticListDiscoveryStrategy.class;
  }

  private int getPropertyPort() {
    String portString = System.getProperty(DISCOVERY_CONFIG_SYSTEM_PROPERTY_STATIC_LIST_PORT);
    if (portString == null || portString.isEmpty()) {
      return -1;
    }
    return Integer.parseInt(portString);
  }

  private int defaultPortProvider(int port) {
    int propertyPort = getPropertyPort();
    if (propertyPort > 0) {
      return propertyPort;
    }
    if (port > 0) {
      return port;
    }
    return 7001;
  }

  private List<String> defaultAddressListProvider(List<String> providedAddresses) {
    List<String> propertyAddresses = getPropertyAddressList();
    if (propertyAddresses.size() > 0) {
      return propertyAddresses;
    }
    return providedAddresses;
  }

  private List<String> getPropertyAddressList() {
    String propertyList =
        System.getProperty(DISCOVERY_CONFIG_SYSTEM_PROPERTY_STATIC_LIST_ADDRESSES);
    if (propertyList == null || propertyList.isEmpty()) {
      return Collections.emptyList();
    }
    return commaDelimitedListToList(propertyList);
  }

  private List<String> commaDelimitedListToList(String commaDelimitedList) {
    if (commaDelimitedList == null || commaDelimitedList.isEmpty()) {
      return Collections.emptyList();
    }
    return Arrays.asList(commaDelimitedList.split("\\s*,\\s*"));
  }
}
