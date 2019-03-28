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

import org.junit.Before;
import org.junit.Test;

public class EC2TagsDiscoveryConfigTest {

  @Before
  public void clearProperties() {
    System.clearProperty(EC2TagsDiscoveryConfig.DISCOVERY_CONFIG_SYSTEM_PROPERTY_EC2_TAG_NAME);
    System.clearProperty(EC2TagsDiscoveryConfig.DISCOVERY_CONFIG_SYSTEM_PROPERTY_EC2_TAG_VALUE);
    System.clearProperty(EC2TagsDiscoveryConfig.DISCOVERY_CONFIG_SYSTEM_PROPERTY_EC2_PORT);
  }

  @Test
  public void propertyConfig() {
    String name = "foobar";
    String value = "binbaz";
    String portString = "8101";
    int portInt = 8101;

    System.setProperty(EC2TagsDiscoveryConfig.DISCOVERY_CONFIG_SYSTEM_PROPERTY_EC2_TAG_NAME, name);
    System.setProperty(
        EC2TagsDiscoveryConfig.DISCOVERY_CONFIG_SYSTEM_PROPERTY_EC2_TAG_VALUE, value);
    System.setProperty(
        EC2TagsDiscoveryConfig.DISCOVERY_CONFIG_SYSTEM_PROPERTY_EC2_PORT, portString);

    EC2TagsDiscoveryConfig ec2TagsDiscoveryConfig = new EC2TagsDiscoveryConfig();
    assertEquals(name, ec2TagsDiscoveryConfig.getTagName());
    assertEquals(value, ec2TagsDiscoveryConfig.getTagValue());
    assertEquals(portInt, ec2TagsDiscoveryConfig.getPort());
  }

  @Test
  public void propertyOverride() {
    String name = "magic";
    String overrideName = "reality";
    String value = "foo";
    String overrideValue = "bar";
    int portInt = 8101;
    int overridePortInt = 9001;

    System.setProperty(
        EC2TagsDiscoveryConfig.DISCOVERY_CONFIG_SYSTEM_PROPERTY_EC2_TAG_NAME, overrideName);
    System.setProperty(
        EC2TagsDiscoveryConfig.DISCOVERY_CONFIG_SYSTEM_PROPERTY_EC2_TAG_VALUE, overrideValue);
    System.setProperty(
        EC2TagsDiscoveryConfig.DISCOVERY_CONFIG_SYSTEM_PROPERTY_EC2_PORT,
        Integer.toString(overridePortInt));
    EC2TagsDiscoveryConfig ec2TagsDiscoveryConfig =
        new EC2TagsDiscoveryConfig(name, value, portInt);
    assertEquals(overrideName, ec2TagsDiscoveryConfig.getTagName());
    assertEquals(overrideValue, ec2TagsDiscoveryConfig.getTagValue());
    assertEquals(overridePortInt, ec2TagsDiscoveryConfig.getPort());
  }

  @Test
  public void defaultConfig() {
    EC2TagsDiscoveryConfig ec2TagsDiscoveryConfig = new EC2TagsDiscoveryConfig();
    assertEquals(
        EC2TagsDiscoveryConfig.DISCOVERY_CONFIG_SYSTEM_PROPERTY_EC2_DEFAULT_TAG_NAME,
        ec2TagsDiscoveryConfig.getTagName());
    assertEquals(
        EC2TagsDiscoveryConfig.DISCOVERY_CONFIG_SYSTEM_PROPERTY_EC2_DEFAULT_TAG_VALUE,
        ec2TagsDiscoveryConfig.getTagValue());
    assertEquals(
        EC2TagsDiscoveryConfig.DISCOVERY_CONFIG_SYSTEM_PROPERTY_EC2_DEFAULT_PORT,
        ec2TagsDiscoveryConfig.getPort());
  }
}
