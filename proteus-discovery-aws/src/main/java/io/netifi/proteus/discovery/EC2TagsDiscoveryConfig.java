package io.netifi.proteus.discovery;

public class EC2TagsDiscoveryConfig implements DiscoveryConfig {

  public static final String DISCOVERY_CONFIG_SYSTEM_PROPERTY_EC2_TAG_NAME =
      "netifi.proteus.discovery.ec2.tagName";
  public static final String DISCOVERY_CONFIG_SYSTEM_PROPERTY_EC2_TAG_VALUE =
      "netifi.proteus.discovery.ec2.tagValue";
  public static final String DISCOVERY_CONFIG_SYSTEM_PROPERTY_EC2_PORT =
      "netifi.proteus.discovery.ec2.port";
  public static final String DISCOVERY_CONFIG_SYSTEM_PROPERTY_EC2_DEFAULT_TAG_NAME = "service";
  public static final String DISCOVERY_CONFIG_SYSTEM_PROPERTY_EC2_DEFAULT_TAG_VALUE =
      "netifi-proteus-broker";
  public static final int DISCOVERY_CONFIG_SYSTEM_PROPERTY_EC2_DEFAULT_PORT = 7001;

  private final String tagName;
  private final String tagValue;
  private final int port;

  public EC2TagsDiscoveryConfig(String tagName, String tagValue, int port) {
    this.tagName = defaultTagNameProvider(tagName);
    this.tagValue = defaultTagValueProvider(tagValue);
    this.port = defaultPortProvider(port);
  }

  public EC2TagsDiscoveryConfig() {
    this.tagName = defaultTagNameProvider(null);
    this.tagValue = defaultTagValueProvider(null);
    this.port = defaultPortProvider(-1);
  }

  public String getTagName() {
    return tagName;
  }

  public String getTagValue() {
    return tagValue;
  }

  public int getPort() {
    return port;
  }

  @Override
  public Class getDiscoveryStrategyClass() {
    return EC2TagsDiscoveryStrategy.class;
  }

  private String defaultTagNameProvider(String providedName) {
    String propertyName = getPropertyTagName();
    if (propertyName != null) {
      return propertyName;
    }
    if (providedName != null && !providedName.isEmpty()) {
      return providedName;
    }
    return DISCOVERY_CONFIG_SYSTEM_PROPERTY_EC2_DEFAULT_TAG_NAME;
  }

  private String defaultTagValueProvider(String providedValue) {
    String propertyValue = getPropertyTagValue();
    if (propertyValue != null) {
      return propertyValue;
    }
    if (providedValue != null && !providedValue.isEmpty()) {
      return providedValue;
    }
    return DISCOVERY_CONFIG_SYSTEM_PROPERTY_EC2_DEFAULT_TAG_VALUE;
  }

  private int defaultPortProvider(int port) {
    int propertyPort = getPropertyPort();
    if (propertyPort > 0) {
      return propertyPort;
    }
    if (port > 0) {
      return port;
    }
    return DISCOVERY_CONFIG_SYSTEM_PROPERTY_EC2_DEFAULT_PORT;
  }

  private String getPropertyTagName() {
    String propertyName = System.getProperty(DISCOVERY_CONFIG_SYSTEM_PROPERTY_EC2_TAG_NAME);
    if (propertyName == null || propertyName.isEmpty()) {
      return null;
    }
    return propertyName;
  }

  private String getPropertyTagValue() {
    String propertyValue = System.getProperty(DISCOVERY_CONFIG_SYSTEM_PROPERTY_EC2_TAG_VALUE);
    if (propertyValue == null || propertyValue.isEmpty()) {
      return null;
    }
    return propertyValue;
  }

  private int getPropertyPort() {
    String portString = System.getProperty(DISCOVERY_CONFIG_SYSTEM_PROPERTY_EC2_PORT);
    if (portString == null || portString.isEmpty()) {
      return -1;
    }
    return Integer.parseInt(portString);
  }
}
