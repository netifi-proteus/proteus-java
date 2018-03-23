package com.netflix.spectator.atlas;

import com.netflix.spectator.api.RegistryConfig;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

public interface NetifiAtlasConfig extends RegistryConfig {

  static NetifiAtlasConfig defaultInstance(Map<String, String> commonTags) {
    NetifiAtlasConfig c =
        new NetifiAtlasConfig() {
          @Override
          public String get(String k) {
            return commonTags.get(k);
          }

          @Override
          public Map<String, String> commonTags() {
            return commonTags;
          }
        };

    return c;
  }

  static NetifiAtlasConfig defaultInstance() {
    return defaultInstance(Collections.EMPTY_MAP);
  }

  /** Returns the step size (reporting frequency) to use. The default is 1 minute. */
  default Duration step() {
    String v = get("atlas.step");
    return (v == null) ? Duration.ofMinutes(1) : Duration.parse(v);
  }

  /**
   * Returns the TTL for meters that do not have any activity. After this period the meter will be
   * considered expired and will not get reported. Default is 15 minutes.
   */
  default Duration meterTTL() {
    String v = get("atlas.meterTTL");
    return (v == null) ? Duration.ofMinutes(15) : Duration.parse(v);
  }

  /** Returns true if publishing to Atlas is enabled. Default is true. */
  default boolean enabled() {
    String v = get("atlas.enabled");
    return v == null || Boolean.valueOf(v);
  }

  /** Returns the frequency for refreshing config settings from the LWC service. */
  default Duration configRefreshFrequency() {
    String v = get("atlas.configRefreshFrequency");
    return (v == null) ? Duration.ofSeconds(10) : Duration.parse(v);
  }

  /**
   * Returns the number of measurements per request to use for the backend. If more measurements are
   * found, then multiple requests will be made. The default is 10,000.
   */
  default int batchSize() {
    String v = get("atlas.batchSize");
    return (v == null) ? 10_000 : Integer.parseInt(v);
  }

  /**
   * Returns the common tags to apply to all metrics reported to Atlas. The default is an empty map.
   */
  default Map<String, String> commonTags() {
    return Collections.emptyMap();
  }

  /**
   * Returns a pattern indicating the valid characters for a tag key or value. The character set for
   * tag values can be overridden for a particular tag key using {@link #validTagValueCharacters()}.
   * The default is {@code -._A-Za-z0-9}.
   */
  default String validTagCharacters() {
    return "-._A-Za-z0-9";
  }

  /**
   * Returns a map from tag key to a pattern indicating the valid characters for the values of that
   * key. The default is an empty map.
   */
  default Map<String, String> validTagValueCharacters() {
    return Collections.emptyMap();
  }
}
