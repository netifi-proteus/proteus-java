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

import io.netifi.proteus.common.net.HostAndPort;
import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Objects;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;

public interface DiscoveryStrategy {

  static DiscoveryStrategy getInstance(DiscoveryConfig discoveryConfig) {
    Objects.requireNonNull(discoveryConfig);
    try {
      Class discoveryStrategyClass = discoveryConfig.getDiscoveryStrategyClass();
      Constructor discoveryStrategyClassConstructor =
          discoveryStrategyClass.getConstructor(discoveryConfig.getClass());
      return (DiscoveryStrategy) discoveryStrategyClassConstructor.newInstance(discoveryConfig);

    } catch (Exception e) {
      throw Exceptions.propagate(e);
    }
  }

  Mono<? extends Collection<HostAndPort>> discoverNodes();
}
