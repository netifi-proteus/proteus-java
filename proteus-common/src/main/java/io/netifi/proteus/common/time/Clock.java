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
package io.netifi.proteus.common.time;

import java.util.concurrent.TimeUnit;

@FunctionalInterface
public interface Clock {
  long getEpochTime();

  default long elapsedSince(long timestamp) {
    long t = getEpochTime();
    return Math.max(0L, t - timestamp);
  }

  default TimeUnit unit() {
    return TimeUnit.MILLISECONDS;
  }

  Clock DEFAULT = System::currentTimeMillis;
}
