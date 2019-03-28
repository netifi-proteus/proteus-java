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
package io.netifi.proteus.auth;

import io.netty.buffer.ByteBuf;

/** Generates a shared secret based on a string input. */
public abstract class SessionUtil {
  public static final SessionUtil instance() {
    return new DefaultSessionUtil();
  }

  public abstract byte[] generateSessionToken(byte[] key, ByteBuf data, long count);

  public abstract int generateRequestToken(byte[] sessionToken, ByteBuf message, long count);

  public abstract boolean validateMessage(
      byte[] sessionToken, ByteBuf message, int requestToken, long count);

  public abstract long getThirtySecondsStepsFromEpoch();
}
