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
package io.netifi.proteus.frames;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

public interface ConnectionId {

  static ConnectionId wrap(byte[] connectionId) {
    return new WrappedConnectionId(connectionId);
  }

  static ConnectionId from(ByteBuf connectionId) {
    byte[] bytes = new byte[16];
    connectionId.getBytes(0, bytes);
    return new WrappedConnectionId(bytes);
  }

  static ConnectionId randomConnectionId() {
    try {
      MessageDigest md = MessageDigest.getInstance("MD5");
      md.update(UUID.randomUUID().toString().getBytes());
      return ConnectionId.wrap(md.digest());
    } catch (NoSuchAlgorithmException ex) {
      throw new RuntimeException("Failed to build connection id", ex);
    }
  }

  static boolean equal(ConnectionId a, ConnectionId b) {
    return a.first() == b.first() && a.second() == b.second();
  }

  long first();

  long second();

  byte[] bytes();

  class WrappedConnectionId implements ConnectionId {
    private final byte[] bytes;
    private final long first;
    private final long second;
    private final String asString;

    public WrappedConnectionId(byte[] connectionId) {
      if (connectionId.length != 16) {
        throw new IllegalArgumentException("Connection Id must be 16 bytes");
      }

      this.bytes = connectionId;
      ByteBuf tmp = Unpooled.wrappedBuffer(connectionId);
      this.first = tmp.readLong();
      this.second = tmp.readLong();
      this.asString = asString(this.bytes);
    }

    @Override
    public long first() {
      return this.first;
    }

    @Override
    public long second() {
      return this.second;
    }

    private static final char[] hexArray = "0123456789ABCDEF".toCharArray();

    private String asString(byte[] connectionId) {
      char[] hexChars = new char[connectionId.length * 2];
      for (int j = 0; j < connectionId.length; j++) {
        int v = connectionId[j] & 0xFF;
        hexChars[j * 2] = hexArray[v >>> 4];
        hexChars[j * 2 + 1] = hexArray[v & 0x0F];
      }
      return new String(hexChars);
    }

    @Override
    public byte[] bytes() {
      return this.bytes;
    }

    @Override
    public String toString() {
      return asString;
    }
  }
}
