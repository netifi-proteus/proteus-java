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

/** */
public enum FrameType {
  UNDEFINED(0x00),
  BROKER_SETUP(0x01),
  DESTINATION_SETUP(0x02),
  GROUP(0x03),
  BROADCAST(0x04),
  SHARD(0x05);

  private static FrameType[] typesById;

  private final int id;

  /** Index types by id for indexed lookup. */
  static {
    int max = 0;

    for (FrameType t : values()) {
      max = Math.max(t.id, max);
    }

    typesById = new FrameType[max + 1];

    for (FrameType t : values()) {
      typesById[t.id] = t;
    }
  }

  FrameType(int id) {
    this.id = id;
  }

  public int getEncodedType() {
    return id;
  }

  public static FrameType from(int id) {
    return typesById[id];
  }
}
