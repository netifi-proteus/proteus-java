package io.netifi.proteus.tags;

import io.netty.handler.codec.UnsupportedValueConverter;
import io.netty.util.AsciiString;
import io.netty.util.internal.ConstantTimeUtils;
import io.netty.util.internal.PlatformDependent;
import java.util.Arrays;
import java.util.List;

final class TagsStaticTable {

  private static final List<TagField> STATIC_TABLE =
      Arrays.asList(
          /*  1 */ newEmptyTagField("group"),
          /*  2 */ newEmptyTagField("destination"),
          /*  3 */ newEmptyTagField("region"));

  private static TagField newEmptyTagField(String name) {
    return new TagField(AsciiString.cached(name), AsciiString.EMPTY_STRING);
  }

  private static TagField newTagField(String name, String value) {
    return new TagField(AsciiString.cached(name), AsciiString.cached(value));
  }

  private static final CharSequenceMap<Integer> STATIC_INDEX_BY_NAME = createMap();

  /** The number of header fields in the static table. */
  static final int length = STATIC_TABLE.size();

  /** Return the header field at the given index value. */
  static TagField getEntry(int index) {
    return STATIC_TABLE.get(index - 1);
  }

  /**
   * Returns the lowest index value for the given header field name in the static table. Returns -1
   * if the header field name is not in the static table.
   */
  static int getIndex(CharSequence name) {
    Integer index = STATIC_INDEX_BY_NAME.get(name);
    if (index == null) {
      return -1;
    }
    return index;
  }

  /**
   * Returns the index value for the given header field in the static table. Returns -1 if the
   * header field is not in the static table.
   */
  static int getIndex(CharSequence name, CharSequence value) {
    int index = getIndex(name);
    if (index == -1) {
      return -1;
    }

    // Note this assumes all entries for a given header field are sequential.
    while (index <= length) {
      TagField entry = getEntry(index);
      if (equalsConstantTime(name, entry.name) == 0) {
        break;
      }
      if (equalsConstantTime(value, entry.value) != 0) {
        return index;
      }
      index++;
    }

    return -1;
  }

  /**
   * Compare two {@link CharSequence} objects without leaking timing information.
   *
   * <p>The {@code int} return type is intentional and is designed to allow cascading of constant
   * time operations:
   *
   * <pre>
   *     String s1 = "foo";
   *     String s2 = "foo";
   *     String s3 = "foo";
   *     String s4 = "goo";
   *     boolean equals = (equalsConstantTime(s1, s2) & equalsConstantTime(s3, s4)) != 0;
   * </pre>
   *
   * @param s1 the first value.
   * @param s2 the second value.
   * @return {@code 0} if not equal. {@code 1} if equal.
   */
  static int equalsConstantTime(CharSequence s1, CharSequence s2) {
    if (s1 instanceof AsciiString && s2 instanceof AsciiString) {
      if (s1.length() != s2.length()) {
        return 0;
      }
      AsciiString s1Ascii = (AsciiString) s1;
      AsciiString s2Ascii = (AsciiString) s2;
      return PlatformDependent.equalsConstantTime(
          s1Ascii.array(),
          s1Ascii.arrayOffset(),
          s2Ascii.array(),
          s2Ascii.arrayOffset(),
          s1.length());
    }

    return ConstantTimeUtils.equalsConstantTime(s1, s2);
  }

  // create a map CharSequenceMap header name to index value to allow quick lookup
  private static CharSequenceMap<Integer> createMap() {
    int length = STATIC_TABLE.size();
    CharSequenceMap<Integer> ret =
        new CharSequenceMap<>(true, UnsupportedValueConverter.instance(), length);
    // Iterate through the static table in reverse order to
    // save the smallest index for a given name in the map.
    for (int index = length; index > 0; index--) {
      TagField entry = getEntry(index);
      CharSequence name = entry.name;
      ret.set(name, index);
    }
    return ret;
  }

  private TagsStaticTable() {}
}
