package io.netifi.proteus.tags;

import static io.netty.util.internal.ObjectUtil.checkNotNull;

class TagField {

  // Section 4.1. Calculating Table Size
  // The additional 32 octets account for an estimated
  // overhead associated with the structure.
  static final int TAG_ENTRY_OVERHEAD = 32;

  static long sizeOf(CharSequence name, CharSequence value) {
    return name.length() + value.length() + TAG_ENTRY_OVERHEAD;
  }

  final CharSequence name;
  final CharSequence value;

  // This constructor can only be used if name and value are ISO-8859-1 encoded.
  TagField(CharSequence name, CharSequence value) {
    this.name = checkNotNull(name, "name");
    this.value = checkNotNull(value, "value");
  }

  final int size() {
    return name.length() + value.length() + TAG_ENTRY_OVERHEAD;
  }

  @Override
  public final boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof TagField)) {
      return false;
    }
    TagField other = (TagField) obj;
    // To avoid short circuit behavior a bitwise operator is used instead of a boolean operator.
    return (TagsStaticTable.equalsConstantTime(name, other.name)
            & TagsStaticTable.equalsConstantTime(value, other.value))
        != 0;
  }

  @Override
  public String toString() {
    return name + ": " + value;
  }
}
