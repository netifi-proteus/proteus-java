package io.netifi.proteus.tags;

import io.netty.handler.codec.EmptyHeaders;

public final class EmptyTags extends EmptyHeaders<CharSequence, CharSequence, Tags>
    implements Tags {
  public static final EmptyTags INSTANCE = new EmptyTags();

  private EmptyTags() {}

  @Override
  public boolean contains(CharSequence name, CharSequence value, boolean caseInsensitive) {
    return false;
  }
}
