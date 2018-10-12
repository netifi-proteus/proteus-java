package io.netifi.proteus.tags;

import io.netty.handler.codec.Headers;
import java.util.Iterator;

public interface Tags extends Headers<CharSequence, CharSequence, Tags> {

  /**
   * Equivalent to {@link #getAll(Object)} but no intermediate list is generated.
   *
   * @param name the name of the header to retrieve
   * @return an {@link Iterator} of header values corresponding to {@code name}.
   */
  Iterator<CharSequence> valueIterator(CharSequence name);

  /**
   * Returns {@code true} if a header with the {@code name} and {@code value} exists, {@code false}
   * otherwise.
   *
   * <p>If {@code caseInsensitive} is {@code true} then a case insensitive compare is done on the
   * value.
   *
   * @param name the name of the header to find
   * @param value the value of the header to find
   * @param caseInsensitive {@code true} then a case insensitive compare is run to compare values.
   *     otherwise a case sensitive compare is run to compare values.
   */
  boolean contains(CharSequence name, CharSequence value, boolean caseInsensitive);
}
