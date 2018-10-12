package io.netifi.proteus.tags;

import static io.netty.util.AsciiString.*;

import io.netty.handler.codec.CharSequenceValueConverter;
import io.netty.handler.codec.DefaultHeaders;
import io.netty.util.AsciiString;

public class DefaultTags extends DefaultHeaders<CharSequence, CharSequence, Tags> implements Tags {
  private static final NameValidator<CharSequence> TAG_NAME_VALIDATOR =
      name -> {
        if (name == null || name.length() == 0) {
          throw new IllegalArgumentException("empty tags are not allowed: " + name);
        }
        if (name instanceof AsciiString) {
          final int index;
          try {
            index = ((AsciiString) name).forEachByte(value -> !isUpperCase(value));
          } catch (Throwable t) {
            throw new IllegalArgumentException("unexpected error. invalid tag name: " + name, t);
          }

          if (index != -1) {
            throw new IllegalArgumentException("invalid tag name: " + name);
          }
        } else {
          for (int i = 0; i < name.length(); ++i) {
            if (isUpperCase(name.charAt(i))) {
              throw new IllegalArgumentException("invalid tag name: " + name);
            }
          }
        }
      };

  /**
   * Create a new instance.
   *
   * <p>Header names will be validated according to <a
   * href="https://tools.ietf.org/html/rfc7540">rfc7540</a>.
   */
  public DefaultTags() {
    this(true);
  }

  /**
   * Create a new instance.
   *
   * @param validate {@code true} to validate header names according to <a
   *     href="https://tools.ietf.org/html/rfc7540">rfc7540</a>. {@code false} to not validate
   *     header names.
   */
  @SuppressWarnings("unchecked")
  public DefaultTags(boolean validate) {
    // Case sensitive compare is used because it is cheaper, and header validation can be used to
    // catch invalid
    // headers.
    super(
        CASE_SENSITIVE_HASHER,
        CharSequenceValueConverter.INSTANCE,
        validate ? TAG_NAME_VALIDATOR : NameValidator.NOT_NULL);
  }

  /**
   * Create a new instance.
   *
   * @param validate {@code true} to validate header names according to <a
   *     href="https://tools.ietf.org/html/rfc7540">rfc7540</a>. {@code false} to not validate
   *     header names.
   * @param arraySizeHint A hint as to how large the hash data structure should be. The next
   *     positive power of two will be used. An upper bound may be enforced.
   */
  @SuppressWarnings("unchecked")
  public DefaultTags(boolean validate, int arraySizeHint) {
    // Case sensitive compare is used because it is cheaper, and header validation can be used to
    // catch invalid
    // headers.
    super(
        CASE_SENSITIVE_HASHER,
        CharSequenceValueConverter.INSTANCE,
        validate ? TAG_NAME_VALIDATOR : NameValidator.NOT_NULL,
        arraySizeHint);
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof Tags && equals((Tags) o, CASE_SENSITIVE_HASHER);
  }

  @Override
  public int hashCode() {
    return hashCode(CASE_SENSITIVE_HASHER);
  }

  @Override
  public boolean contains(CharSequence name, CharSequence value) {
    return contains(name, value, false);
  }

  @Override
  public boolean contains(CharSequence name, CharSequence value, boolean caseInsensitive) {
    return contains(name, value, caseInsensitive ? CASE_INSENSITIVE_HASHER : CASE_SENSITIVE_HASHER);
  }
}
