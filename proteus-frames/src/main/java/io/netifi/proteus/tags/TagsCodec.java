package io.netifi.proteus.tags;

import static io.netty.util.AsciiString.EMPTY_STRING;
import static io.netty.util.internal.ThrowableUtil.unknownStackTrace;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.codec.DecoderException;
import io.netty.util.AsciiString;
import io.netty.util.CharsetUtil;
import java.util.Map;

public final class TagsCodec {
  private static final long DEFAULT_MAX_TAG_LIST_SIZE = 8192;

  private static final DecoderException DECODE_ULE_128_DECOMPRESSION_EXCEPTION =
      unknownStackTrace(
          new DecoderException("decompression failure"), TagsCodec.class, "decodeULE128(..)");
  private static final DecoderException DECODE_ULE_128_TO_LONG_DECOMPRESSION_EXCEPTION =
      unknownStackTrace(new DecoderException("long overflow"), TagsCodec.class, "decodeULE128(..)");
  private static final DecoderException DECODE_ULE_128_TO_INT_DECOMPRESSION_EXCEPTION =
      unknownStackTrace(
          new DecoderException("int overflow"), TagsCodec.class, "decodeULE128ToInt(..)");
  private static final DecoderException DECODE_ILLEGAL_INDEX_VALUE =
      unknownStackTrace(new DecoderException("illegal index value"), TagsCodec.class, "decode(..)");
  private static final DecoderException INDEX_HEADER_ILLEGAL_INDEX_VALUE =
      unknownStackTrace(
          new DecoderException("illegal index value"), TagsCodec.class, "indexHeader(..)");
  private static final DecoderException READ_NAME_ILLEGAL_INDEX_VALUE =
      unknownStackTrace(
          new DecoderException("illegal index value"), TagsCodec.class, "readName(..)");

  private static void tagListSizeExceeded(long maxTagListSize) throws DecoderException {
    throw new DecoderException("Tags size exceeded max allowed size: " + maxTagListSize);
  }

  private static final byte READ_HEADER_REPRESENTATION = 0;
  private static final byte READ_INDEXED_HEADER = 1;
  private static final byte READ_INDEXED_HEADER_NAME = 2;
  private static final byte READ_LITERAL_HEADER_NAME_LENGTH_PREFIX = 3;
  private static final byte READ_LITERAL_HEADER_NAME_LENGTH = 4;
  private static final byte READ_LITERAL_HEADER_NAME = 5;
  private static final byte READ_LITERAL_HEADER_VALUE_LENGTH_PREFIX = 6;
  private static final byte READ_LITERAL_HEADER_VALUE_LENGTH = 7;
  private static final byte READ_LITERAL_HEADER_VALUE = 8;

  /**
   * Decode the header block into header fields.
   *
   * <p>This method assumes the entire header block is contained in {@code in}.
   */
  public static Tags decode(ByteBuf in) throws DecoderException {
    Sink<Tags> sink = new TagsSink(DEFAULT_MAX_TAG_LIST_SIZE);
    decode(in, sink);

    // Now that we've read all of our tags we can perform the validation steps. We must
    // delay throwing until this point to prevent dynamic table corruption.
    return sink.finish();
  }

  private static void decode(ByteBuf in, Sink sink) throws DecoderException {
    int index = 0;
    int nameLength = 0;
    int valueLength = 0;
    byte state = READ_HEADER_REPRESENTATION;
    CharSequence name = null;
    while (in.isReadable()) {
      switch (state) {
        case READ_HEADER_REPRESENTATION:
          byte b = in.readByte();
          if (b < 0) {
            // Indexed Header Field
            index = b & 0x7F;
            switch (index) {
              case 0:
                throw DECODE_ILLEGAL_INDEX_VALUE;
              case 0x7F:
                state = READ_INDEXED_HEADER;
                break;
              default:
                TagField indexedHeader = getIndexedHeader(index);
                sink.appendToTagList(indexedHeader.name, indexedHeader.value);
            }
          } else {
            // Literal Header Field without Indexing
            index = b & 0x0F;
            switch (index) {
              case 0:
                state = READ_LITERAL_HEADER_NAME_LENGTH_PREFIX;
                break;
              case 0x0F:
                state = READ_INDEXED_HEADER_NAME;
                break;
              default:
                // Index was stored as the prefix
                name = readName(index);
                nameLength = name.length();
                state = READ_LITERAL_HEADER_VALUE_LENGTH_PREFIX;
            }
          }
          break;

        case READ_INDEXED_HEADER:
          TagField indexedHeader = getIndexedHeader(decodeULE128(in, index));
          sink.appendToTagList(indexedHeader.name, indexedHeader.value);
          state = READ_HEADER_REPRESENTATION;
          break;

        case READ_INDEXED_HEADER_NAME:
          // Header Name matches an entry in the Header Table
          name = readName(decodeULE128(in, index));
          nameLength = name.length();
          state = READ_LITERAL_HEADER_VALUE_LENGTH_PREFIX;
          break;

        case READ_LITERAL_HEADER_NAME_LENGTH_PREFIX:
          b = in.readByte();
          index = b & 0x7F;
          if (index == 0x7f) {
            state = READ_LITERAL_HEADER_NAME_LENGTH;
          } else {
            nameLength = index;
            state = READ_LITERAL_HEADER_NAME;
          }
          break;

        case READ_LITERAL_HEADER_NAME_LENGTH:
          // Header Name is a Literal String
          nameLength = decodeULE128(in, index);

          state = READ_LITERAL_HEADER_NAME;
          break;

        case READ_LITERAL_HEADER_NAME:
          // Wait until entire name is readable
          if (in.readableBytes() < nameLength) {
            throw notEnoughDataException(in);
          }

          name = readStringLiteral(in, nameLength);

          state = READ_LITERAL_HEADER_VALUE_LENGTH_PREFIX;
          break;

        case READ_LITERAL_HEADER_VALUE_LENGTH_PREFIX:
          b = in.readByte();
          index = b & 0x7F;
          switch (index) {
            case 0x7f:
              state = READ_LITERAL_HEADER_VALUE_LENGTH;
              break;
            case 0:
              insertHeader(sink, name, EMPTY_STRING);
              state = READ_HEADER_REPRESENTATION;
              break;
            default:
              valueLength = index;
              state = READ_LITERAL_HEADER_VALUE;
          }

          break;

        case READ_LITERAL_HEADER_VALUE_LENGTH:
          // Header Value is a Literal String
          valueLength = decodeULE128(in, index);

          state = READ_LITERAL_HEADER_VALUE;
          break;

        case READ_LITERAL_HEADER_VALUE:
          // Wait until entire value is readable
          if (in.readableBytes() < valueLength) {
            throw notEnoughDataException(in);
          }

          CharSequence value = readStringLiteral(in, valueLength);
          insertHeader(sink, name, value);
          state = READ_HEADER_REPRESENTATION;
          break;

        default:
          throw new Error("should not reach here state: " + state);
      }
    }

    if (state != READ_HEADER_REPRESENTATION) {
      throw new DecoderException("Incomplete header block fragment.");
    }
  }

  private static CharSequence readName(int index) throws DecoderException {
    if (index <= TagsStaticTable.length) {
      TagField tagField = TagsStaticTable.getEntry(index);
      return tagField.name;
    }
    throw READ_NAME_ILLEGAL_INDEX_VALUE;
  }

  private static TagField getIndexedHeader(int index) throws DecoderException {
    if (index <= TagsStaticTable.length) {
      return TagsStaticTable.getEntry(index);
    }
    throw INDEX_HEADER_ILLEGAL_INDEX_VALUE;
  }

  private static void insertHeader(Sink sink, CharSequence name, CharSequence value) {
    sink.appendToTagList(name, value);
  }

  private static CharSequence readStringLiteral(ByteBuf in, int length) throws DecoderException {
    byte[] buf = new byte[length];
    in.readBytes(buf);
    return new AsciiString(buf, false);
  }

  private static IllegalArgumentException notEnoughDataException(ByteBuf in) {
    return new IllegalArgumentException("decode only works with an entire header block! " + in);
  }

  /**
   * Unsigned Little Endian Base 128 Variable-Length Integer Encoding
   *
   * <p>Visible for testing only!
   */
  static int decodeULE128(ByteBuf in, int result) throws DecoderException {
    final int readerIndex = in.readerIndex();
    final long v = decodeULE128(in, (long) result);
    if (v > Integer.MAX_VALUE) {
      // the maximum value that can be represented by a signed 32 bit number is:
      // [0x1,0x7f] + 0x7f + (0x7f << 7) + (0x7f << 14) + (0x7f << 21) + (0x6 << 28)
      // OR
      // 0x0 + 0x7f + (0x7f << 7) + (0x7f << 14) + (0x7f << 21) + (0x7 << 28)
      // we should reset the readerIndex if we overflowed the int type.
      in.readerIndex(readerIndex);
      throw DECODE_ULE_128_TO_INT_DECOMPRESSION_EXCEPTION;
    }
    return (int) v;
  }

  /**
   * Unsigned Little Endian Base 128 Variable-Length Integer Encoding
   *
   * <p>Visible for testing only!
   */
  static long decodeULE128(ByteBuf in, long result) throws DecoderException {
    assert result <= 0x7f && result >= 0;
    final boolean resultStartedAtZero = result == 0;
    final int writerIndex = in.writerIndex();
    for (int readerIndex = in.readerIndex(), shift = 0;
        readerIndex < writerIndex;
        ++readerIndex, shift += 7) {
      byte b = in.getByte(readerIndex);
      if (shift == 56 && ((b & 0x80) != 0 || b == 0x7F && !resultStartedAtZero)) {
        // the maximum value that can be represented by a signed 64 bit number is:
        // [0x01L, 0x7fL] + 0x7fL + (0x7fL << 7) + (0x7fL << 14) + (0x7fL << 21) + (0x7fL << 28) +
        // (0x7fL << 35)
        // + (0x7fL << 42) + (0x7fL << 49) + (0x7eL << 56)
        // OR
        // 0x0L + 0x7fL + (0x7fL << 7) + (0x7fL << 14) + (0x7fL << 21) + (0x7fL << 28) + (0x7fL <<
        // 35) +
        // (0x7fL << 42) + (0x7fL << 49) + (0x7fL << 56)
        // this means any more shifts will result in overflow so we should break out and throw an
        // error.
        throw DECODE_ULE_128_TO_LONG_DECOMPRESSION_EXCEPTION;
      }

      if ((b & 0x80) == 0) {
        in.readerIndex(readerIndex + 1);
        return result + ((b & 0x7FL) << shift);
      }
      result += (b & 0x7FL) << shift;
    }

    throw DECODE_ULE_128_DECOMPRESSION_EXCEPTION;
  }

  private interface Sink<T> {
    void appendToTagList(CharSequence name, CharSequence value);

    T finish() throws DecoderException;
  }

  private static final class TagsSink implements Sink<Tags> {
    private final Tags tags;
    private final long maxTagListSize;
    private long tagsLength;
    private boolean exceededMaxLength;

    public TagsSink(long maxTagListSize) {
      this.tags = new DefaultTags();
      this.maxTagListSize = maxTagListSize;
    }

    @Override
    public Tags finish() throws DecoderException {
      if (exceededMaxLength) {
        tagListSizeExceeded(maxTagListSize);
      }
      return tags;
    }

    @Override
    public void appendToTagList(CharSequence name, CharSequence value) {
      tagsLength += TagField.sizeOf(name, value);
      exceededMaxLength |= tagsLength > maxTagListSize;

      if (exceededMaxLength) {
        // We don't store the header since we've already failed validation requirements.
        return;
      }

      tags.add(name, value);
    }
  }

  /**
   * Encode the header field into the header block.
   *
   * <p><strong>The given {@link CharSequence}s must be immutable!</strong>
   */
  public static ByteBuf encode(ByteBufAllocator allocator, Tags headers) {
    ByteBuf out = allocator.buffer();
    for (Map.Entry<CharSequence, CharSequence> header : headers) {
      CharSequence name = header.getKey();
      CharSequence value = header.getValue();
      encode(out, name, value);
    }
    return out;
  }

  /**
   * Encode the header field into the header block.
   *
   * <p><strong>The given {@link CharSequence}s must be immutable!</strong>
   */
  private static void encode(ByteBuf out, CharSequence name, CharSequence value) {
    int staticTableIndex = TagsStaticTable.getIndex(name, value);
    if (staticTableIndex == -1) {
      int nameIndex = TagsStaticTable.getIndex(name);
      encodeLiteral(out, name, value, nameIndex);
    } else {
      encodeInteger(out, 0x80, 7, staticTableIndex);
    }
  }

  /**
   * Encode integer according to <a href="https://tools.ietf.org/html/rfc7541#section-5.1">Section
   * 5.1</a>.
   */
  private static void encodeInteger(ByteBuf out, int mask, int n, int i) {
    encodeInteger(out, mask, n, (long) i);
  }

  /**
   * Encode integer according to <a href="https://tools.ietf.org/html/rfc7541#section-5.1">Section
   * 5.1</a>.
   */
  private static void encodeInteger(ByteBuf out, int mask, int n, long i) {
    assert n >= 0 && n <= 8 : "N: " + n;
    int nbits = 0xFF >>> (8 - n);
    if (i < nbits) {
      out.writeByte((int) (mask | i));
    } else {
      out.writeByte(mask | nbits);
      long length = i - nbits;
      for (; (length & ~0x7F) != 0; length >>>= 7) {
        out.writeByte((int) ((length & 0x7F) | 0x80));
      }
      out.writeByte((int) length);
    }
  }

  /** Encode string literal according to Section 5.2. */
  private static void encodeStringLiteral(ByteBuf out, CharSequence string) {
    encodeInteger(out, 0x00, 7, string.length());
    if (string instanceof AsciiString) {
      // Fast-path
      AsciiString asciiString = (AsciiString) string;
      out.writeBytes(asciiString.array(), asciiString.arrayOffset(), asciiString.length());
    } else {
      // Only ASCII is allowed in http2 headers, so its fine to use this.
      // https://tools.ietf.org/html/rfc7540#section-8.1.2
      out.writeCharSequence(string, CharsetUtil.ISO_8859_1);
    }
  }

  /** Encode literal header field according to Section 6.2. */
  private static void encodeLiteral(
      ByteBuf out, CharSequence name, CharSequence value, int nameIndex) {
    boolean nameIndexValid = nameIndex != -1;
    encodeInteger(out, 0x00, 4, nameIndexValid ? nameIndex : 0);
    if (!nameIndexValid) {
      encodeStringLiteral(out, name);
    }
    encodeStringLiteral(out, value);
  }

  private TagsCodec() {}
}
