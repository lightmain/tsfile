/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tsfile.encoding.floatlayer;

import org.apache.tsfile.compress.ICompressor;
import org.apache.tsfile.compress.IUnCompressor;
import org.apache.tsfile.encoding.decoder.SinglePrecisionDecoderV2;
import org.apache.tsfile.encoding.encoder.SinglePrecisionEncoderV2;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.i18n.Messages;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Independently addressable BF16 and exact recovery streams inside a native value page. */
public final class FloatLayerFrame {
  public static final int MAGIC = 0x42463136;
  public static final int HEADER_BYTES = 24;
  public static final int ENTRY_BYTES = 32;
  private static final int MAX_VALUES = 1 << 24;

  private FloatLayerFrame() {}

  public enum CoarseCodec {
    LZ4,
    GORILLA,
    GORILLA_COST,
    ADAPTIVE
  }

  @FunctionalInterface
  public interface ReadAt {
    byte[] read(long offset, int length) throws IOException;
  }

  @FunctionalInterface
  public interface ReadInto {
    void read(long offset, byte[] target, int targetOffset, int length) throws IOException;
  }

  public record Entry(
      int begin,
      int count,
      int coarseOffset,
      int coarseLength,
      int coarseRaw,
      int fineOffset,
      int fineLength,
      int fineRaw) {}

  public record Directory(
      int count, int unit, boolean roundEven, int format, int frameLength, List<Entry> entries) {}

  public static IOException invalid(Object detail) {
    return new IOException(Messages.format("error.encoding.bf16_invalid", detail));
  }

  public static int coarse(int bits, boolean roundEven) {
    int absolute = bits & 0x7fffffff;
    if (absolute > 0x7f800000) {
      return ((bits >>> 16) & 0x8000) | 0x7fc0;
    }
    if (absolute == 0x7f800000 || !roundEven) {
      return bits >>> 16;
    }
    return (bits + 0x7fff + ((bits >>> 16) & 1)) >>> 16;
  }

  private record Corrections(int[] indexes, int[] highs) {}

  private static int predictedHigh(int high, int low, boolean roundEven) {
    return (high - (roundEven && low > 0x8000 ? 1 : 0)) & 0xffff;
  }

  private static byte[] corrections(int[] bits, int begin, int count, boolean roundEven) {
    ByteBuffer result = ByteBuffer.allocate(4 + count * 6);
    result.putInt(0);
    int exceptions = 0;
    for (int i = 0; i < count; i++) {
      int word = bits[begin + i];
      int actualHigh = word >>> 16;
      if (predictedHigh(coarse(word, roundEven), word & 0xffff, roundEven) != actualHigh) {
        result.putInt(i).putShort((short) actualHigh);
        exceptions++;
      }
    }
    result.putInt(0, exceptions);
    return Arrays.copyOf(result.array(), result.position());
  }

  private static Corrections parseCorrections(byte[] bytes, int count) throws IOException {
    if (bytes.length < 4) {
      throw invalid("fine-corrections-header");
    }
    ByteBuffer data = ByteBuffer.wrap(bytes);
    int size = data.getInt();
    if (size < 0 || size > count || 4 + 6L * size != bytes.length) {
      throw invalid("fine-corrections-size");
    }
    int[] indexes = new int[size], highs = new int[size];
    int previous = -1;
    for (int i = 0; i < size; i++) {
      indexes[i] = data.getInt();
      highs[i] = data.getShort() & 0xffff;
      if (indexes[i] <= previous || indexes[i] >= count) {
        throw invalid("fine-corrections-index");
      }
      previous = indexes[i];
    }
    return new Corrections(indexes, highs);
  }

  private static int restoreCompact(
      int high, int low, int index, boolean roundEven, Corrections corrections) {
    int found = Arrays.binarySearch(corrections.indexes, index);
    int originalHigh = found >= 0 ? corrections.highs[found] : predictedHigh(high, low, roundEven);
    return (originalHigh << 16) | low;
  }

  public static byte[] encode(int[] bits, int count, int unit, boolean roundEven)
      throws IOException {
    return encode(bits, count, unit, roundEven, false);
  }

  public static byte[] encode(
      int[] bits, int count, int unit, boolean roundEven, boolean coarseGorilla)
      throws IOException {
    return encode(
        bits, count, unit, roundEven, coarseGorilla ? CoarseCodec.GORILLA : CoarseCodec.LZ4);
  }

  public static byte[] encode(int[] bits, int count, int unit, boolean roundEven, CoarseCodec codec)
      throws IOException {
    if (count < 0 || count > bits.length || count > MAX_VALUES || unit < 1) {
      throw invalid("count/unit");
    }
    if (codec == null) {
      throw invalid("coarse-codec");
    }
    if (codec == CoarseCodec.ADAPTIVE) {
      byte[] plain = encode(bits, count, unit, roundEven, CoarseCodec.LZ4);
      byte[] gorilla = encode(bits, count, unit, roundEven, CoarseCodec.GORILLA_COST);
      return gorilla.length < plain.length ? gorilla : plain;
    }
    boolean coarseGorilla = codec != CoarseCodec.LZ4;
    int units = (int) ((count + (long) unit - 1) / unit);
    int offset = Math.addExact(HEADER_BYTES, Math.multiplyExact(units, ENTRY_BYTES));
    List<Entry> entries = new ArrayList<>();
    ByteArrayOutputStream payload = new ByteArrayOutputStream();
    ICompressor compressor = ICompressor.getCompressor(CompressionType.LZ4);
    for (int begin = 0; begin < count; ) {
      int n = Math.min(unit, count - begin);
      ByteBuffer high = ByteBuffer.allocate(n * 2);
      int[] words = coarseGorilla ? new int[n] : null;
      byte[] corrections = corrections(bits, begin, n, roundEven);
      ByteBuffer low = ByteBuffer.allocate(n * 2 + corrections.length);
      for (int i = 0; i < n; i++) {
        int h = coarse(bits[begin + i], roundEven);
        high.putShort((short) h);
        if (coarseGorilla) {
          words[i] = h;
        }
        low.putShort((short) bits[begin + i]);
      }
      low.put(corrections);
      byte[] highEncoded =
          coarseGorilla
              ? Bf16GorillaStream.encode(words, codec == CoarseCodec.GORILLA_COST)
              : high.array();
      byte[] highCompressed = compressor.compress(highEncoded);
      byte[] lowCompressed = compressor.compress(low.array());
      int lowOffset = Math.addExact(offset, highCompressed.length);
      entries.add(
          new Entry(
              begin,
              n,
              offset,
              highCompressed.length,
              highEncoded.length,
              lowOffset,
              lowCompressed.length,
              low.capacity()));
      payload.write(highCompressed);
      payload.write(lowCompressed);
      offset = Math.addExact(lowOffset, lowCompressed.length);
      begin += n;
    }
    ByteBuffer out = ByteBuffer.allocate(offset);
    out.putInt(MAGIC)
        .putInt(roundEven ? 9 : 8)
        .putInt(count)
        .putInt(unit)
        .putInt(units)
        .putInt(offset);
    for (Entry e : entries) {
      out.putInt(e.begin)
          .putInt(e.count)
          .putInt(e.coarseOffset)
          .putInt(e.coarseLength)
          .putInt(e.coarseRaw)
          .putInt(e.fineOffset)
          .putInt(e.fineLength)
          .putInt(e.fineRaw);
    }
    out.put(payload.toByteArray());
    return groupLayers(out.array(), entries, roundEven, coarseGorilla);
  }

  private static byte[] groupLayers(
      byte[] frame, List<Entry> entries, boolean roundEven, boolean coarseGorilla) {
    ByteBuffer out = ByteBuffer.allocate(frame.length);
    int next = HEADER_BYTES + entries.size() * ENTRY_BYTES;
    out.put(frame, 0, next);
    out.putInt(4, (coarseGorilla ? 20 : 16) | (roundEven ? 1 : 0));
    for (int i = 0; i < entries.size(); i++) {
      Entry e = entries.get(i);
      out.putInt(HEADER_BYTES + i * ENTRY_BYTES + 8, next);
      out.position(next);
      out.put(frame, e.coarseOffset, e.coarseLength);
      next += e.coarseLength;
    }
    for (int i = 0; i < entries.size(); i++) {
      Entry e = entries.get(i);
      out.putInt(HEADER_BYTES + i * ENTRY_BYTES + 20, next);
      out.position(next);
      out.put(frame, e.fineOffset, e.fineLength);
      next += e.fineLength;
    }
    return out.array();
  }

  private static byte[] exact(ReadAt source, long offset, int length) throws IOException {
    byte[] result = source.read(offset, length);
    if (result.length != length) {
      throw invalid("short-read");
    }
    return result;
  }

  /** Independently restarted original GORILLA plus LZ4 units; exact raw fallback for sentinel. */
  public static byte[] encodeOriginal(int[] bits, int count, int unit) throws IOException {
    return encodeOriginal(bits, count, unit, false);
  }

  public static byte[] encodeOriginal(int[] bits, int count, int unit, boolean roundEven)
      throws IOException {
    if (count < 0 || count > bits.length || count > MAX_VALUES || unit < 1) {
      throw invalid("count/unit");
    }
    int units = (int) ((count + (long) unit - 1) / unit);
    int offset = HEADER_BYTES + units * ENTRY_BYTES;
    List<Entry> entries = new ArrayList<>();
    ByteArrayOutputStream payload = new ByteArrayOutputStream();
    int sentinel =
        Float.floatToRawIntBits(
            org.apache.tsfile.common.conf.TSFileConfig.GORILLA_ENCODING_ENDING_FLOAT);
    for (int begin = 0; begin < count; ) {
      int n = Math.min(unit, count - begin);
      boolean fallback = false;
      for (int i = 0; i < n; i++) {
        fallback |= bits[begin + i] == sentinel;
      }
      byte[] encoded;
      if (fallback) {
        ByteBuffer raw = ByteBuffer.allocate(n * 4);
        for (int i = 0; i < n; i++) {
          raw.putInt(bits[begin + i]);
        }
        encoded = raw.array();
      } else {
        SinglePrecisionEncoderV2 encoder = new SinglePrecisionEncoderV2();
        ByteArrayOutputStream raw = new ByteArrayOutputStream();
        for (int i = 0; i < n; i++) {
          encoder.encode(bits[begin + i], raw);
        }
        encoder.flush(raw);
        encoded = raw.toByteArray();
      }
      byte[] compressed = ICompressor.getCompressor(CompressionType.LZ4).compress(encoded);
      entries.add(
          new Entry(
              begin,
              n,
              offset,
              compressed.length,
              fallback ? -encoded.length : encoded.length,
              offset + compressed.length,
              0,
              0));
      payload.write(compressed);
      offset = Math.addExact(offset, compressed.length);
      begin += n;
    }
    ByteBuffer out = ByteBuffer.allocate(offset);
    out.putInt(MAGIC)
        .putInt(roundEven ? 3 : 2)
        .putInt(count)
        .putInt(unit)
        .putInt(units)
        .putInt(offset);
    for (Entry e : entries) {
      out.putInt(e.begin)
          .putInt(e.count)
          .putInt(e.coarseOffset)
          .putInt(e.coarseLength)
          .putInt(e.coarseRaw)
          .putInt(e.fineOffset)
          .putInt(e.fineLength)
          .putInt(e.fineRaw);
    }
    out.put(payload.toByteArray());
    return out.array();
  }

  /** Linear prediction in integer-word space with modulo-32-bit exact residual recovery. */
  public static byte[] encodeLinear(int[] bits, int count, int unit, boolean roundEven)
      throws IOException {
    if (count < 0 || count > bits.length || count > MAX_VALUES || unit < 1) {
      throw invalid("count/unit");
    }
    int units = (int) ((count + (long) unit - 1) / unit);
    int offset = HEADER_BYTES + units * ENTRY_BYTES;
    List<Entry> entries = new ArrayList<>();
    ByteArrayOutputStream payload = new ByteArrayOutputStream();
    for (int begin = 0; begin < count; ) {
      int n = Math.min(unit, count - begin);
      int[] high = new int[n], low = new int[n];
      for (int i = 0; i < n; i++) {
        int h = coarse(bits[begin + i], roundEven);
        high[i] = (short) h;
        low[i] = bits[begin + i] & 0xffff;
      }
      byte[] coarse = LinearResidualStream.encode(high);
      byte[] residuals = LinearResidualStream.encode(low);
      byte[] corrections = corrections(bits, begin, n, roundEven);
      byte[] fine =
          ByteBuffer.allocate(4 + residuals.length + corrections.length)
              .putInt(residuals.length)
              .put(residuals)
              .put(corrections)
              .array();
      int fineOffset = Math.addExact(offset, coarse.length);
      entries.add(
          new Entry(
              begin,
              n,
              offset,
              coarse.length,
              n * 2,
              fineOffset,
              fine.length,
              n * 2 + corrections.length));
      payload.write(coarse);
      payload.write(fine);
      offset = Math.addExact(fineOffset, fine.length);
      begin += n;
    }
    ByteBuffer out = ByteBuffer.allocate(offset);
    out.putInt(MAGIC)
        .putInt(roundEven ? 13 : 12)
        .putInt(count)
        .putInt(unit)
        .putInt(units)
        .putInt(offset);
    for (Entry e : entries) {
      out.putInt(e.begin)
          .putInt(e.count)
          .putInt(e.coarseOffset)
          .putInt(e.coarseLength)
          .putInt(e.coarseRaw)
          .putInt(e.fineOffset)
          .putInt(e.fineLength)
          .putInt(e.fineRaw);
    }
    out.put(payload.toByteArray());
    return out.array();
  }

  public static Directory directory(ReadAt source, int available) throws IOException {
    if (available < HEADER_BYTES) {
      throw invalid("header-size");
    }
    ByteBuffer header = ByteBuffer.wrap(exact(source, 0, HEADER_BYTES));
    int magic = header.getInt(), flags = header.getInt(), count = header.getInt();
    int unit = header.getInt(), units = header.getInt(), length = header.getInt();
    if (magic != MAGIC
        || flags < 0
        || !((flags <= 5)
            || flags == 8
            || flags == 9
            || flags == 12
            || flags == 13
            || flags == 16
            || flags == 17
            || flags == 20
            || flags == 21)
        || count < 0
        || count > MAX_VALUES
        || unit < 1
        || units != (count + (long) unit - 1) / unit
        || length != available
        || HEADER_BYTES + units * (long) ENTRY_BYTES > available) {
      throw invalid("header");
    }
    ByteBuffer table = ByteBuffer.wrap(exact(source, HEADER_BYTES, units * ENTRY_BYTES));
    List<Entry> entries = new ArrayList<>(units);
    long next = HEADER_BYTES + units * (long) ENTRY_BYTES;
    boolean coarseGorilla = (flags & 30) == 20;
    boolean grouped = (flags & 30) == 16 || coarseGorilla;
    long fineNext = next;
    if (grouped) {
      for (int i = 0; i < units; i++) {
        int size = table.getInt(i * ENTRY_BYTES + 12);
        if (size < 1) {
          throw invalid("directory");
        }
        fineNext += size;
      }
    }
    int begin = 0;
    for (int i = 0; i < units; i++) {
      Entry e =
          new Entry(
              table.getInt(),
              table.getInt(),
              table.getInt(),
              table.getInt(),
              table.getInt(),
              table.getInt(),
              table.getInt(),
              table.getInt());
      int n = Math.min(unit, count - begin);
      boolean rawSizesValid =
          (flags & 6) == 2
              ? ((e.coarseRaw > 0 && e.coarseRaw <= n * 8L + 64) || e.coarseRaw == -n * 4)
                  && e.fineLength == 0
                  && e.fineRaw == 0
              : e.coarseRaw == n * 2 && e.fineLength > 0 && e.fineRaw == n * 4;
      if ((flags & 6) == 4) {
        rawSizesValid =
            e.coarseRaw == n * 2
                && e.fineRaw == n * 4
                && e.coarseLength >= LinearResidualStream.HEADER_BYTES
                && e.fineLength >= LinearResidualStream.HEADER_BYTES
                && e.coarseLength <= n * 4L + LinearResidualStream.HEADER_BYTES
                && e.fineLength <= n * 4L + LinearResidualStream.HEADER_BYTES;
      }
      if ((flags & 14) == 8 || (flags & 14) == 12 || grouped) {
        rawSizesValid =
            (coarseGorilla ? e.coarseRaw >= 2 && e.coarseRaw <= n * 4L + 2 : e.coarseRaw == n * 2)
                && e.fineLength > 0
                && e.fineRaw >= n * 2L + 4
                && e.fineRaw <= n * 8L + 4;
        if ((flags & 14) == 12) {
          rawSizesValid &=
              e.coarseLength >= LinearResidualStream.HEADER_BYTES
                  && e.coarseLength <= n * 4L + LinearResidualStream.HEADER_BYTES
                  && e.fineLength >= 17
                  && e.fineLength <= n * 10L + 17;
        }
      }
      if (e.begin != begin
          || e.count != n
          || e.coarseOffset != next
          || e.coarseLength < 1
          || !rawSizesValid
          || e.fineOffset != (grouped ? fineNext : next + e.coarseLength)
          || e.fineOffset + (long) e.fineLength > length) {
        throw invalid("directory");
      }
      entries.add(e);
      next = grouped ? next + e.coarseLength : e.fineOffset + (long) e.fineLength;
      if (grouped) {
        fineNext += e.fineLength;
      }
      begin += n;
    }
    if ((grouped ? fineNext : next) != length) {
      throw invalid("payload-size");
    }
    return new Directory(count, unit, (flags & 1) != 0, flags & 30, length, List.copyOf(entries));
  }

  private static ByteBuffer unpack(ReadAt source, int offset, int length, int raw)
      throws IOException {
    byte[] compressed = exact(source, offset, length), output = new byte[raw];
    int n =
        IUnCompressor.getUnCompressor(CompressionType.LZ4)
            .uncompress(compressed, 0, length, output, 0);
    if (n != raw) {
      throw invalid("decompressed-size");
    }
    return ByteBuffer.wrap(output);
  }

  /** Full mode returns original FP32 bits; coarse mode returns unsigned BF16 codes. */
  public static int[] readRange(
      ReadAt source, Directory directory, int start, int length, boolean coarseOnly)
      throws IOException {
    if (start < 0 || length < 0 || start > directory.count - length) {
      throw invalid("range");
    }
    int[] result = new int[length];
    if (length == 0) {
      return result;
    }
    if (directory.format == 20) {
      readRangeInto(source, directory, start, length, coarseOnly, result, 0, new Workspace(), true);
      return result;
    }
    for (Entry e : directory.entries) {
      int begin = Math.max(start, e.begin), end = Math.min(start + length, e.begin + e.count);
      if (begin >= end) {
        continue;
      }
      if (directory.format == 4 || directory.format == 12) {
        int offsetInUnit = begin - e.begin, size = end - begin;
        int[] high =
            LinearResidualStream.readRange(
                source, e.coarseOffset, e.coarseLength, e.count, offsetInUnit, size);
        int[] fine = null;
        Corrections corrections = null;
        if (!coarseOnly) {
          int offset = e.fineOffset, bytes = e.fineLength;
          if (directory.format == 12) {
            int encoded = ByteBuffer.wrap(exact(source, offset, 4)).getInt();
            int correctionBytes = bytes - 4 - encoded;
            if (encoded < LinearResidualStream.HEADER_BYTES
                || encoded > e.count * 4L + LinearResidualStream.HEADER_BYTES
                || correctionBytes < 4
                || correctionBytes > e.count * 6L + 4
                || e.fineRaw != e.count * 2L + correctionBytes) {
              throw invalid("linear-fine-size");
            }
            corrections =
                parseCorrections(exact(source, offset + 4L + encoded, correctionBytes), e.count);
            offset += 4;
            bytes = encoded;
          }
          fine = LinearResidualStream.readRange(source, offset, bytes, e.count, offsetInUnit, size);
        }
        for (int i = 0; i < size; i++) {
          if (high[i] < Short.MIN_VALUE || high[i] > Short.MAX_VALUE) {
            throw invalid("linear-coarse-code");
          }
          int code = high[i] & 0xffff;
          if (coarseOnly) {
            result[begin - start + i] = code;
          } else if (directory.format == 12) {
            if (fine[i] < 0 || fine[i] > 0xffff) {
              throw invalid("linear-fine-code");
            }
            result[begin - start + i] =
                restoreCompact(code, fine[i], offsetInUnit + i, directory.roundEven, corrections);
          } else {
            result[begin - start + i] = (code << 16) ^ fine[i];
          }
        }
        continue;
      }
      if (directory.format == 2) {
        ByteBuffer data = unpack(source, e.coarseOffset, e.coarseLength, Math.abs(e.coarseRaw));
        SinglePrecisionDecoderV2 decoder = new SinglePrecisionDecoderV2();
        for (int i = 0; i < e.count; i++) {
          int bits =
              e.coarseRaw < 0 ? data.getInt() : Float.floatToRawIntBits(decoder.readFloat(data));
          int at = e.begin + i;
          if (at >= begin && at < end) {
            result[at - start] = coarseOnly ? coarse(bits, directory.roundEven) : bits;
          }
        }
        continue;
      }
      ByteBuffer high = unpack(source, e.coarseOffset, e.coarseLength, e.coarseRaw);
      ByteBuffer low = coarseOnly ? null : unpack(source, e.fineOffset, e.fineLength, e.fineRaw);
      Corrections corrections = null;
      if (!coarseOnly && (directory.format == 8 || directory.format == 16)) {
        byte[] bytes = new byte[e.fineRaw - e.count * 2];
        low.position(e.count * 2);
        low.get(bytes);
        corrections = parseCorrections(bytes, e.count);
      }
      for (int at = begin; at < end; at++) {
        int h = high.getShort((at - e.begin) * 2) & 0xffff;
        if (coarseOnly) {
          result[at - start] = h;
        } else if (directory.format == 8 || directory.format == 16) {
          result[at - start] =
              restoreCompact(
                  h,
                  low.getShort((at - e.begin) * 2) & 0xffff,
                  at - e.begin,
                  directory.roundEven,
                  corrections);
        } else {
          result[at - start] = (h << 16) ^ low.getInt((at - e.begin) * 4);
        }
      }
    }
    return result;
  }

  /** Per-reader decompression storage, never shared by concurrent requests. */
  public static final class Workspace {
    private byte[] high = new byte[0], low = new byte[0];
    private final SinglePrecisionDecoderV2 decoder = new SinglePrecisionDecoderV2();
    private final Bf16GorillaStream.Cursor coarseDecoder = new Bf16GorillaStream.Cursor();
    public long decompressed, decoded;
  }

  /** Decode into caller storage. Source can serve already-coalesced byte ranges. */
  public static void readRangeInto(
      ReadAt source,
      Directory directory,
      int start,
      int length,
      boolean coarseOnly,
      int[] output,
      int outputOffset,
      Workspace workspace,
      boolean stopAtEnd)
      throws IOException {
    if (start < 0
        || length < 0
        || start > directory.count - length
        || outputOffset < 0
        || outputOffset > output.length - length) {
      throw invalid("range");
    }
    if (directory.format != 2
        && directory.format != 8
        && directory.format != 16
        && directory.format != 20) {
      int[] values = readRange(source, directory, start, length, coarseOnly);
      System.arraycopy(values, 0, output, outputOffset, length);
      workspace.decoded += length;
      return;
    }
    for (Entry e : directory.entries) {
      int begin = Math.max(start, e.begin), end = Math.min(start + length, e.begin + e.count);
      if (begin >= end) {
        continue;
      }
      int highBytes = Math.abs(e.coarseRaw);
      if (workspace.high.length < highBytes) {
        workspace.high = new byte[highBytes];
      }
      byte[] compressed = source.read(e.coarseOffset, e.coarseLength);
      int actual =
          IUnCompressor.getUnCompressor(CompressionType.LZ4)
              .uncompress(compressed, 0, e.coarseLength, workspace.high, 0);
      if (actual != highBytes) {
        throw invalid("decompressed-size");
      }
      workspace.decompressed += highBytes;
      ByteBuffer high = ByteBuffer.wrap(workspace.high, 0, highBytes);
      if (directory.format == 2) {
        workspace.decoder.reset();
        int limit = stopAtEnd ? end - e.begin : e.count;
        for (int i = 0; i < limit; i++) {
          int bits =
              e.coarseRaw < 0
                  ? high.getInt()
                  : Float.floatToRawIntBits(workspace.decoder.readFloat(high));
          if (e.begin + i >= begin && e.begin + i < end) {
            output[outputOffset + e.begin + i - start] =
                coarseOnly ? coarse(bits, directory.roundEven) : bits;
          }
        }
        workspace.decoded += limit;
      } else {
        boolean gorilla = directory.format == 20;
        if (gorilla) {
          workspace.coarseDecoder.reset(high);
        }
        ByteBuffer low = null;
        Corrections corrections = null;
        if (!coarseOnly) {
          if (workspace.low.length < e.fineRaw) {
            workspace.low = new byte[e.fineRaw];
          }
          compressed = source.read(e.fineOffset, e.fineLength);
          actual =
              IUnCompressor.getUnCompressor(CompressionType.LZ4)
                  .uncompress(compressed, 0, e.fineLength, workspace.low, 0);
          if (actual != e.fineRaw) {
            throw invalid("decompressed-size");
          }
          workspace.decompressed += e.fineRaw;
          low = ByteBuffer.wrap(workspace.low);
          corrections =
              parseCorrections(Arrays.copyOfRange(workspace.low, e.count * 2, e.fineRaw), e.count);
        }
        int decodeBegin = gorilla ? e.begin : begin;
        int decodeEnd = gorilla && !stopAtEnd ? e.begin + e.count : end;
        for (int at = decodeBegin; at < decodeEnd; at++) {
          int index = at - e.begin;
          int h = gorilla ? workspace.coarseDecoder.next() : high.getShort(index * 2) & 0xffff;
          if (at < begin || at >= end) {
            continue;
          }
          output[outputOffset + at - start] =
              coarseOnly
                  ? h
                  : restoreCompact(
                      h, low.getShort(index * 2) & 0xffff, index, directory.roundEven, corrections);
        }
        workspace.decoded += decodeEnd - decodeBegin;
      }
    }
  }

  public static final class LinearWorkspace {
    private final LinearResidualStream.Workspace stream = new LinearResidualStream.Workspace();
    private final byte[] length = new byte[4];
    private int[] high = new int[0], low = new int[0];
    private byte[] corrections = new byte[0];
  }

  /** Compact linear streams retain exact range reads, including sparse recovery records. */
  public static void readLinearRangeInto(
      ReadInto source,
      Directory directory,
      int start,
      int length,
      boolean coarseOnly,
      int[] output,
      int outputOffset,
      LinearWorkspace workspace)
      throws IOException {
    if (directory.format != 12
        || start < 0
        || length < 0
        || start > directory.count - length
        || outputOffset < 0
        || outputOffset > output.length - length) {
      throw invalid("linear-range");
    }
    if (length == 0) {
      return;
    }
    if (workspace.high.length < length) {
      workspace.high = new int[length];
    }
    if (!coarseOnly && workspace.low.length < length) {
      workspace.low = new int[length];
    }
    for (Entry e : directory.entries) {
      int begin = Math.max(start, e.begin), end = Math.min(start + length, e.begin + e.count);
      if (begin >= end) {
        continue;
      }
      int first = begin - e.begin, size = end - begin;
      LinearResidualStream.readRangeInto(
          source,
          e.coarseOffset,
          e.coarseLength,
          e.count,
          first,
          size,
          workspace.high,
          0,
          workspace.stream);
      Corrections corrections = null;
      if (!coarseOnly) {
        source.read(e.fineOffset, workspace.length, 0, 4);
        int encoded = ByteBuffer.wrap(workspace.length).getInt();
        int correctionBytes = e.fineLength - 4 - encoded;
        if (encoded < LinearResidualStream.HEADER_BYTES
            || encoded > e.count * 4L + LinearResidualStream.HEADER_BYTES
            || correctionBytes < 4
            || correctionBytes > e.count * 6L + 4
            || e.fineRaw != e.count * 2L + correctionBytes) {
          throw invalid("linear-fine-size");
        }
        if (workspace.corrections.length != correctionBytes) {
          workspace.corrections = new byte[correctionBytes];
        }
        source.read(e.fineOffset + 4L + encoded, workspace.corrections, 0, correctionBytes);
        corrections = parseCorrections(workspace.corrections, e.count);
        LinearResidualStream.readRangeInto(
            source,
            e.fineOffset + 4,
            encoded,
            e.count,
            first,
            size,
            workspace.low,
            0,
            workspace.stream);
      }
      for (int i = 0; i < size; i++) {
        int h = workspace.high[i];
        if (h < Short.MIN_VALUE || h > Short.MAX_VALUE) {
          throw invalid("linear-coarse-code");
        }
        int code = h & 0xffff;
        if (coarseOnly) {
          output[outputOffset + begin - start + i] = code;
        } else {
          int low = workspace.low[i];
          if (low < 0 || low > 0xffff) {
            throw invalid("linear-fine-code");
          }
          output[outputOffset + begin - start + i] =
              restoreCompact(code, low, first + i, directory.roundEven, corrections);
        }
      }
    }
  }

  public static int[] decode(byte[] frame) throws IOException {
    if (BitShuffleFrame.isFrame(frame)) {
      return BitShuffleFrame.decode(frame);
    }
    ReadAt reader =
        (offset, length) -> {
          if (offset < 0 || length < 0 || offset > frame.length - (long) length) {
            throw invalid("offset");
          }
          return java.util.Arrays.copyOfRange(frame, (int) offset, (int) offset + length);
        };
    Directory table = directory(reader, frame.length);
    return readRange(reader, table, 0, table.count, false);
  }
}
