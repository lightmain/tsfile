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
import java.util.List;

/** Independently compressed BF16 and exact XOR-recovery streams inside a native value page. */
public final class FloatLayerFrame {
  public static final int MAGIC = 0x42463136;
  public static final int HEADER_BYTES = 24;
  public static final int ENTRY_BYTES = 32;
  private static final int MAX_VALUES = 1 << 24;

  private FloatLayerFrame() {}

  @FunctionalInterface
  public interface ReadAt {
    byte[] read(long offset, int length) throws IOException;
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

  public static byte[] encode(int[] bits, int count, int unit, boolean roundEven)
      throws IOException {
    if (count < 0 || count > bits.length || count > MAX_VALUES || unit < 1) {
      throw invalid("count/unit");
    }
    int units = (int) ((count + (long) unit - 1) / unit);
    int offset = Math.addExact(HEADER_BYTES, Math.multiplyExact(units, ENTRY_BYTES));
    List<Entry> entries = new ArrayList<>();
    ByteArrayOutputStream payload = new ByteArrayOutputStream();
    ICompressor compressor = ICompressor.getCompressor(CompressionType.LZ4);
    for (int begin = 0; begin < count; ) {
      int n = Math.min(unit, count - begin);
      ByteBuffer high = ByteBuffer.allocate(n * 2);
      ByteBuffer low = ByteBuffer.allocate(n * 4);
      for (int i = 0; i < n; i++) {
        int h = coarse(bits[begin + i], roundEven);
        high.putShort((short) h);
        low.putInt(bits[begin + i] ^ (h << 16));
      }
      byte[] highCompressed = compressor.compress(high.array());
      byte[] lowCompressed = compressor.compress(low.array());
      int lowOffset = Math.addExact(offset, highCompressed.length);
      entries.add(
          new Entry(
              begin,
              n,
              offset,
              highCompressed.length,
              n * 2,
              lowOffset,
              lowCompressed.length,
              n * 4));
      payload.write(highCompressed);
      payload.write(lowCompressed);
      offset = Math.addExact(lowOffset, lowCompressed.length);
      begin += n;
    }
    ByteBuffer out = ByteBuffer.allocate(offset);
    out.putInt(MAGIC)
        .putInt(roundEven ? 1 : 0)
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

  private static byte[] exact(ReadAt source, long offset, int length) throws IOException {
    byte[] result = source.read(offset, length);
    if (result.length != length) {
      throw invalid("short-read");
    }
    return result;
  }

  /** Independently restarted original GORILLA plus LZ4 units; exact raw fallback for sentinel. */
  public static byte[] encodeOriginal(int[] bits, int count, int unit) throws IOException {
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
    out.putInt(MAGIC).putInt(2).putInt(count).putInt(unit).putInt(units).putInt(offset);
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
        || (flags != 0 && flags != 1 && flags != 2)
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
          flags == 2
              ? ((e.coarseRaw > 0 && e.coarseRaw <= n * 8L + 64) || e.coarseRaw == -n * 4)
                  && e.fineLength == 0
                  && e.fineRaw == 0
              : e.coarseRaw == n * 2 && e.fineLength > 0 && e.fineRaw == n * 4;
      if (e.begin != begin
          || e.count != n
          || e.coarseOffset != next
          || e.coarseLength < 1
          || !rawSizesValid
          || e.fineOffset != next + e.coarseLength
          || e.fineOffset + (long) e.fineLength > length) {
        throw invalid("directory");
      }
      entries.add(e);
      next = e.fineOffset + (long) e.fineLength;
      begin += n;
    }
    if (next != length) {
      throw invalid("payload-size");
    }
    return new Directory(count, unit, flags == 1, flags == 2 ? 2 : 0, length, List.copyOf(entries));
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
    for (Entry e : directory.entries) {
      int begin = Math.max(start, e.begin), end = Math.min(start + length, e.begin + e.count);
      if (begin >= end) {
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
      for (int at = begin; at < end; at++) {
        int h = high.getShort((at - e.begin) * 2) & 0xffff;
        result[at - start] = coarseOnly ? h : (h << 16) ^ low.getInt((at - e.begin) * 4);
      }
    }
    return result;
  }

  public static int[] decode(byte[] frame) throws IOException {
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
