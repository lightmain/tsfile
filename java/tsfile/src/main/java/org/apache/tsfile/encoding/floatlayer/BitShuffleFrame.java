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
import org.apache.tsfile.file.metadata.enums.CompressionType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Independently LZ4-compressed bit groups with parent-wide high/low ordering. */
public final class BitShuffleFrame {
  public static final int HEADER_BYTES = 24;
  private static final int MAX_VALUES = 1 << 24;

  private BitShuffleFrame() {}

  public static boolean supportedDegree(int degree) {
    return degree >= 1 && degree <= 32 && (degree & (degree - 1)) == 0;
  }

  public static boolean isFrame(byte[] bytes) {
    if (bytes.length < HEADER_BYTES) {
      return false;
    }
    ByteBuffer header = ByteBuffer.wrap(bytes);
    int flags = header.getInt(4);
    return header.getInt(0) == FloatLayerFrame.MAGIC
        && flags >= 64
        && flags <= 74
        && (flags & 1) == 0;
  }

  public static int degreeFromFlags(int flags) throws IOException {
    if (flags == 16 || flags == 8) {
      return 2;
    }
    if (flags < 64 || flags > 74 || (flags & 1) != 0) {
      throw FloatLayerFrame.invalid("bitshuffle-flags");
    }
    return 1 << ((flags - 64) / 2);
  }

  public static int entryBytes(int degree) {
    return 32 + 4 * Math.max(0, degree - 2);
  }

  public record Unit(
      int begin,
      int count,
      int highOffset,
      int highLength,
      int highRaw,
      int lowOffset,
      int lowLength,
      int lowRaw,
      int[] lengths) {}

  public record Directory(int count, int unit, int degree, int frameLength, List<Unit> units) {}

  private static int groupRaw(int count, int degree) {
    return (int) ((count * (long) (32 / degree) + 7) / 8);
  }

  public static byte[] encode(int[] words, int count, int unit, int degree) throws IOException {
    if (count < 0
        || count > words.length
        || count > MAX_VALUES
        || unit < 1
        || !supportedDegree(degree)) {
      throw FloatLayerFrame.invalid("bitshuffle-parameters");
    }
    if (degree == 2) {
      return FloatLayerFrame.encode(words, count, unit, false);
    }
    int units = (int) ((count + (long) unit - 1) / unit);
    int width = 32 / degree, half = degree == 1 ? 1 : degree / 2;
    int metadata = Math.addExact(HEADER_BYTES, Math.multiplyExact(units, entryBytes(degree)));
    List<byte[][]> encoded = new ArrayList<>(units);
    int[] fineRaw = new int[units];
    long total = metadata;
    var compressor = ICompressor.getCompressor(CompressionType.LZ4);
    for (int k = 0; k < units; k++) {
      int begin = k * unit, n = Math.min(unit, count - begin);
      int rawSize = groupRaw(n, degree);
      ByteBuffer corrections = ByteBuffer.allocate(4 + 6 * n);
      corrections.putInt(0);
      int exceptions = 0;
      byte[][] raw = new byte[degree][rawSize];
      for (int i = 0; i < n; i++) {
        int original = words[begin + i];
        int high = FloatLayerFrame.coarse(original, false);
        int word = degree == 1 ? original : (high << 16) | (original & 0xffff);
        if (degree > 1 && high != (original >>> 16)) {
          corrections.putInt(i).putShort((short) (original >>> 16));
          exceptions++;
        }
        for (int plane = 0; plane < degree; plane++) {
          int value = word >>> (32 - (plane + 1) * width);
          if (width == 32) {
            int at = i * 4;
            raw[plane][at] = (byte) (value >>> 24);
            raw[plane][at + 1] = (byte) (value >>> 16);
            raw[plane][at + 2] = (byte) (value >>> 8);
            raw[plane][at + 3] = (byte) value;
          } else {
            int at = i * width;
            raw[plane][at >>> 3] |= (byte) ((value & ((1 << width) - 1)) << (8 - width - (at & 7)));
          }
        }
      }
      corrections.putInt(0, exceptions);
      if (degree > 1) {
        int extra = corrections.position();
        raw[degree - 1] = Arrays.copyOf(raw[degree - 1], rawSize + extra);
        System.arraycopy(corrections.array(), 0, raw[degree - 1], rawSize, extra);
        fineRaw[k] = half * rawSize + extra;
      }
      byte[][] compressed = new byte[degree][];
      for (int plane = 0; plane < degree; plane++) {
        compressed[plane] = compressor.compress(raw[plane]);
        total += compressed[plane].length;
      }
      encoded.add(compressed);
    }
    if (total > Integer.MAX_VALUE) {
      throw FloatLayerFrame.invalid("bitshuffle-frame-size");
    }
    ByteBuffer output = ByteBuffer.allocate((int) total);
    output
        .putInt(FloatLayerFrame.MAGIC)
        .putInt(64 + 2 * Integer.numberOfTrailingZeros(degree))
        .putInt(count)
        .putInt(unit)
        .putInt(units)
        .putInt((int) total);
    int highNext = metadata;
    int lowNext = metadata;
    for (byte[][] data : encoded) {
      for (int plane = 0; plane < half; plane++) {
        lowNext += data[plane].length;
      }
    }
    for (int k = 0; k < units; k++) {
      int begin = k * unit, n = Math.min(unit, count - begin);
      byte[][] data = encoded.get(k);
      int highLength = 0, lowLength = 0;
      for (int plane = 0; plane < degree; plane++) {
        if (plane < half) {
          highLength += data[plane].length;
        } else {
          lowLength += data[plane].length;
        }
      }
      output
          .putInt(begin)
          .putInt(n)
          .putInt(highNext)
          .putInt(highLength)
          .putInt(half * groupRaw(n, degree))
          .putInt(degree == 1 ? highNext + highLength : lowNext)
          .putInt(lowLength)
          .putInt(fineRaw[k]);
      if (degree > 1) {
        for (int plane = 0; plane < degree; plane++) {
          if (plane != half - 1 && plane != degree - 1) {
            output.putInt(data[plane].length);
          }
        }
      }
      highNext += highLength;
      lowNext += lowLength;
    }
    for (byte[][] data : encoded) {
      for (int plane = 0; plane < half; plane++) {
        output.put(data[plane]);
      }
    }
    if (degree > 1) {
      for (byte[][] data : encoded) {
        for (int plane = half; plane < degree; plane++) {
          output.put(data[plane]);
        }
      }
    }
    return output.array();
  }

  /** Reads only header and directory bytes; offsets refer to the complete frame. */
  public static Directory directory(ByteBuffer input, int available) throws IOException {
    ByteBuffer data = input.duplicate();
    if (available < HEADER_BYTES || data.remaining() < HEADER_BYTES) {
      throw FloatLayerFrame.invalid("bitshuffle-header");
    }
    int magic = data.getInt(), flags = data.getInt(), count = data.getInt();
    int unit = data.getInt(), units = data.getInt(), length = data.getInt();
    int degree = degreeFromFlags(flags);
    long meta = HEADER_BYTES + units * (long) entryBytes(degree);
    if (magic != FloatLayerFrame.MAGIC
        || (flags == 8 && units > 1)
        || count < 0
        || count > MAX_VALUES
        || unit < 1
        || units != (count + (long) unit - 1) / unit
        || length != available
        || units < 0
        || meta > available
        || meta > input.remaining()) {
      throw FloatLayerFrame.invalid("bitshuffle-directory");
    }
    List<Unit> entries = new ArrayList<>(units);
    long highNext = meta, lowNext = meta;
    int half = degree == 1 ? 1 : degree / 2;
    for (int k = 0; k < units; k++) {
      lowNext += input.getInt(input.position() + HEADER_BYTES + k * entryBytes(degree) + 12);
    }
    for (int k = 0; k < units; k++) {
      int begin = data.getInt(), n = data.getInt();
      int highOffset = data.getInt(), highLength = data.getInt(), highRaw = data.getInt();
      int lowOffset = data.getInt(), lowLength = data.getInt(), lowRaw = data.getInt();
      int rawSize = groupRaw(n, degree);
      if (begin != k * (long) unit
          || n != Math.min(unit, count - begin)
          || highOffset != highNext
          || highLength < 1
          || highRaw != half * (long) rawSize
          || lowOffset != (degree == 1 ? highNext + highLength : lowNext)
          || lowLength < 0
          || lowOffset + (long) lowLength > length
          || (degree == 1
              ? lowLength != 0 || lowRaw != 0
              : lowLength < 1
                  || lowRaw < half * (long) rawSize + 4
                  || lowRaw > half * (long) rawSize + 4 + 6L * n)) {
        throw FloatLayerFrame.invalid("bitshuffle-unit");
      }
      int[] lengths = new int[degree];
      if (degree == 1) {
        lengths[0] = highLength;
      } else {
        long sumHigh = 0, sumLow = 0;
        for (int plane = 0; plane < degree; plane++) {
          if (plane == half - 1) {
            lengths[plane] = (int) (highLength - sumHigh);
          } else if (plane == degree - 1) {
            lengths[plane] = (int) (lowLength - sumLow);
          } else {
            lengths[plane] = data.getInt();
          }
          if (lengths[plane] < 1) {
            throw FloatLayerFrame.invalid("bitshuffle-block-length");
          }
          if (plane < half) {
            sumHigh += lengths[plane];
          } else {
            sumLow += lengths[plane];
          }
        }
      }
      entries.add(
          new Unit(
              begin, n, highOffset, highLength, highRaw, lowOffset, lowLength, lowRaw, lengths));
      highNext += highLength;
      lowNext += lowLength;
    }
    if ((degree == 1 ? highNext : lowNext) != length) {
      throw FloatLayerFrame.invalid("bitshuffle-payload");
    }
    return new Directory(count, unit, degree, length, List.copyOf(entries));
  }

  public static final class Workspace {
    private final byte[][] planes = new byte[32][];
    public long decompressedBytes, decodedValues, transposeNs, decompressNs;

    public void resetCounters() {
      decompressedBytes = decodedValues = transposeNs = decompressNs = 0;
    }
  }

  public static void readRangeInto(
      Directory directory,
      byte[] high,
      int highStart,
      byte[] low,
      int lowStart,
      int start,
      int length,
      boolean coarseOnly,
      int[] output,
      int outputOffset,
      Workspace workspace,
      boolean profile)
      throws IOException {
    if (start < 0
        || length < 0
        || start > directory.count - length
        || outputOffset < 0
        || outputOffset > output.length - length) {
      throw FloatLayerFrame.invalid("bitshuffle-range");
    }
    int degree = directory.degree, width = 32 / degree;
    int half = degree == 1 ? 1 : degree / 2;
    var decompressor = IUnCompressor.getUnCompressor(CompressionType.LZ4);
    for (Unit unit : directory.units) {
      int begin = Math.max(start, unit.begin),
          end = Math.min(start + length, unit.begin + unit.count);
      if (begin >= end) {
        continue;
      }
      int rawSize = groupRaw(unit.count, degree);
      int planesToRead = coarseOnly ? half : degree;
      int highAt = unit.highOffset - highStart, lowAt = unit.lowOffset - lowStart;
      long timer = profile ? System.nanoTime() : 0;
      for (int plane = 0; plane < planesToRead; plane++) {
        int raw = rawSize;
        if (degree > 1 && plane == degree - 1) {
          raw += unit.lowRaw - half * rawSize;
        }
        if (workspace.planes[plane] == null || workspace.planes[plane].length < raw) {
          workspace.planes[plane] = new byte[raw];
        }
        boolean coarse = plane < half;
        byte[] from = coarse ? high : low;
        int at = coarse ? highAt : lowAt, bytes = unit.lengths[plane];
        if (from == null || at < 0 || at > from.length - bytes) {
          throw FloatLayerFrame.invalid("bitshuffle-compressed-range");
        }
        int actual = decompressor.uncompress(from, at, bytes, workspace.planes[plane], 0);
        if (actual != raw) {
          throw FloatLayerFrame.invalid("bitshuffle-decompressed-size");
        }
        workspace.decompressedBytes += raw;
        if (coarse) {
          highAt += bytes;
        } else {
          lowAt += bytes;
        }
      }
      if (profile) {
        workspace.decompressNs += System.nanoTime() - timer;
      }
      timer = profile && degree >= 4 ? System.nanoTime() : 0;
      for (int index = begin - unit.begin; index < end - unit.begin; index++) {
        int result = 0;
        if (degree == 1) {
          byte[] bytes = workspace.planes[0];
          int at = index * 4;
          result =
              ((bytes[at] & 255) << 24)
                  | ((bytes[at + 1] & 255) << 16)
                  | ((bytes[at + 2] & 255) << 8)
                  | (bytes[at + 3] & 255);
          if (coarseOnly) {
            result = FloatLayerFrame.coarse(result, false);
          }
        } else if (degree == 2) {
          int at = index * 2;
          result = ((workspace.planes[0][at] & 255) << 8) | (workspace.planes[0][at + 1] & 255);
          if (!coarseOnly) {
            result =
                (result << 16)
                    | ((workspace.planes[1][at] & 255) << 8)
                    | (workspace.planes[1][at + 1] & 255);
          }
        } else {
          int bit = index * width, at = bit >>> 3, shift = 8 - width - (bit & 7);
          int mask = (1 << width) - 1;
          for (int plane = 0; plane < planesToRead; plane++) {
            result = (result << width) | ((workspace.planes[plane][at] >>> shift) & mask);
          }
        }
        output[outputOffset + unit.begin + index - start] = result;
      }
      if (profile && degree >= 4) {
        workspace.transposeNs += System.nanoTime() - timer;
      }
      if (!coarseOnly && degree > 1) {
        ByteBuffer corrections = ByteBuffer.wrap(workspace.planes[degree - 1]);
        corrections.position(rawSize);
        int count = corrections.getInt();
        if (count < 0 || count > unit.count || unit.lowRaw - half * rawSize != 4 + 6L * count) {
          throw FloatLayerFrame.invalid("bitshuffle-corrections");
        }
        int previous = -1;
        for (int i = 0; i < count; i++) {
          int index = corrections.getInt(), originalHigh = corrections.getShort() & 0xffff;
          if (index <= previous || index >= unit.count) {
            throw FloatLayerFrame.invalid("bitshuffle-correction-position");
          }
          int position = unit.begin + index;
          if (position >= begin && position < end) {
            int at = outputOffset + position - start;
            output[at] = (originalHigh << 16) | (output[at] & 0xffff);
          }
          previous = index;
        }
      }
      workspace.decodedValues += end - begin;
    }
  }

  public static int[] decode(byte[] frame) throws IOException {
    Directory directory = directory(ByteBuffer.wrap(frame), frame.length);
    int[] output = new int[directory.count];
    readRangeInto(
        directory, frame, 0, frame, 0, 0, output.length, false, output, 0, new Workspace(), false);
    return output;
  }
}
