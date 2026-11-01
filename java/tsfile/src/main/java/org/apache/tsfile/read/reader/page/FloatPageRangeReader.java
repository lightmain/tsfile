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
package org.apache.tsfile.read.reader.page;

import org.apache.tsfile.encoding.floatlayer.FloatLayerFrame;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/** Read a logical row range from an uncompressed aligned native FLOAT-frame payload. */
public final class FloatPageRangeReader {
  private FloatPageRangeReader() {}

  public record Result(int[] values, boolean[] valid) {}

  @FunctionalInterface
  public interface ReadInto {
    void read(long offset, byte[] target, int targetOffset, int length) throws IOException;
  }

  /** One instance per serial reader; buffers and bitmap ranks are reused within each request. */
  public static final class Session {
    private byte[] metadata = new byte[0],
        high = new byte[0],
        low = new byte[0],
        packet = new byte[0];
    private int[] ranks = new int[0], present = new int[0];
    private FloatLayerFrame.Workspace workspace = new FloatLayerFrame.Workspace();
    private final boolean reuse, stopAtEnd, fastRanks;
    public long requestedBytes, readCalls, decompressedBytes, decodedValues;

    public Session(boolean reuse, boolean stopAtEnd) {
      this(reuse, stopAtEnd, true);
    }

    public Session(boolean reuse, boolean stopAtEnd, boolean fastRanks) {
      this.reuse = reuse;
      this.stopAtEnd = stopAtEnd;
      this.fastRanks = fastRanks;
    }

    public void readInto(
        ReadInto source,
        int payloadBytes,
        int rows,
        int start,
        int length,
        boolean coarseOnly,
        int[] output,
        int outputOffset)
        throws IOException {
      requestedBytes = readCalls = decompressedBytes = decodedValues = 0;
      if (rows < 0
          || rows > (1 << 24)
          || start < 0
          || length < 0
          || start > rows - length
          || outputOffset < 0
          || outputOffset > output.length - length) {
        throw FloatLayerFrame.invalid("logical-range");
      }
      if (length == 0) {
        return;
      }
      if (payloadBytes == 0) {
        Arrays.fill(output, outputOffset, outputOffset + length, 0);
        return;
      }
      if (!reuse) {
        metadata = high = low = packet = new byte[0];
        ranks = present = new int[0];
        workspace = new FloatLayerFrame.Workspace();
      }
      int bitmap = (rows + 7) / 8, base = 4 + bitmap;
      int prefix = base + FloatLayerFrame.HEADER_BYTES;
      if (prefix > payloadBytes) {
        throw FloatLayerFrame.invalid("aligned-header");
      }
      if (metadata.length < prefix) {
        metadata = new byte[prefix];
      }
      source.read(0, metadata, 0, prefix);
      requestedBytes += prefix;
      readCalls++;
      ByteBuffer header = ByteBuffer.wrap(metadata);
      if (header.getInt(0) != rows) {
        throw FloatLayerFrame.invalid("logical-count");
      }
      int units = header.getInt(base + 16);
      long metaLength = prefix + units * (long) FloatLayerFrame.ENTRY_BYTES;
      if (units < 0 || metaLength > payloadBytes) {
        throw FloatLayerFrame.invalid("directory");
      }
      int size = (int) metaLength;
      if (metadata.length < size) {
        metadata = Arrays.copyOf(metadata, size);
      }
      if (size > prefix) {
        source.read(prefix, metadata, prefix, size - prefix);
        requestedBytes += size - prefix;
        readCalls++;
      }
      final byte[] meta = metadata;
      var directory =
          FloatLayerFrame.directory(
              (offset, n) -> {
                if (offset < 0 || offset + n > size - base) {
                  throw FloatLayerFrame.invalid("metadata-offset");
                }
                return Arrays.copyOfRange(meta, base + (int) offset, base + (int) offset + n);
              },
              payloadBytes - base);
      if (directory.format() != 2 && directory.format() != 8 && directory.format() != 16) {
        throw FloatLayerFrame.invalid("session-format");
      }
      if (ranks.length < rows + 1) {
        ranks = new int[rows + 1];
      }
      int total = 0;
      for (int i = 0; i < bitmap; i++) {
        total += Integer.bitCount(metadata[4 + i] & 0xff);
      }
      if (total != directory.count()) {
        throw FloatLayerFrame.invalid("present-count");
      }
      boolean allPresent = total == rows;
      if (!allPresent || !fastRanks) {
        ranks[0] = 0;
        for (int i = 0; i < rows; i++) {
          ranks[i + 1] = ranks[i] + ((metadata[4 + i / 8] >>> (7 - i % 8)) & 1);
        }
      }
      int first = allPresent ? start : ranks[start],
          count = (allPresent ? start + length : ranks[start + length]) - first;
      if (count == 0) {
        Arrays.fill(output, outputOffset, outputOffset + length, 0);
        return;
      }
      FloatLayerFrame.Entry firstEntry = null, lastEntry = null;
      for (var e : directory.entries()) {
        if (e.begin() < first + count && e.begin() + e.count() > first) {
          if (firstEntry == null) {
            firstEntry = e;
          }
          lastEntry = e;
        }
      }
      if (firstEntry == null) {
        throw FloatLayerFrame.invalid("range");
      }
      int highStart = firstEntry.coarseOffset(),
          highLength = lastEntry.coarseOffset() + lastEntry.coarseLength() - highStart;
      if (high.length < highLength) {
        high = new byte[highLength];
      }
      source.read(base + (long) highStart, high, 0, highLength);
      requestedBytes += highLength;
      readCalls++;
      int lowStart = 0, lowLength = 0;
      if (!coarseOnly && directory.format() != 2) {
        lowStart = firstEntry.fineOffset();
        lowLength = lastEntry.fineOffset() + lastEntry.fineLength() - lowStart;
        if (low.length < lowLength) {
          low = new byte[lowLength];
        }
        source.read(base + (long) lowStart, low, 0, lowLength);
        requestedBytes += lowLength;
        readCalls++;
      }
      final int fineStart = lowStart, fineLength = lowLength;
      if (present.length < count) {
        present = new int[count];
      }
      workspace.decompressed = workspace.decoded = 0;
      FloatLayerFrame.readRangeInto(
          (offset, n) -> {
            byte[] from;
            int at;
            if (offset >= highStart && offset + n <= highStart + highLength) {
              from = high;
              at = (int) offset - highStart;
            } else if (offset >= fineStart && offset + n <= fineStart + fineLength) {
              from = low;
              at = (int) offset - fineStart;
            } else {
              throw FloatLayerFrame.invalid("coalesced-offset");
            }
            if (packet.length < n) {
              packet = new byte[n];
            }
            System.arraycopy(from, at, packet, 0, n);
            return packet;
          },
          directory,
          first,
          count,
          coarseOnly,
          present,
          0,
          workspace,
          stopAtEnd);
      decompressedBytes = workspace.decompressed;
      decodedValues = workspace.decoded;
      if (allPresent) {
        System.arraycopy(present, 0, output, outputOffset, length);
      } else {
        int cursor = 0;
        for (int i = 0; i < length; i++) {
          output[outputOffset + i] =
              ranks[start + i + 1] == ranks[start + i] ? 0 : present[cursor++];
        }
      }
    }
  }

  public static Result read(
      FloatLayerFrame.ReadAt source, int payloadBytes, int start, int length, boolean coarseOnly)
      throws IOException {
    if (payloadBytes < 4) {
      throw FloatLayerFrame.invalid("aligned-header");
    }
    byte[] header = source.read(0, 4);
    if (header.length != 4) {
      throw FloatLayerFrame.invalid("short-aligned-header");
    }
    int rows = ByteBuffer.wrap(header).getInt();
    if (rows < 0 || rows > (1 << 24) || start < 0 || length < 0 || start > rows - length) {
      throw FloatLayerFrame.invalid("logical-range");
    }
    int bitmapBytes = (rows + 7) / 8;
    if (4L + bitmapBytes + FloatLayerFrame.HEADER_BYTES > payloadBytes) {
      throw FloatLayerFrame.invalid("bitmap-size");
    }
    byte[] bitmap = source.read(4, bitmapBytes);
    if (bitmap.length != bitmapBytes) {
      throw FloatLayerFrame.invalid("short-bitmap");
    }
    int base = 4 + bitmapBytes;
    FloatLayerFrame.ReadAt frameSource = (offset, size) -> source.read(base + offset, size);
    FloatLayerFrame.Directory directory =
        FloatLayerFrame.directory(frameSource, payloadBytes - base);
    int first = 0, count = 0, all = 0;
    boolean[] valid = new boolean[length];
    for (int i = 0; i < rows; i++) {
      if (((bitmap[i / 8] >>> (7 - i % 8)) & 1) != 0) {
        all++;
        if (i < start) {
          first++;
        } else if (i < start + length) {
          valid[i - start] = true;
          count++;
        }
      }
    }
    if (all != directory.count()) {
      throw FloatLayerFrame.invalid("present-count");
    }
    int[] present = FloatLayerFrame.readRange(frameSource, directory, first, count, coarseOnly);
    int[] result = new int[length];
    int cursor = 0;
    for (int i = 0; i < length; i++) {
      if (valid[i]) {
        result[i] = present[cursor++];
      }
    }
    return new Result(result, valid);
  }
}
