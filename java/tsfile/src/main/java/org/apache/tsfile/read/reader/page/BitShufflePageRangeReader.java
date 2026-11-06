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

import org.apache.tsfile.encoding.floatlayer.BitShuffleFrame;
import org.apache.tsfile.encoding.floatlayer.FloatLayerFrame;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/** Reuses storage and coalesces a contiguous interval of bitshuffle subpages per half. */
public final class BitShufflePageRangeReader {
  private BitShufflePageRangeReader() {}

  public static final class Session {
    private byte[] metadata = new byte[0], high = new byte[0], low = new byte[0];
    private int[] ranks = new int[0], present = new int[0];
    private final BitShuffleFrame.Workspace workspace = new BitShuffleFrame.Workspace();
    public long requestedBytes, readCalls, decompressedBytes, decodedValues;
    public long transposeNs, decompressNs;

    public void readInto(
        FloatLayerFrame.ReadInto source,
        int payloadBytes,
        int rows,
        int start,
        int length,
        boolean coarseOnly,
        int[] output,
        int outputOffset,
        boolean profile)
        throws IOException {
      requestedBytes =
          readCalls = decompressedBytes = decodedValues = transposeNs = decompressNs = 0;
      if (rows < 0
          || rows > (1 << 24)
          || start < 0
          || length < 0
          || start > rows - length
          || outputOffset < 0
          || outputOffset > output.length - length) {
        throw FloatLayerFrame.invalid("bitshuffle-logical-range");
      }
      if (length == 0) {
        return;
      }
      if (payloadBytes == 0) {
        Arrays.fill(output, outputOffset, outputOffset + length, 0);
        return;
      }
      int bitmap = (rows + 7) / 8, base = 4 + bitmap, prefix = base + BitShuffleFrame.HEADER_BYTES;
      if (prefix > payloadBytes) {
        throw FloatLayerFrame.invalid("bitshuffle-aligned-header");
      }
      if (metadata.length < prefix) {
        metadata = new byte[prefix];
      }
      source.read(0, metadata, 0, prefix);
      requestedBytes += prefix;
      readCalls++;
      ByteBuffer header = ByteBuffer.wrap(metadata);
      if (header.getInt(0) != rows) {
        throw FloatLayerFrame.invalid("bitshuffle-row-count");
      }
      int degree = BitShuffleFrame.degreeFromFlags(header.getInt(base + 4));
      int units = header.getInt(base + 16);
      long size = prefix + units * (long) BitShuffleFrame.entryBytes(degree);
      if (units < 0 || size > payloadBytes) {
        throw FloatLayerFrame.invalid("bitshuffle-metadata-size");
      }
      if (metadata.length < size) {
        metadata = Arrays.copyOf(metadata, (int) size);
      }
      if (size > prefix) {
        source.read(prefix, metadata, prefix, (int) size - prefix);
        requestedBytes += size - prefix;
        readCalls++;
      }
      var directory =
          BitShuffleFrame.directory(
              ByteBuffer.wrap(metadata, base, (int) size - base).slice(), payloadBytes - base);
      int total = 0;
      for (int i = 0; i < bitmap; i++) {
        total += Integer.bitCount(metadata[4 + i] & 255);
      }
      if (total != directory.count()) {
        throw FloatLayerFrame.invalid("bitshuffle-present-count");
      }
      boolean allPresent = total == rows;
      if (!allPresent) {
        if (ranks.length < rows + 1) {
          ranks = new int[rows + 1];
        }
        ranks[0] = 0;
        for (int i = 0; i < rows; i++) {
          ranks[i + 1] = ranks[i] + ((metadata[4 + i / 8] >>> (7 - i % 8)) & 1);
        }
      }
      int first = allPresent ? start : ranks[start];
      int count = (allPresent ? start + length : ranks[start + length]) - first;
      if (count == 0) {
        Arrays.fill(output, outputOffset, outputOffset + length, 0);
        return;
      }
      BitShuffleFrame.Unit firstUnit = null, lastUnit = null;
      for (var unit : directory.units()) {
        if (unit.begin() < first + count && unit.begin() + unit.count() > first) {
          if (firstUnit == null) {
            firstUnit = unit;
          }
          lastUnit = unit;
        }
      }
      if (firstUnit == null) {
        throw FloatLayerFrame.invalid("bitshuffle-unit-range");
      }
      int highStart = firstUnit.highOffset();
      int highLength = lastUnit.highOffset() + lastUnit.highLength() - highStart;
      if (high.length < highLength) {
        high = new byte[highLength];
      }
      source.read(base + (long) highStart, high, 0, highLength);
      requestedBytes += highLength;
      readCalls++;
      int lowStart = 0;
      if (!coarseOnly && directory.degree() > 1) {
        lowStart = firstUnit.lowOffset();
        int lowLength = lastUnit.lowOffset() + lastUnit.lowLength() - lowStart;
        if (low.length < lowLength) {
          low = new byte[lowLength];
        }
        source.read(base + (long) lowStart, low, 0, lowLength);
        requestedBytes += lowLength;
        readCalls++;
      }
      if (present.length < count) {
        present = new int[count];
      }
      workspace.resetCounters();
      BitShuffleFrame.readRangeInto(
          directory,
          high,
          highStart,
          low,
          lowStart,
          first,
          count,
          coarseOnly,
          present,
          0,
          workspace,
          profile);
      decompressedBytes = workspace.decompressedBytes;
      decodedValues = workspace.decodedValues;
      transposeNs = workspace.transposeNs;
      decompressNs = workspace.decompressNs;
      if (allPresent) {
        System.arraycopy(present, 0, output, outputOffset, length);
      } else {
        int at = 0;
        for (int i = 0; i < length; i++) {
          output[outputOffset + i] = ranks[start + i] == ranks[start + i + 1] ? 0 : present[at++];
        }
      }
    }
  }
}
