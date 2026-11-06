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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/** Integer-word linear prediction and directly addressable fixed-width signed residuals. */
public final class LinearResidualStream {
  public static final int HEADER_BYTES = 9;

  private LinearResidualStream() {}

  public static final class Workspace {
    private final byte[] header = new byte[HEADER_BYTES];
    private byte[] packed = new byte[0];
  }

  /** Reuse caller output and read only parameters plus the requested packed-byte span. */
  public static void readRangeInto(
      FloatLayerFrame.ReadInto source,
      int offset,
      int encodedBytes,
      int count,
      int start,
      int length,
      int[] output,
      int outputOffset,
      Workspace workspace)
      throws IOException {
    if (count < 1
        || count > (1 << 24)
        || start < 0
        || length < 0
        || start > count - length
        || offset < 0
        || encodedBytes < HEADER_BYTES
        || outputOffset < 0
        || outputOffset > output.length - length) {
      throw FloatLayerFrame.invalid("linear-range");
    }
    if (length == 0) {
      return;
    }
    source.read(offset, workspace.header, 0, HEADER_BYTES);
    ByteBuffer header = ByteBuffer.wrap(workspace.header);
    int base = header.getInt(), step = header.getInt(), width = header.get() & 0xff;
    if (width > 32 || HEADER_BYTES + (count * (long) width + 7) / 8 != encodedBytes) {
      throw FloatLayerFrame.invalid("linear-width");
    }
    long firstBit = start * (long) width;
    int firstByte = (int) (firstBit / 8);
    int endByte = (int) (((start + (long) length) * width + 7) / 8);
    int bytes = endByte - firstByte;
    if (workspace.packed.length < bytes) {
      workspace.packed = new byte[bytes];
    }
    if (bytes > 0) {
      source.read((long) offset + HEADER_BYTES + firstByte, workspace.packed, 0, bytes);
    }
    long mask = width == 32 ? 0xffffffffL : (1L << width) - 1;
    for (int i = 0; i < length; i++) {
      long bit = (start + (long) i) * width - firstByte * 8L;
      int at = (int) (bit / 8), shift = (int) (bit & 7);
      long word = 0;
      for (int j = 0; j < 5 && at + j < bytes; j++) {
        word |= (workspace.packed[at + j] & 0xffL) << (8 * j);
      }
      int code = (int) ((word >>> shift) & mask);
      int residual = (code >>> 1) ^ -(code & 1);
      output[outputOffset + i] = base + step * (start + i) + residual;
    }
  }

  public static byte[] encode(int[] values) throws IOException {
    if (values.length == 0 || values.length > (1 << 24)) {
      throw FloatLayerFrame.invalid("linear-empty-unit");
    }
    int base = values[0];
    int step =
        values.length == 1
            ? 0
            : (int) (((long) values[values.length - 1] - base) / (values.length - 1));
    int[] residuals = new int[values.length];
    int combined = 0;
    for (int i = 0; i < values.length; i++) {
      int residual = values[i] - (base + step * i);
      int zigzag = (residual << 1) ^ (residual >> 31);
      residuals[i] = zigzag;
      combined |= zigzag;
    }
    int width = 32 - Integer.numberOfLeadingZeros(combined);
    ByteArrayOutputStream payload = new ByteArrayOutputStream();
    long pending = 0;
    int pendingBits = 0;
    for (int residual : residuals) {
      pending |= Integer.toUnsignedLong(residual) << pendingBits;
      pendingBits += width;
      while (pendingBits >= 8) {
        payload.write((int) pending & 0xff);
        pending >>>= 8;
        pendingBits -= 8;
      }
    }
    if (pendingBits > 0) {
      payload.write((int) pending & 0xff);
    }
    byte[] packed = payload.toByteArray();
    ByteBuffer result = ByteBuffer.allocate(HEADER_BYTES + packed.length);
    result.putInt(base).putInt(step).put((byte) width).put(packed);
    return result.array();
  }

  /** Only the shared nine-byte header and the requested residual bytes are read. */
  public static int[] readRange(
      FloatLayerFrame.ReadAt source, int offset, int encodedBytes, int count, int start, int length)
      throws IOException {
    if (count < 1
        || count > (1 << 24)
        || start < 0
        || length < 0
        || start > count - length
        || offset < 0
        || encodedBytes < HEADER_BYTES) {
      throw FloatLayerFrame.invalid("linear-range");
    }
    int[] result = new int[length];
    if (length == 0) {
      return result;
    }
    byte[] headerBytes = source.read(offset, HEADER_BYTES);
    if (headerBytes.length != HEADER_BYTES) {
      throw FloatLayerFrame.invalid("linear-short-header");
    }
    ByteBuffer header = ByteBuffer.wrap(headerBytes);
    int base = header.getInt(), step = header.getInt(), width = header.get() & 0xff;
    if (width > 32 || HEADER_BYTES + (count * (long) width + 7) / 8 != encodedBytes) {
      throw FloatLayerFrame.invalid("linear-width");
    }
    long firstBit = start * (long) width;
    int firstByte = (int) (firstBit / 8);
    int endByte = (int) (((start + (long) length) * width + 7) / 8);
    int bytes = endByte - firstByte;
    byte[] data =
        bytes == 0 ? new byte[0] : source.read((long) offset + HEADER_BYTES + firstByte, bytes);
    if (data.length != bytes) {
      throw FloatLayerFrame.invalid("linear-short-payload");
    }
    long mask = width == 32 ? 0xffffffffL : (1L << width) - 1;
    for (int i = 0; i < length; i++) {
      long bit = (start + (long) i) * width - firstByte * 8L;
      int at = (int) (bit / 8), shift = (int) (bit & 7);
      long word = 0;
      for (int j = 0; j < 5 && at + j < data.length; j++) {
        word |= (data[at + j] & 0xffL) << (8 * j);
      }
      int code = (int) ((word >>> shift) & mask);
      int residual = (code >>> 1) ^ -(code & 1);
      result[i] = base + step * (start + i) + residual;
    }
    return result;
  }
}
