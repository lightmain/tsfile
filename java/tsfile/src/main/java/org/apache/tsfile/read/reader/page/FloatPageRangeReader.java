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

/** Read a logical row range from an uncompressed aligned native FLOAT-frame payload. */
public final class FloatPageRangeReader {
  private FloatPageRangeReader() {}

  public record Result(int[] values, boolean[] valid) {}

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
