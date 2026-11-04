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

/** Count-delimited, independently restarted Gorilla XOR coding for unsigned 16-bit words. */
public final class Bf16GorillaStream {
  private Bf16GorillaStream() {}

  public static byte[] encode(int[] words) {
    return encode(words, false);
  }

  public static byte[] encode(int[] words, boolean minimizeBits) {
    Bits out = new Bits();
    int previous = 0, leading = 16, trailing = 0;
    for (int i = 0; i < words.length; i++) {
      int word = words[i] & 0xffff;
      if (i == 0) {
        out.put(word, 16);
      } else {
        int xor = word ^ previous;
        if (xor == 0) {
          out.put(0, 1);
        } else {
          out.put(1, 1);
          int nextLeading = Integer.numberOfLeadingZeros(xor) - 16;
          int nextTrailing = Integer.numberOfTrailingZeros(xor);
          int width = 16 - nextLeading - nextTrailing;
          int previousWidth = 16 - leading - trailing;
          if (leading < 16
              && nextLeading >= leading
              && nextTrailing >= trailing
              && (!minimizeBits || previousWidth < 8 + width)) {
            out.put(0, 1);
          } else {
            out.put(1, 1);
            leading = nextLeading;
            trailing = nextTrailing;
            out.put(leading, 4);
            out.put(15 - leading - trailing, 4);
          }
          out.put(xor >>> trailing, 16 - leading - trailing);
        }
      }
      previous = word;
    }
    return out.finish();
  }

  private static final class Bits {
    private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    private long pending;
    private int size;

    void put(int value, int width) {
      pending = (pending << width) | (value & ((1L << width) - 1));
      size += width;
      while (size >= 8) {
        size -= 8;
        bytes.write((int) (pending >>> size) & 0xff);
      }
      pending &= (1L << size) - 1;
    }

    byte[] finish() {
      if (size > 0) {
        bytes.write((int) (pending << (8 - size)));
      }
      return bytes.toByteArray();
    }
  }

  /** Caller supplies the number of words from the enclosing subpage directory. */
  public static final class Cursor {
    private ByteBuffer input;
    private long pending;
    private int size, previous, leading, trailing;
    private boolean first;

    public void reset(ByteBuffer input) {
      this.input = input;
      pending = 0;
      size = previous = trailing = 0;
      leading = 16;
      first = true;
    }

    private int get(int width) throws IOException {
      while (size < width) {
        if (!input.hasRemaining()) {
          throw FloatLayerFrame.invalid("gorilla16-truncated");
        }
        pending = (pending << 8) | (input.get() & 0xff);
        size += 8;
      }
      size -= width;
      int value = (int) ((pending >>> size) & ((1L << width) - 1));
      pending &= (1L << size) - 1;
      return value;
    }

    public int next() throws IOException {
      if (first) {
        previous = get(16);
        first = false;
      } else if (get(1) != 0) {
        if (get(1) != 0) {
          leading = get(4);
          int width = get(4) + 1;
          trailing = 16 - leading - width;
          if (trailing < 0) {
            throw FloatLayerFrame.invalid("gorilla16-window");
          }
        } else if (leading == 16) {
          throw FloatLayerFrame.invalid("gorilla16-window");
        }
        previous ^= get(16 - leading - trailing) << trailing;
      }
      return previous;
    }
  }
}
