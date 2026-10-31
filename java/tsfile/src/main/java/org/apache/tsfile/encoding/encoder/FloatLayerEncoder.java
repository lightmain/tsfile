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
package org.apache.tsfile.encoding.encoder;

import org.apache.tsfile.encoding.floatlayer.FloatLayerFrame;
import org.apache.tsfile.file.metadata.enums.TSEncoding;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

/** Buffer raw FP32 bits until the native page is sealed. */
public class FloatLayerEncoder extends Encoder {
  private int[] bits = new int[1024];
  private int count;
  private final int unit;
  private final boolean roundEven;

  public FloatLayerEncoder(int unit, boolean roundEven) {
    super(TSEncoding.BF16_LAYERED);
    if (unit < 1) {
      throw new IllegalArgumentException(
          org.apache.tsfile.i18n.Messages.format("error.encoding.bf16_invalid", "unit"));
    }
    this.unit = unit;
    this.roundEven = roundEven;
  }

  @Override
  public void encode(float value, ByteArrayOutputStream out) {
    if (count == bits.length) {
      bits = Arrays.copyOf(bits, Math.multiplyExact(count, 2));
    }
    bits[count++] = Float.floatToRawIntBits(value);
  }

  @Override
  public void flush(ByteArrayOutputStream out) throws IOException {
    out.write(FloatLayerFrame.encode(bits, count, unit, roundEven));
    count = 0;
  }

  @Override
  public int getOneItemMaxSize() {
    return 8;
  }

  @Override
  public long getMaxByteSize() {
    return bits.length * 4L + count * 8L + 64;
  }
}
