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
  private final FloatLayerFrame.CoarseCodec coarseCodec;

  public FloatLayerEncoder(int unit, boolean roundEven) {
    this(TSEncoding.BF16_LAYERED, unit, roundEven);
  }

  public FloatLayerEncoder(TSEncoding type, int unit, boolean roundEven) {
    this(type, unit, roundEven, false);
  }

  public FloatLayerEncoder(TSEncoding type, int unit, boolean roundEven, boolean coarseGorilla) {
    this(
        type,
        unit,
        roundEven,
        coarseGorilla ? FloatLayerFrame.CoarseCodec.GORILLA : FloatLayerFrame.CoarseCodec.LZ4);
  }

  public FloatLayerEncoder(
      TSEncoding type, int unit, boolean roundEven, FloatLayerFrame.CoarseCodec coarseCodec) {
    super(type);
    if (type != TSEncoding.BF16_LAYERED
        && type != TSEncoding.FLOAT_SUBPAGE
        && type != TSEncoding.LINEAR_RESIDUAL) {
      throw new IllegalArgumentException(
          org.apache.tsfile.i18n.Messages.format("error.encoding.bf16_invalid", "type"));
    }
    if (unit < 1) {
      throw new IllegalArgumentException(
          org.apache.tsfile.i18n.Messages.format("error.encoding.bf16_invalid", "unit"));
    }
    this.unit = unit;
    this.roundEven = roundEven;
    if (coarseCodec == null
        || (coarseCodec != FloatLayerFrame.CoarseCodec.LZ4 && type != TSEncoding.BF16_LAYERED)) {
      throw new IllegalArgumentException(
          org.apache.tsfile.i18n.Messages.format("error.encoding.bf16_invalid", "coarse-codec"));
    }
    this.coarseCodec = coarseCodec;
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
    out.write(
        switch (getType()) {
          case FLOAT_SUBPAGE -> FloatLayerFrame.encodeOriginal(bits, count, unit, roundEven);
          case LINEAR_RESIDUAL -> FloatLayerFrame.encodeLinear(bits, count, unit, roundEven);
          default -> FloatLayerFrame.encode(bits, count, unit, roundEven, coarseCodec);
        });
    count = 0;
  }

  @Override
  public int getOneItemMaxSize() {
    return 8;
  }

  @Override
  public long getMaxByteSize() {
    long units = (count + (long) unit - 1) / unit;
    return bits.length * 4L + count * 12L + units * 64 + 128;
  }
}
