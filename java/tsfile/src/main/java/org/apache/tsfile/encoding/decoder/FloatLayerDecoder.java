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
package org.apache.tsfile.encoding.decoder;

import org.apache.tsfile.encoding.floatlayer.FloatLayerFrame;
import org.apache.tsfile.exception.encoding.TsFileDecodingException;
import org.apache.tsfile.file.metadata.enums.TSEncoding;

import java.io.IOException;
import java.nio.ByteBuffer;

/** Ordinary query compatibility for exact complete-layer reads. */
public class FloatLayerDecoder extends Decoder {
  private int[] bits;
  private int cursor;

  public FloatLayerDecoder() {
    this(TSEncoding.BF16_LAYERED);
  }

  public FloatLayerDecoder(TSEncoding type) {
    super(type);
  }

  private void load(ByteBuffer buffer) {
    if (bits == null) {
      byte[] frame = new byte[buffer.remaining()];
      buffer.get(frame);
      try {
        bits = FloatLayerFrame.decode(frame);
      } catch (IOException e) {
        throw new TsFileDecodingException(e.getMessage());
      }
    }
  }

  @Override
  public float readFloat(ByteBuffer buffer) {
    load(buffer);
    return Float.intBitsToFloat(bits[cursor++]);
  }

  @Override
  public boolean hasNext(ByteBuffer buffer) {
    load(buffer);
    return cursor < bits.length;
  }

  @Override
  public void reset() {
    bits = null;
    cursor = 0;
  }
}
