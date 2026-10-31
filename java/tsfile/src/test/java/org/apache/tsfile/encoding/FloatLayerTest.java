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
package org.apache.tsfile.encoding;

import org.apache.tsfile.compress.ICompressor;
import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.encoding.encoder.Encoder;
import org.apache.tsfile.encoding.encoder.FloatLayerEncoder;
import org.apache.tsfile.encoding.floatlayer.FloatLayerFrame;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.reader.page.FloatPageRangeReader;
import org.apache.tsfile.write.page.ValuePageWriter;
import org.apache.tsfile.write.schema.MeasurementSchema;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FloatLayerTest {
  private FloatLayerFrame.ReadAt memory(byte[] frame) {
    return (offset, size) -> Arrays.copyOfRange(frame, (int) offset, (int) offset + size);
  }

  @Test
  public void testRandomBitRecoveryBothRulesAndUnits() throws Exception {
    int[] bits = new Random(20261101).ints(65539).toArray();
    int[] specials = {
      0,
      0x80000000,
      1,
      0x80000001,
      0x7f800000,
      0xff800000,
      0x7f800001,
      0x7fc12345,
      0xffc54321,
      0x7f7fffff,
      0x3f808000,
      0x3f818000
    };
    System.arraycopy(specials, 0, bits, 0, specials.length);
    for (boolean round : new boolean[] {false, true}) {
      for (int unit : new int[] {256, 4096, Integer.MAX_VALUE}) {
        byte[] frame = FloatLayerFrame.encode(bits, bits.length, unit, round);
        assertArrayEquals(bits, FloatLayerFrame.decode(frame));
        var directory = FloatLayerFrame.directory(memory(frame), frame.length);
        int[] coarse = FloatLayerFrame.readRange(memory(frame), directory, 250, 8200, true);
        for (int i = 0; i < coarse.length; i++) {
          assertEquals(FloatLayerFrame.coarse(bits[i + 250], round), coarse[i]);
        }
      }
    }
  }

  @Test
  public void testExplicitRoundingAndSpecialCodes() {
    assertEquals(0x3f80, FloatLayerFrame.coarse(0x3f808000, true));
    assertEquals(0x3f82, FloatLayerFrame.coarse(0x3f818000, true));
    assertEquals(0x3f81, FloatLayerFrame.coarse(0x3f818000, false));
    assertEquals(0x7fc0, FloatLayerFrame.coarse(0x7f800001, false));
    assertEquals(0xffc0, FloatLayerFrame.coarse(0xffa12345, true));
    assertEquals(0x8000, FloatLayerFrame.coarse(0x80000000, true));
    assertEquals(0x7f80, FloatLayerFrame.coarse(0x7f7fffff, true));
  }

  @Test
  public void testOriginalSubpageRecoveryAndSelectiveUnitReads() throws Exception {
    int[] bits = new Random(131).ints(1027).toArray();
    bits[9] =
        Float.floatToRawIntBits(
            org.apache.tsfile.common.conf.TSFileConfig.GORILLA_ENCODING_ENDING_FLOAT);
    byte[] frame = FloatLayerFrame.encodeOriginal(bits, bits.length, 256);
    assertArrayEquals(bits, FloatLayerFrame.decode(frame));
    var directory = FloatLayerFrame.directory(memory(frame), frame.length);
    assertEquals(2, directory.format());
    var selected = directory.entries().get(1);
    FloatLayerFrame.ReadAt observed =
        (offset, length) -> {
          if (offset
              >= FloatLayerFrame.HEADER_BYTES
                  + directory.entries().size() * FloatLayerFrame.ENTRY_BYTES) {
            assertEquals(selected.coarseOffset(), offset);
          }
          return memory(frame).read(offset, length);
        };
    assertArrayEquals(
        Arrays.copyOfRange(bits, 300, 330),
        FloatLayerFrame.readRange(observed, directory, 300, 30, false));
    assertTrue(TSEncoding.isSupported(TSDataType.FLOAT, TSEncoding.FLOAT_SUBPAGE));
    assertFalse(TSEncoding.isSupported(TSDataType.DOUBLE, TSEncoding.FLOAT_SUBPAGE));
  }

  @Test
  public void testLinearLayerRecoveryForArbitraryWordsAndRules() throws Exception {
    int[] bits = new Random(731).ints(8195).toArray();
    bits[0] = 0x7f800001;
    bits[1] = 0x80000000;
    bits[2] = 0x7f7fffff;
    for (boolean rounded : new boolean[] {false, true}) {
      byte[] frame = FloatLayerFrame.encodeLinear(bits, bits.length, 1024, rounded);
      assertArrayEquals(bits, FloatLayerFrame.decode(frame));
      var directory = FloatLayerFrame.directory(memory(frame), frame.length);
      int[] coarse = FloatLayerFrame.readRange(memory(frame), directory, 999, 2099, true);
      for (int i = 0; i < coarse.length; i++) {
        assertEquals(FloatLayerFrame.coarse(bits[999 + i], rounded), coarse[i]);
      }
      var schema =
          new MeasurementSchema(
              "value",
              TSDataType.FLOAT,
              TSEncoding.LINEAR_RESIDUAL,
              CompressionType.UNCOMPRESSED,
              Map.of("bf16_rule", rounded ? "round_nearest_even" : "truncate"));
      Encoder encoder = schema.getValueEncoder();
      ByteArrayOutputStream output = new ByteArrayOutputStream();
      for (int word : bits) {
        encoder.encode(Float.intBitsToFloat(word), output);
      }
      encoder.flush(output);
      assertArrayEquals(bits, FloatLayerFrame.decode(output.toByteArray()));
    }
    assertFalse(TSEncoding.isSupported(TSDataType.DOUBLE, TSEncoding.LINEAR_RESIDUAL));
    assertEquals(TSEncoding.LINEAR_RESIDUAL, TSEncoding.deserialize((byte) 17));
  }

  @Test
  public void testOrdinaryEncoderDecoderReuseAndTypeSupport() throws Exception {
    assertTrue(TSEncoding.isSupported(TSDataType.FLOAT, TSEncoding.BF16_LAYERED));
    assertFalse(TSEncoding.isSupported(TSDataType.DOUBLE, TSEncoding.BF16_LAYERED));
    assertEquals(TSEncoding.BF16_LAYERED, TSEncoding.deserialize((byte) 15));
    var schema =
        new MeasurementSchema(
            "value",
            TSDataType.FLOAT,
            TSEncoding.BF16_LAYERED,
            CompressionType.UNCOMPRESSED,
            Map.of("bf16_rule", "round_nearest_even"));
    Encoder encoder = schema.getValueEncoder();
    Decoder decoder = Decoder.getDecoderByType(TSEncoding.BF16_LAYERED, TSDataType.FLOAT);
    for (int count : new int[] {17, 1, 8200}) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      for (int i = 0; i < count; i++) {
        encoder.encode(Float.intBitsToFloat(0x3f800000 + i), out);
      }
      encoder.flush(out);
      ByteBuffer frame = ByteBuffer.wrap(out.toByteArray());
      for (int i = 0; i < count; i++) {
        assertTrue(decoder.hasNext(frame));
        assertEquals(0x3f800000 + i, Float.floatToRawIntBits(decoder.readFloat(frame)));
      }
      assertFalse(decoder.hasNext(frame));
      decoder.reset();
    }
  }

  @Test
  public void testSparseLogicalRangeSkipsEveryFinePayload() throws Exception {
    int[] bits = {0, 0x80000000, 0x7f800001, 0x3f818000, 0x7f7fffff};
    byte[] frame = FloatLayerFrame.encode(bits, bits.length, 2, true);
    var directory = FloatLayerFrame.directory(memory(frame), frame.length);
    byte[] bitmap = {(byte) 0b10110101};
    ByteBuffer page = ByteBuffer.allocate(5 + frame.length).putInt(8).put(bitmap).put(frame);
    FloatLayerFrame.ReadAt coarseReader =
        (offset, size) -> {
          for (var entry : directory.entries()) {
            long fineStart = 5L + entry.fineOffset();
            if (offset < fineStart + entry.fineLength() && offset + size > fineStart) {
              fail("Coarse read touched the fine layer");
            }
          }
          return Arrays.copyOfRange(page.array(), (int) offset, (int) offset + size);
        };
    var result = FloatPageRangeReader.read(coarseReader, page.capacity(), 1, 6, true);
    assertArrayEquals(new boolean[] {false, true, true, false, true, false}, result.valid());
    assertArrayEquals(new int[] {0, 0x8000, 0x7fc0, 0, 0x3f82, 0}, result.values());
    var full = FloatPageRangeReader.read(memory(page.array()), page.capacity(), 0, 8, false);
    assertArrayEquals(
        new int[] {0, 0, 0x80000000, 0x7f800001, 0, 0x3f818000, 0, 0x7f7fffff}, full.values());
    assertEquals(
        0, FloatPageRangeReader.read(coarseReader, page.capacity(), 8, 0, true).values().length);
  }

  @Test
  public void testMalformedHeadersAndRangesAreRejected() throws Exception {
    byte[] frame = FloatLayerFrame.encode(new int[] {1, 2, 3}, 3, 2, false);
    for (int at : new int[] {0, 4, 8, 12, 16, 20, 24, 32}) {
      byte[] broken = frame.clone();
      ByteBuffer.wrap(broken).putInt(at, -1);
      try {
        FloatLayerFrame.decode(broken);
        fail("Accepted malformed field at " + at);
      } catch (IOException expected) {
        assertNotNull(expected.getMessage());
      }
    }
    var directory = FloatLayerFrame.directory(memory(frame), frame.length);
    try {
      FloatLayerFrame.readRange(memory(frame), directory, 2, Integer.MAX_VALUE, true);
      fail("Accepted invalid range");
    } catch (IOException expected) {
      assertNotNull(expected.getMessage());
    }
  }

  @Test
  public void testOuterCompressionRejectedAtSchemaAndPageBoundaries() {
    try {
      new MeasurementSchema("value", TSDataType.FLOAT, TSEncoding.BF16_LAYERED, CompressionType.LZ4)
          .getValueEncoder();
      fail("Accepted outer compression");
    } catch (IllegalArgumentException expected) {
      assertNotNull(expected.getMessage());
    }
    try {
      new ValuePageWriter(
          new FloatLayerEncoder(Integer.MAX_VALUE, false),
          ICompressor.getCompressor(CompressionType.LZ4),
          TSDataType.FLOAT);
      fail("Accepted outer compression");
    } catch (IllegalArgumentException expected) {
      assertNotNull(expected.getMessage());
    }
  }
}
