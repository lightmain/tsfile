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
import org.apache.tsfile.encoding.floatlayer.LinearResidualStream;
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
  @Test
  public void testCoalescedInteriorRangesAndReuse() throws Exception {
    int[] bits = new Random(20261102).ints(4096).toArray();
    for (boolean rounded : new boolean[] {false, true}) {
      for (boolean original : new boolean[] {false, true}) {
        byte[] frame =
            original
                ? FloatLayerFrame.encodeOriginal(bits, bits.length, 256, rounded)
                : FloatLayerFrame.encode(bits, bits.length, 256, rounded);
        int base = 4 + 512;
        byte[] page = new byte[base + frame.length];
        ByteBuffer.wrap(page).putInt(bits.length);
        Arrays.fill(page, 4, base, (byte) 0xff);
        System.arraycopy(frame, 0, page, base, frame.length);
        var session = new FloatPageRangeReader.Session(true, true);
        int[] output = new int[4096];
        for (int[] range : new int[][] {{779, 900}, {0, 4096}, {4090, 6}, {1001, 3}, {4096, 0}}) {
          for (boolean coarse : new boolean[] {false, true}) {
            session.readInto(
                (offset, target, at, n) -> System.arraycopy(page, (int) offset, target, at, n),
                page.length,
                bits.length,
                range[0],
                range[1],
                coarse,
                output,
                0);
            assertEquals(range[1] == 0 ? 0 : (!coarse && !original ? 4 : 3), session.readCalls);
            for (int i = 0; i < range[1]; i++) {
              assertEquals(
                  coarse ? FloatLayerFrame.coarse(bits[range[0] + i], rounded) : bits[range[0] + i],
                  output[i]);
            }
          }
        }
      }
    }
  }

  @Test
  public void testSparseCoalescedRankAndTailStop() throws Exception {
    int[] bits = {0x3f800000, 0x40000000, 0x40400000, 0x40800000};
    byte[] frame = FloatLayerFrame.encodeOriginal(bits, 4, 4);
    byte[] page =
        ByteBuffer.allocate(5 + frame.length).putInt(8).put((byte) 0b10110100).put(frame).array();
    int[] output = new int[8];
    var session = new FloatPageRangeReader.Session(true, true);
    session.readInto(
        (offset, target, at, n) -> System.arraycopy(page, (int) offset, target, at, n),
        page.length,
        8,
        1,
        4,
        false,
        output,
        0);
    assertArrayEquals(new int[] {0, bits[1], bits[2], 0}, Arrays.copyOf(output, 4));
    assertEquals(3, session.decodedValues);
    session.readInto(
        (offset, target, at, n) -> {
          throw new IOException();
        },
        0,
        8,
        0,
        8,
        false,
        output,
        0);
    assertArrayEquals(new int[8], output);
  }

  private FloatLayerFrame.ReadAt memory(byte[] frame) {
    return (offset, size) -> Arrays.copyOfRange(frame, (int) offset, (int) offset + size);
  }

  @Test
  public void testCompactFineEveryHighWordAndRoundingBoundary() throws Exception {
    int[] lows = {0, 1, 0x7fff, 0x8000, 0x8001, 0xffff};
    int[] bits = new int[65536 * lows.length];
    for (int high = 0; high < 65536; high++) {
      for (int i = 0; i < lows.length; i++) {
        bits[high * lows.length + i] = (high << 16) | lows[i];
      }
    }
    for (boolean round : new boolean[] {false, true}) {
      for (boolean linear : new boolean[] {false, true}) {
        byte[] frame =
            linear
                ? FloatLayerFrame.encodeLinear(bits, bits.length, 1024, round)
                : FloatLayerFrame.encode(bits, bits.length, 1024, round);
        assertArrayEquals(bits, FloatLayerFrame.decode(frame));
        var directory = FloatLayerFrame.directory(memory(frame), frame.length);
        assertEquals(linear ? 12 : 16, directory.format());
        for (int start : new int[] {0, 1019, 196607, bits.length - 7}) {
          int length = Math.min(29, bits.length - start);
          assertArrayEquals(
              Arrays.copyOfRange(bits, start, start + length),
              FloatLayerFrame.readRange(memory(frame), directory, start, length, false));
        }
      }
    }
  }

  @Test
  public void testCompactFineUsesTwoBytesAndCoarseSkipsCorrections() throws Exception {
    int[] bits = new int[257];
    for (int i = 0; i < bits.length; i++) {
      bits[i] = 0x3f800001 + i * 179;
    }
    for (boolean linear : new boolean[] {false, true}) {
      byte[] frame =
          linear
              ? FloatLayerFrame.encodeLinear(bits, bits.length, 256, false)
              : FloatLayerFrame.encode(bits, bits.length, 256, false);
      var directory = FloatLayerFrame.directory(memory(frame), frame.length);
      for (var entry : directory.entries()) {
        assertEquals(entry.count() * 2 + 4, entry.fineRaw());
      }
      FloatLayerFrame.ReadAt observed =
          (offset, size) -> {
            for (var entry : directory.entries()) {
              if (offset < entry.fineOffset() + entry.fineLength()
                  && offset + size > entry.fineOffset()) {
                fail("Coarse range touched compact fine data");
              }
            }
            return memory(frame).read(offset, size);
          };
      int[] actual = FloatLayerFrame.readRange(observed, directory, 250, 7, true);
      for (int i = 0; i < actual.length; i++) {
        assertEquals(FloatLayerFrame.coarse(bits[250 + i], false), actual[i]);
      }
    }
  }

  private byte[] legacyFrame(int[] bits, boolean linear, boolean round) throws Exception {
    int n = bits.length;
    ByteBuffer highRaw = ByteBuffer.allocate(n * 2), lowRaw = ByteBuffer.allocate(n * 4);
    int[] highWords = new int[n], lowWords = new int[n];
    for (int i = 0; i < n; i++) {
      int high = FloatLayerFrame.coarse(bits[i], round);
      highRaw.putShort((short) high);
      lowRaw.putInt(bits[i] ^ (high << 16));
      highWords[i] = (short) high;
      lowWords[i] = bits[i] ^ (high << 16);
    }
    var compressor = ICompressor.getCompressor(CompressionType.LZ4);
    byte[] high =
        linear ? LinearResidualStream.encode(highWords) : compressor.compress(highRaw.array());
    byte[] low =
        linear ? LinearResidualStream.encode(lowWords) : compressor.compress(lowRaw.array());
    int length = 56 + high.length + low.length;
    return ByteBuffer.allocate(length)
        .putInt(FloatLayerFrame.MAGIC)
        .putInt((linear ? 4 : 0) + (round ? 1 : 0))
        .putInt(n)
        .putInt(n)
        .putInt(1)
        .putInt(length)
        .putInt(0)
        .putInt(n)
        .putInt(56)
        .putInt(high.length)
        .putInt(n * 2)
        .putInt(56 + high.length)
        .putInt(low.length)
        .putInt(n * 4)
        .put(high)
        .put(low)
        .array();
  }

  @Test
  public void testLegacyFourByteFilesRemainReadable() throws Exception {
    int[] bits = {0, 0x80000000, 0x3f818000, 0x7f7fffff, 0x7f800001, 0xffa12345};
    for (boolean linear : new boolean[] {false, true}) {
      for (boolean round : new boolean[] {false, true}) {
        byte[] frame = legacyFrame(bits, linear, round);
        assertArrayEquals(bits, FloatLayerFrame.decode(frame));
      }
    }
  }

  @Test
  public void testMalformedCompactCorrectionsAreRejected() throws Exception {
    int[] bits = {0x3f818000, 0x7f800001, 0xffa12345};
    byte[] frame = FloatLayerFrame.encodeLinear(bits, bits.length, bits.length, true);
    var directory = FloatLayerFrame.directory(memory(frame), frame.length);
    var entry = directory.entries().get(0);
    int encoded = ByteBuffer.wrap(frame).getInt(entry.fineOffset());
    int correctionOffset = entry.fineOffset() + 4 + encoded;
    for (int offset : new int[] {entry.fineOffset(), correctionOffset, correctionOffset + 4}) {
      byte[] broken = frame.clone();
      ByteBuffer.wrap(broken).putInt(offset, -1);
      try {
        FloatLayerFrame.decode(broken);
        fail("Accepted malformed compact correction");
      } catch (IOException expected) {
        assertNotNull(expected.getMessage());
      }
    }
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
