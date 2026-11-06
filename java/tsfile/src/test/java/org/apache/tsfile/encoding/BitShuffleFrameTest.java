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

import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.encoding.floatlayer.BitShuffleFrame;
import org.apache.tsfile.encoding.floatlayer.FloatLayerFrame;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.reader.page.BitShufflePageRangeReader;
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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class BitShuffleFrameTest {
  private static final int[] DEGREES = {1, 2, 4, 8, 16, 32};

  @Test
  public void testArbitraryWordsAndPaddingRoundTrip() throws Exception {
    int[] special = {
      0,
      0x80000000,
      0x7f800000,
      0xff800000,
      0x7f800001,
      0xff800001,
      0x7fc12345,
      0xffc12345,
      1,
      0x80000001,
      0x7f7fffff,
      0xff7fffff
    };
    for (int degree : DEGREES) {
      for (int[] words :
          new int[][] {
            special, new Random(27).ints(2053).toArray(), new int[0], new int[1], new int[1031]
          }) {
        for (int unit : new int[] {1, 7, 256, 8192, Integer.MAX_VALUE}) {
          byte[] frame = BitShuffleFrame.encode(words, words.length, unit, degree);
          assertArrayEquals(words, BitShuffleFrame.decode(frame));
          assertArrayEquals(words, FloatLayerFrame.decode(frame));
        }
      }
    }
  }

  @Test
  public void testAllHighWordsAndLowBoundaries() throws Exception {
    int[] words = new int[65536];
    for (int i = 0; i < words.length; i++) {
      words[i] = (i << 16) | ((i & 1) == 0 ? 0x8000 : 0xffff);
    }
    for (int degree : DEGREES) {
      assertArrayEquals(
          words, BitShuffleFrame.decode(BitShuffleFrame.encode(words, words.length, 1023, degree)));
    }
  }

  @Test
  public void testSparseRangesAndCrossDegreeWorkspaceReuse() throws Exception {
    int[] words = new Random(32).ints(2053).toArray();
    words[1] = 0x7f800001;
    int rows = words.length * 2, base = 4 + (rows + 7) / 8;
    var session = new BitShufflePageRangeReader.Session();
    for (int degree : DEGREES) {
      byte[] frame = BitShuffleFrame.encode(words, words.length, 257, degree);
      var directory = BitShuffleFrame.directory(ByteBuffer.wrap(frame), frame.length);
      byte[] page = new byte[base + frame.length];
      ByteBuffer.wrap(page).putInt(rows);
      for (int row = 0; row < rows; row += 2) {
        page[4 + row / 8] |= (byte) (1 << (7 - row % 8));
      }
      System.arraycopy(frame, 0, page, base, frame.length);
      for (int[] range :
          new int[][] {
            {0, rows}, {511, 1113}, {rows - 9, 9}, {999, 3}, {2, 1}, {1, 1}, {rows, 0}
          }) {
        for (boolean coarse : new boolean[] {true, false}) {
          for (boolean profile : new boolean[] {true, false}) {
            int[] output = new int[range[1] + 10];
            Arrays.fill(output, -17);
            long[] calls = {0};
            session.readInto(
                (offset, target, at, length) -> {
                  calls[0]++;
                  if (coarse && calls[0] == 3 && degree > 1) {
                    int lowStart = directory.units().get(0).lowOffset();
                    assertTrue(offset + length <= base + lowStart);
                  }
                  System.arraycopy(page, (int) offset, target, at, length);
                },
                page.length,
                rows,
                range[0],
                range[1],
                coarse,
                output,
                5,
                profile);
            for (int i = 0; i < range[1]; i++) {
              int row = range[0] + i;
              int expected =
                  (row & 1) == 1
                      ? 0
                      : coarse ? FloatLayerFrame.coarse(words[row / 2], false) : words[row / 2];
              assertEquals(expected, output[5 + i]);
            }
            assertEquals(-17, output[4]);
            assertEquals(-17, output[5 + range[1]]);
            assertEquals(calls[0], session.readCalls);
            if (!profile || degree < 4) {
              assertEquals(0, session.transposeNs);
            }
          }
        }
      }
    }
  }

  @Test
  public void testDegreeTwoRetainsExistingBytes() throws Exception {
    int[] words = new Random(2).ints(10003).toArray();
    for (int unit : new int[] {256, 8192, Integer.MAX_VALUE}) {
      assertArrayEquals(
          FloatLayerFrame.encode(words, words.length, unit, false),
          BitShuffleFrame.encode(words, words.length, unit, 2));
    }
    byte[] legacy = BitShuffleFrame.encode(words, words.length, Integer.MAX_VALUE, 2);
    ByteBuffer.wrap(legacy).putInt(4, 8);
    assertArrayEquals(words, BitShuffleFrame.decode(legacy));
  }

  @Test
  public void testOrdinaryDecoderAndSchemaSelection() throws Exception {
    int[] words = {0, 0x80000000, 0x7f800001, 0xffc12345, 0x3f818000, 0xdeadbeef};
    for (int degree : DEGREES) {
      var schema =
          new MeasurementSchema(
              "value",
              TSDataType.FLOAT,
              TSEncoding.BF16_LAYERED,
              CompressionType.UNCOMPRESSED,
              Map.of("float_bitshuffle_degree", Integer.toString(degree), "float_layer_unit", "3"));
      var encoder = schema.getValueEncoder();
      var bytes = new ByteArrayOutputStream();
      for (int word : words) {
        encoder.encode(Float.intBitsToFloat(word), bytes);
      }
      encoder.flush(bytes);
      Decoder decoder = Decoder.getDecoderByType(TSEncoding.BF16_LAYERED, TSDataType.FLOAT);
      ByteBuffer input = ByteBuffer.wrap(bytes.toByteArray());
      for (int word : words) {
        assertTrue(decoder.hasNext(input));
        assertEquals(word, Float.floatToRawIntBits(decoder.readFloat(input)));
      }
      assertTrue(!decoder.hasNext(input));
    }
  }

  @Test
  public void testRejectInvalidDirectoryAndAllMissingPage() throws Exception {
    byte[] valid = BitShuffleFrame.encode(new int[1031], 1031, 256, 32);
    for (int[] edit :
        new int[][] {
          {4, 67},
          {16, Integer.MAX_VALUE},
          {20, valid.length + 1},
          {24 + 4, -1},
          {24 + 8, Integer.MAX_VALUE},
          {24 + 28, 0},
          {24 + 32, -1}
        }) {
      byte[] changed = valid.clone();
      ByteBuffer.wrap(changed).putInt(edit[0], edit[1]);
      try {
        BitShuffleFrame.decode(changed);
        fail("Expected invalid directory");
      } catch (IOException expected) {
        assertTrue(expected.getMessage().length() > 0);
      }
    }
    try {
      BitShuffleFrame.decode(Arrays.copyOf(valid, valid.length - 1));
      fail("Expected truncated frame");
    } catch (IOException expected) {
      assertTrue(expected.getMessage().length() > 0);
    }
    var session = new BitShufflePageRangeReader.Session();
    int[] output = new int[9];
    session.readInto(
        (offset, target, at, length) -> {
          throw new IOException("All missing page must not read");
        },
        0,
        9,
        0,
        9,
        true,
        output,
        0,
        false);
    assertArrayEquals(new int[9], output);
    assertEquals(0, session.readCalls);
  }
}
