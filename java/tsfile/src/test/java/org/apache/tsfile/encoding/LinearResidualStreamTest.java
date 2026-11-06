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

import org.apache.tsfile.encoding.floatlayer.FloatLayerFrame;
import org.apache.tsfile.encoding.floatlayer.LinearResidualStream;

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class LinearResidualStreamTest {
  @Test
  public void testReusableRangeReadsOnlyTargetBytes() throws Exception {
    int[] values = new java.util.Random(77).ints(4097).toArray();
    byte[] encoded = LinearResidualStream.encode(values);
    var workspace = new LinearResidualStream.Workspace();
    for (int[] range : new int[][] {{0, values.length}, {1001, 3}, {4096, 1}, {4097, 0}}) {
      int[] output = new int[range[1] + 2];
      LinearResidualStream.readRangeInto(
          (offset, target, at, n) -> {
            if (offset == 0) {
              assertEquals(9, n);
            } else {
              assertEquals(9 + range[0] * 4L, offset);
              assertEquals(range[1] * 4, n);
            }
            System.arraycopy(encoded, (int) offset, target, at, n);
          },
          0,
          encoded.length,
          values.length,
          range[0],
          range[1],
          output,
          1,
          workspace);
      assertArrayEquals(
          java.util.Arrays.copyOfRange(values, range[0], range[0] + range[1]),
          java.util.Arrays.copyOfRange(output, 1, 1 + range[1]));
    }
    byte[] zeroWidth = LinearResidualStream.encode(new int[] {10, 12, 14, 16});
    int[] output = new int[2];
    LinearResidualStream.readRangeInto(
        (offset, target, at, n) -> {
          assertEquals(0, offset);
          assertEquals(9, n);
          System.arraycopy(zeroWidth, 0, target, at, n);
        },
        0,
        zeroWidth.length,
        4,
        1,
        2,
        output,
        0,
        workspace);
    assertArrayEquals(new int[] {12, 14}, output);
  }

  @Test
  public void testIntegerWrapAndEveryRequestedStart() throws Exception {
    int[][] samples = {
      new int[] {Integer.MIN_VALUE},
      new int[] {Integer.MAX_VALUE, Integer.MIN_VALUE},
      new int[] {0, 1, 0, 1, 0, 1, 0},
      new Random(111).ints(257).toArray()
    };
    for (int[] values : samples) {
      byte[] encoded = LinearResidualStream.encode(values);
      FloatLayerFrame.ReadAt source =
          (offset, size) -> Arrays.copyOfRange(encoded, (int) offset, (int) offset + size);
      assertArrayEquals(
          values,
          LinearResidualStream.readRange(
              source, 0, encoded.length, values.length, 0, values.length));
      for (int start = 0; start < values.length; start++) {
        int length = Math.min(13, values.length - start);
        assertArrayEquals(
            Arrays.copyOfRange(values, start, start + length),
            LinearResidualStream.readRange(
                source, 0, encoded.length, values.length, start, length));
      }
    }
  }

  @Test
  public void testNoEarlierResidualBytesRead() throws Exception {
    int[] values = new Random(71).ints(2048).toArray();
    byte[] encoded = LinearResidualStream.encode(values);
    int width = encoded[8] & 0xff, start = 900, length = 17;
    int firstByte = (start * width) / 8;
    int endByte = ((start + length) * width + 7) / 8;
    int[] calls = {0};
    FloatLayerFrame.ReadAt observed =
        (offset, size) -> {
          calls[0]++;
          if (offset == 0) {
            assertEquals(9, size);
          } else {
            assertEquals(9 + firstByte, offset);
            assertEquals(endByte - firstByte, size);
          }
          return Arrays.copyOfRange(encoded, (int) offset, (int) offset + size);
        };
    assertArrayEquals(
        Arrays.copyOfRange(values, start, start + length),
        LinearResidualStream.readRange(observed, 0, encoded.length, values.length, start, length));
    assertEquals(2, calls[0]);
  }

  @Test
  public void testZeroWidthOnlyReadsParameters() throws Exception {
    int[] values = new int[1024];
    for (int i = 0; i < values.length; i++) {
      values[i] = Integer.MAX_VALUE - 40 * i;
    }
    byte[] encoded = LinearResidualStream.encode(values);
    assertEquals(9, encoded.length);
    FloatLayerFrame.ReadAt reader =
        (offset, length) -> {
          assertEquals(0, offset);
          assertEquals(9, length);
          return encoded.clone();
        };
    assertArrayEquals(
        Arrays.copyOfRange(values, 513, 519),
        LinearResidualStream.readRange(reader, 0, encoded.length, values.length, 513, 6));
  }

  @Test
  public void testMalformedWidthAndLengthAreRejected() throws Exception {
    byte[] encoded = LinearResidualStream.encode(new int[] {1, 4, 2, 9});
    encoded[8] = 33;
    try {
      LinearResidualStream.readRange(
          (offset, length) -> Arrays.copyOfRange(encoded, (int) offset, (int) offset + length),
          0,
          encoded.length,
          4,
          0,
          1);
      fail("Accepted malformed bit width");
    } catch (IOException expected) {
      assertNotNull(expected.getMessage());
    }
    try {
      LinearResidualStream.readRange((offset, length) -> new byte[8], 0, 9, 4, 0, 1);
      fail("Accepted short header");
    } catch (IOException expected) {
      assertTrue(expected.getMessage().length() > 0);
    }
  }
}
