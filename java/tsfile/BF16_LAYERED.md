<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Native BF16 layered FLOAT pages

Experimental Java format extension, encoding ID 15 (`BF16_LAYERED`). BF16 is a
16-bit floating-point representation with eight exponent bits. FP32 is the
original 32-bit floating-point representation. Existing encoding IDs are
unchanged. Readers without ID 15 reject its chunk header; C++ and Python source
implementations are not extended here.

Use FLOAT with outer compression UNCOMPRESSED. The aligned page retains the
native big-endian row count and most-significant-bit-first validity bitmap.
The value stream contains the frame below. Full ordinary queries use
`FloatLayerDecoder`; `FloatPageRangeReader` accepts a logical row start and
length, maps them through the bitmap, and accesses only overlapping streams.
All-null native value pages have no payload and must be handled by the caller
using the corresponding time-page row count.

All integers and BF16 codes in the frame are big-endian. Offsets are relative to
the beginning of the frame, after the native row count and bitmap.

| Header field | Width | Value |
|---|---:|---|
| Magic | 4 | `0x42463136` |
| Flags | 4 | 0: truncate; 1: round to nearest, ties to even |
| Present count | 4 | Number of encoded non-null values |
| Unit size | 4 | Maximum present values per independent unit |
| Unit count | 4 | Ceiling of present count divided by unit size |
| Frame bytes | 4 | Complete header, directory and payload size |

Each 32-byte directory entry contains eight 32-bit integers: present-value
start, count, coarse offset, coarse compressed bytes, coarse raw bytes, fine
offset, fine compressed bytes, fine raw bytes. Units and their two streams are
stored consecutively. Header counts, directory continuity, exact lengths and
range bounds are validated before accessing payloads.

The coarse raw stream contains one unsigned BF16 code per value (two bytes).
The fine raw stream contains the original FP32 word XOR the coarse code shifted
left by 16 (four bytes). Each stream is compressed independently with LZ4, a
general-purpose byte compressor. Full recovery XORs these words again, including
rounding carry and arbitrary NaN payloads. NaN means a non-number floating-point
encoding: coarse output preserves its sign and canonicalizes its payload to a
quiet NaN; exact recovery retains the original word. Signed zero and infinity
are preserved. Finite overflow under rounding is explicit and recoverable.

Properties: `bf16_rule=truncate|round_nearest_even`; `float_layer_unit` is a
positive present-value count (default `Integer.MAX_VALUE`, one unit per page).
Changing unit boundaries is separate from changing precision. Time chunks and
logical row ordering are retained; values are located by the shared row bitmap.
Encrypted payloads are not supported by the direct range helper, which requires
an uncompressed plaintext aligned-page payload.

The 32-bit XOR fine stream deliberately favors simple exact recovery. With LZ4
it can increase total file size; complete-file measurements must include both
streams, validity, time, directories and native metadata. Coarse-only read bytes
must not be reported as total file size or complete-file compression ratio.

## Independently encoded original-value subpages

Encoding ID 16 (`FLOAT_SUBPAGE`) uses the same native aligned-page and directory
layout, with frame flags 2 (truncate) or 3 (round to nearest, ties to even).
Each unit restarts the original GORILLA value encoder
and applies LZ4 independently. The coarse-named stream slot contains the whole
original FP32 encoding; the fine slot is empty (both fine lengths are zero).
The coarse raw length is the positive GORILLA stream length. If an original word
equals GORILLA's reserved end marker, that unit falls back to raw FP32 words and
the raw-length field is negative. The absolute value is its decompressed size.
This fallback preserves arbitrary FP32 payloads without changing their bits.

The default unit size is 1024 present values. Parent-page validity and time
remain shared metadata: bitmap prefix counts map present-value starts to the
corresponding logical rows, which in turn address the unchanged time column.
The direct helper returns numeric ranges, not independently decompressed
timestamp ranges. Ordinary complete queries retain the native time-page path.
Smaller native pages with matching time/value boundaries are a separate control.

## Directly addressable linear residuals

Encoding ID 17 (`LINEAR_RESIDUAL`) uses flags 4 (truncate) or 5 (round to nearest,
ties to even). Its default unit size is 1024 present values. There is no outer
page compression and no general-purpose compression of these unit streams.
Coarse words are sign-extended 16-bit BF16 codes; fine words are the original
FP32 bits XOR the coarse code shifted by 16. Each stream has an independent
nine-byte big-endian header: signed 32-bit base, signed 32-bit step and one-byte
residual width (0 through 32). For count n, base is the first word; step is
the signed 64-bit difference between last and first divided by n-1, truncating
toward zero (zero for n=1). Predictions base+step*i and residual subtraction
wrap modulo 2^32. Signed residual r is mapped to unsigned (r<<1) XOR (r>>31).
The maximum required width is shared by the unit. Codes are packed least
significant bit first, with zero padding in the last byte.

A range reads the header and only bytes covering its residual bits; computing
each prediction requires its unit-local index, not preceding values. Width zero
requires only the shared header. Ordinary full decoding and exact recovery use
the same stream. Arbitrary original FP32 words remain lossless, including NaN
payloads and overflow on rounded coarse conversion. Shared bitmap and directory
reads remain necessary and are separate from the residual addressing advantage.
The coarse/fine raw-length directory fields remain 2*n and 4*n; these describe
logical raw sizes, not decompressed work (the direct streams are not compressed).
