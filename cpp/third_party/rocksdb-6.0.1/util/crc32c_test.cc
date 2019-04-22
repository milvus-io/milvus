//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "util/crc32c.h"
#include "util/testharness.h"
#include "util/coding.h"

namespace rocksdb {
namespace crc32c {

class CRC { };


// Tests for 3-way crc32c algorithm. We need these tests because it uses
// different lookup tables than the original Fast_CRC32
const unsigned int BUFFER_SIZE = 512 * 1024 * sizeof(uint64_t);
char buffer[BUFFER_SIZE];

struct ExpectedResult {
  size_t offset;
  size_t length;
  uint32_t crc32c;
};

ExpectedResult expectedResults[] = {
    // Zero-byte input
    { 0, 0, ~0U },
    // Small aligned inputs to test special cases in SIMD implementations
    { 8, 1, 1543413366 },
    { 8, 2, 523493126 },
    { 8, 3, 1560427360 },
    { 8, 4, 3422504776 },
    { 8, 5, 447841138 },
    { 8, 6, 3910050499 },
    { 8, 7, 3346241981 },
    // Small unaligned inputs
    { 9, 1, 3855826643 },
    { 10, 2, 560880875 },
    { 11, 3, 1479707779 },
    { 12, 4, 2237687071 },
    { 13, 5, 4063855784 },
    { 14, 6, 2553454047 },
    { 15, 7, 1349220140 },
    // Larger inputs to test leftover chunks at the end of aligned blocks
    { 8, 8, 627613930 },
    { 8, 9, 2105929409 },
    { 8, 10, 2447068514 },
    { 8, 11, 863807079 },
    { 8, 12, 292050879 },
    { 8, 13, 1411837737 },
    { 8, 14, 2614515001 },
    { 8, 15, 3579076296 },
    { 8, 16, 2897079161 },
    { 8, 17, 675168386 },
    // // Much larger inputs
    { 0, BUFFER_SIZE, 2096790750 },
    { 1, BUFFER_SIZE / 2, 3854797577 },

};

TEST(CRC, StandardResults) {

  // Original Fast_CRC32 tests.
  // From rfc3720 section B.4.
  char buf[32];

  memset(buf, 0, sizeof(buf));
  ASSERT_EQ(0x8a9136aaU, Value(buf, sizeof(buf)));

  memset(buf, 0xff, sizeof(buf));
  ASSERT_EQ(0x62a8ab43U, Value(buf, sizeof(buf)));

  for (int i = 0; i < 32; i++) {
    buf[i] = static_cast<char>(i);
  }
  ASSERT_EQ(0x46dd794eU, Value(buf, sizeof(buf)));

  for (int i = 0; i < 32; i++) {
    buf[i] = static_cast<char>(31 - i);
  }
  ASSERT_EQ(0x113fdb5cU, Value(buf, sizeof(buf)));

  unsigned char data[48] = {
    0x01, 0xc0, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00,
    0x14, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x04, 0x00,
    0x00, 0x00, 0x00, 0x14,
    0x00, 0x00, 0x00, 0x18,
    0x28, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00,
    0x02, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00,
  };
  ASSERT_EQ(0xd9963a56, Value(reinterpret_cast<char*>(data), sizeof(data)));

  // 3-Way Crc32c tests ported from folly.
  // Test 1: single computation
  for (auto expected : expectedResults) {
    uint32_t result = Value(buffer + expected.offset, expected.length);
    EXPECT_EQ(~expected.crc32c, result);
  }

  // Test 2: stitching two computations
  for (auto expected : expectedResults) {
    size_t partialLength = expected.length / 2;
    uint32_t partialChecksum = Value(buffer + expected.offset, partialLength);
    uint32_t result = Extend(partialChecksum,
        buffer + expected.offset + partialLength,
        expected.length - partialLength);
    EXPECT_EQ(~expected.crc32c, result);
  }

}

TEST(CRC, Values) {
  ASSERT_NE(Value("a", 1), Value("foo", 3));
}

TEST(CRC, Extend) {
  ASSERT_EQ(Value("hello world", 11),
            Extend(Value("hello ", 6), "world", 5));
}

TEST(CRC, Mask) {
  uint32_t crc = Value("foo", 3);
  ASSERT_NE(crc, Mask(crc));
  ASSERT_NE(crc, Mask(Mask(crc)));
  ASSERT_EQ(crc, Unmask(Mask(crc)));
  ASSERT_EQ(crc, Unmask(Unmask(Mask(Mask(crc)))));
}

}  // namespace crc32c
}  // namespace rocksdb

// copied from folly
const uint64_t FNV_64_HASH_START = 14695981039346656037ULL;
inline uint64_t fnv64_buf(const void* buf,
                          size_t n,
                          uint64_t hash = FNV_64_HASH_START) {
  // forcing signed char, since other platforms can use unsigned
  const signed char* char_buf = reinterpret_cast<const signed char*>(buf);

  for (size_t i = 0; i < n; ++i) {
    hash += (hash << 1) + (hash << 4) + (hash << 5) + (hash << 7) +
      (hash << 8) + (hash << 40);
    hash ^= char_buf[i];
  }
  return hash;
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);

  // Populate a buffer with a deterministic pattern
  // on which to compute checksums

  const uint8_t* src = (uint8_t*)rocksdb::crc32c::buffer;
  uint64_t* dst = (uint64_t*)rocksdb::crc32c::buffer;
  const uint64_t* end = (const uint64_t*)(rocksdb::crc32c::buffer + rocksdb::crc32c::BUFFER_SIZE);
  *dst++ = 0;
  while (dst < end) {
    rocksdb::EncodeFixed64(reinterpret_cast<char*>(dst), fnv64_buf((const char*)src, sizeof(uint64_t)));
    dst++;
    src += sizeof(uint64_t);
  }

  return RUN_ALL_TESTS();
}
