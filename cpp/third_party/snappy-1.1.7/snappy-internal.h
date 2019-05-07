// Copyright 2008 Google Inc. All Rights Reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//
// Internals shared between the Snappy implementation and its unittest.

#ifndef THIRD_PARTY_SNAPPY_SNAPPY_INTERNAL_H_
#define THIRD_PARTY_SNAPPY_SNAPPY_INTERNAL_H_

#include "snappy-stubs-internal.h"

namespace snappy {
namespace internal {

class WorkingMemory {
 public:
  WorkingMemory() : large_table_(NULL) { }
  ~WorkingMemory() { delete[] large_table_; }

  // Allocates and clears a hash table using memory in "*this",
  // stores the number of buckets in "*table_size" and returns a pointer to
  // the base of the hash table.
  uint16* GetHashTable(size_t input_size, int* table_size);

 private:
  uint16 small_table_[1<<10];    // 2KB
  uint16* large_table_;          // Allocated only when needed

  // No copying
  WorkingMemory(const WorkingMemory&);
  void operator=(const WorkingMemory&);
};

// Flat array compression that does not emit the "uncompressed length"
// prefix. Compresses "input" string to the "*op" buffer.
//
// REQUIRES: "input_length <= kBlockSize"
// REQUIRES: "op" points to an array of memory that is at least
// "MaxCompressedLength(input_length)" in size.
// REQUIRES: All elements in "table[0..table_size-1]" are initialized to zero.
// REQUIRES: "table_size" is a power of two
//
// Returns an "end" pointer into "op" buffer.
// "end - op" is the compressed size of "input".
char* CompressFragment(const char* input,
                       size_t input_length,
                       char* op,
                       uint16* table,
                       const int table_size);

// Find the largest n such that
//
//   s1[0,n-1] == s2[0,n-1]
//   and n <= (s2_limit - s2).
//
// Return make_pair(n, n < 8).
// Does not read *s2_limit or beyond.
// Does not read *(s1 + (s2_limit - s2)) or beyond.
// Requires that s2_limit >= s2.
//
// Separate implementation for 64-bit, little-endian cpus.
#if !defined(SNAPPY_IS_BIG_ENDIAN) && \
    (defined(ARCH_K8) || defined(ARCH_PPC) || defined(ARCH_ARM))
static inline std::pair<size_t, bool> FindMatchLength(const char* s1,
                                                      const char* s2,
                                                      const char* s2_limit) {
  assert(s2_limit >= s2);
  size_t matched = 0;

  // This block isn't necessary for correctness; we could just start looping
  // immediately.  As an optimization though, it is useful.  It creates some not
  // uncommon code paths that determine, without extra effort, whether the match
  // length is less than 8.  In short, we are hoping to avoid a conditional
  // branch, and perhaps get better code layout from the C++ compiler.
  if (SNAPPY_PREDICT_TRUE(s2 <= s2_limit - 8)) {
    uint64 a1 = UNALIGNED_LOAD64(s1);
    uint64 a2 = UNALIGNED_LOAD64(s2);
    if (a1 != a2) {
      return std::pair<size_t, bool>(Bits::FindLSBSetNonZero64(a1 ^ a2) >> 3,
                                     true);
    } else {
      matched = 8;
      s2 += 8;
    }
  }

  // Find out how long the match is. We loop over the data 64 bits at a
  // time until we find a 64-bit block that doesn't match; then we find
  // the first non-matching bit and use that to calculate the total
  // length of the match.
  while (SNAPPY_PREDICT_TRUE(s2 <= s2_limit - 8)) {
    if (UNALIGNED_LOAD64(s2) == UNALIGNED_LOAD64(s1 + matched)) {
      s2 += 8;
      matched += 8;
    } else {
      uint64 x = UNALIGNED_LOAD64(s2) ^ UNALIGNED_LOAD64(s1 + matched);
      int matching_bits = Bits::FindLSBSetNonZero64(x);
      matched += matching_bits >> 3;
      assert(matched >= 8);
      return std::pair<size_t, bool>(matched, false);
    }
  }
  while (SNAPPY_PREDICT_TRUE(s2 < s2_limit)) {
    if (s1[matched] == *s2) {
      ++s2;
      ++matched;
    } else {
      return std::pair<size_t, bool>(matched, matched < 8);
    }
  }
  return std::pair<size_t, bool>(matched, matched < 8);
}
#else
static inline std::pair<size_t, bool> FindMatchLength(const char* s1,
                                                      const char* s2,
                                                      const char* s2_limit) {
  // Implementation based on the x86-64 version, above.
  assert(s2_limit >= s2);
  int matched = 0;

  while (s2 <= s2_limit - 4 &&
         UNALIGNED_LOAD32(s2) == UNALIGNED_LOAD32(s1 + matched)) {
    s2 += 4;
    matched += 4;
  }
  if (LittleEndian::IsLittleEndian() && s2 <= s2_limit - 4) {
    uint32 x = UNALIGNED_LOAD32(s2) ^ UNALIGNED_LOAD32(s1 + matched);
    int matching_bits = Bits::FindLSBSetNonZero(x);
    matched += matching_bits >> 3;
  } else {
    while ((s2 < s2_limit) && (s1[matched] == *s2)) {
      ++s2;
      ++matched;
    }
  }
  return std::pair<size_t, bool>(matched, matched < 8);
}
#endif

// Lookup tables for decompression code.  Give --snappy_dump_decompression_table
// to the unit test to recompute char_table.

enum {
  LITERAL = 0,
  COPY_1_BYTE_OFFSET = 1,  // 3 bit length + 3 bits of offset in opcode
  COPY_2_BYTE_OFFSET = 2,
  COPY_4_BYTE_OFFSET = 3
};
static const int kMaximumTagLength = 5;  // COPY_4_BYTE_OFFSET plus the actual offset.

// Data stored per entry in lookup table:
//      Range   Bits-used       Description
//      ------------------------------------
//      1..64   0..7            Literal/copy length encoded in opcode byte
//      0..7    8..10           Copy offset encoded in opcode byte / 256
//      0..4    11..13          Extra bytes after opcode
//
// We use eight bits for the length even though 7 would have sufficed
// because of efficiency reasons:
//      (1) Extracting a byte is faster than a bit-field
//      (2) It properly aligns copy offset so we do not need a <<8
static const uint16 char_table[256] = {
  0x0001, 0x0804, 0x1001, 0x2001, 0x0002, 0x0805, 0x1002, 0x2002,
  0x0003, 0x0806, 0x1003, 0x2003, 0x0004, 0x0807, 0x1004, 0x2004,
  0x0005, 0x0808, 0x1005, 0x2005, 0x0006, 0x0809, 0x1006, 0x2006,
  0x0007, 0x080a, 0x1007, 0x2007, 0x0008, 0x080b, 0x1008, 0x2008,
  0x0009, 0x0904, 0x1009, 0x2009, 0x000a, 0x0905, 0x100a, 0x200a,
  0x000b, 0x0906, 0x100b, 0x200b, 0x000c, 0x0907, 0x100c, 0x200c,
  0x000d, 0x0908, 0x100d, 0x200d, 0x000e, 0x0909, 0x100e, 0x200e,
  0x000f, 0x090a, 0x100f, 0x200f, 0x0010, 0x090b, 0x1010, 0x2010,
  0x0011, 0x0a04, 0x1011, 0x2011, 0x0012, 0x0a05, 0x1012, 0x2012,
  0x0013, 0x0a06, 0x1013, 0x2013, 0x0014, 0x0a07, 0x1014, 0x2014,
  0x0015, 0x0a08, 0x1015, 0x2015, 0x0016, 0x0a09, 0x1016, 0x2016,
  0x0017, 0x0a0a, 0x1017, 0x2017, 0x0018, 0x0a0b, 0x1018, 0x2018,
  0x0019, 0x0b04, 0x1019, 0x2019, 0x001a, 0x0b05, 0x101a, 0x201a,
  0x001b, 0x0b06, 0x101b, 0x201b, 0x001c, 0x0b07, 0x101c, 0x201c,
  0x001d, 0x0b08, 0x101d, 0x201d, 0x001e, 0x0b09, 0x101e, 0x201e,
  0x001f, 0x0b0a, 0x101f, 0x201f, 0x0020, 0x0b0b, 0x1020, 0x2020,
  0x0021, 0x0c04, 0x1021, 0x2021, 0x0022, 0x0c05, 0x1022, 0x2022,
  0x0023, 0x0c06, 0x1023, 0x2023, 0x0024, 0x0c07, 0x1024, 0x2024,
  0x0025, 0x0c08, 0x1025, 0x2025, 0x0026, 0x0c09, 0x1026, 0x2026,
  0x0027, 0x0c0a, 0x1027, 0x2027, 0x0028, 0x0c0b, 0x1028, 0x2028,
  0x0029, 0x0d04, 0x1029, 0x2029, 0x002a, 0x0d05, 0x102a, 0x202a,
  0x002b, 0x0d06, 0x102b, 0x202b, 0x002c, 0x0d07, 0x102c, 0x202c,
  0x002d, 0x0d08, 0x102d, 0x202d, 0x002e, 0x0d09, 0x102e, 0x202e,
  0x002f, 0x0d0a, 0x102f, 0x202f, 0x0030, 0x0d0b, 0x1030, 0x2030,
  0x0031, 0x0e04, 0x1031, 0x2031, 0x0032, 0x0e05, 0x1032, 0x2032,
  0x0033, 0x0e06, 0x1033, 0x2033, 0x0034, 0x0e07, 0x1034, 0x2034,
  0x0035, 0x0e08, 0x1035, 0x2035, 0x0036, 0x0e09, 0x1036, 0x2036,
  0x0037, 0x0e0a, 0x1037, 0x2037, 0x0038, 0x0e0b, 0x1038, 0x2038,
  0x0039, 0x0f04, 0x1039, 0x2039, 0x003a, 0x0f05, 0x103a, 0x203a,
  0x003b, 0x0f06, 0x103b, 0x203b, 0x003c, 0x0f07, 0x103c, 0x203c,
  0x0801, 0x0f08, 0x103d, 0x203d, 0x1001, 0x0f09, 0x103e, 0x203e,
  0x1801, 0x0f0a, 0x103f, 0x203f, 0x2001, 0x0f0b, 0x1040, 0x2040
};

}  // end namespace internal
}  // end namespace snappy

#endif  // THIRD_PARTY_SNAPPY_SNAPPY_INTERNAL_H_
