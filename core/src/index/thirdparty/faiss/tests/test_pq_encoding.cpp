/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include <iostream>
#include <vector>
#include <memory>

#include <gtest/gtest.h>

#include <faiss/impl/ProductQuantizer.h>


namespace {

const std::vector<uint64_t> random_vector(size_t s) {
  std::vector<uint64_t> v(s, 0);
  for (size_t i = 0; i < s; ++i) {
    v[i] = rand();
  }

  return v;
}

}  // namespace


TEST(PQEncoderGeneric, encode) {
  const int nsubcodes = 97;
  const int minbits = 1;
  const int maxbits = 24;
  const std::vector<uint64_t> values = random_vector(nsubcodes);

  for(int nbits = minbits; nbits <= maxbits; ++nbits) {
    std::cerr << "nbits = " << nbits << std::endl;

    const uint64_t mask = (1ull << nbits) - 1;
    std::unique_ptr<uint8_t[]> codes(
      new uint8_t[(nsubcodes * maxbits + 7) / 8]
    );

    // NOTE(hoss): Necessary scope to ensure trailing bits are flushed to mem.
    {
      faiss::PQEncoderGeneric encoder(codes.get(), nbits);
      for (const auto& v : values) {
        encoder.encode(v & mask);
      }
    }

    faiss::PQDecoderGeneric decoder(codes.get(), nbits);
    for (int i = 0; i < nsubcodes; ++i) {
      uint64_t v = decoder.decode();
      EXPECT_EQ(values[i] & mask, v);
    }
  }
}


TEST(PQEncoder8, encode) {
  const int nsubcodes = 100;
  const std::vector<uint64_t> values = random_vector(nsubcodes);
  const uint64_t mask = 0xFF;
  std::unique_ptr<uint8_t[]> codes(new uint8_t[nsubcodes]);

  faiss::PQEncoder8 encoder(codes.get(), 8);
  for (const auto& v : values) {
    encoder.encode(v & mask);
  }

  faiss::PQDecoder8 decoder(codes.get(), 8);
  for (int i = 0; i < nsubcodes; ++i) {
    uint64_t v = decoder.decode();
    EXPECT_EQ(values[i] & mask, v);
  }
}


TEST(PQEncoder16, encode) {
  const int nsubcodes = 100;
  const std::vector<uint64_t> values = random_vector(nsubcodes);
  const uint64_t mask = 0xFFFF;
  std::unique_ptr<uint8_t[]> codes(new uint8_t[2 * nsubcodes]);

  faiss::PQEncoder16 encoder(codes.get(), 16);
  for (const auto& v : values) {
    encoder.encode(v & mask);
  }

  faiss::PQDecoder16 decoder(codes.get(), 16);
  for (int i = 0; i < nsubcodes; ++i) {
    uint64_t v = decoder.decode();
    EXPECT_EQ(values[i] & mask, v);
  }
}
