// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <random>
#include <vector>

#include "index/Utils.h"

namespace milvus::index::test {
namespace {

TEST(IndexUtilsTest, SetBitsetSealedFillsContiguousBlocks) {
    TargetBitmap bitmap(256);
    std::vector<uint32_t> doc_ids;
    // a word-aligned block, a block crossing a word boundary, and a tail
    // shorter than a block
    for (uint32_t id = 0; id < 64; ++id) {
        doc_ids.push_back(id);
    }
    for (uint32_t id = 70; id < 134; ++id) {
        doc_ids.push_back(id);
    }
    for (uint32_t id = 200; id < 256; ++id) {
        doc_ids.push_back(id);
    }

    SetBitsetSealed(&bitmap, doc_ids.data(), doc_ids.size());

    for (uint32_t id = 0; id < 256; ++id) {
        const bool expected = id < 64 || (id >= 70 && id < 134) || id >= 200;
        EXPECT_EQ(static_cast<bool>(bitmap[id]), expected) << id;
    }
}

TEST(IndexUtilsTest, SetBitsetSealedVerifiesBlockInterior) {
    // first and last id of the block are one word apart, but the interior is
    // not consecutive: the block must not be range-filled.
    TargetBitmap bitmap(128);
    std::vector<uint32_t> doc_ids;
    for (uint32_t id = 0; id < 64; ++id) {
        doc_ids.push_back(id);
    }
    doc_ids[10] = 11;

    SetBitsetSealed(&bitmap, doc_ids.data(), doc_ids.size());

    EXPECT_FALSE(bitmap[10]);
    EXPECT_EQ(bitmap.count(), 63);
}

TEST(IndexUtilsTest, SetBitsetSealedAcceptsUnorderedIds) {
    TargetBitmap bitmap(192);
    const std::vector<uint32_t> doc_ids = {
        0, 1, 1, 63, 64, 65, 127, 128, 191, 70, 2};

    SetBitsetSealed(&bitmap, doc_ids.data(), doc_ids.size());

    for (const auto doc_id : doc_ids) {
        EXPECT_TRUE(bitmap[doc_id]);
    }
    EXPECT_EQ(bitmap.count(), 10);
}

TEST(IndexUtilsTest, SetBitsetGrowingSkipsOutOfRangeIds) {
    TargetBitmap bitmap(70);
    const std::vector<uint32_t> doc_ids = {0, 63, 64, 69, 70, 100, 2, 64};

    SetBitsetGrowing(&bitmap, doc_ids.data(), doc_ids.size());

    EXPECT_TRUE(bitmap[0]);
    EXPECT_TRUE(bitmap[2]);
    EXPECT_TRUE(bitmap[63]);
    EXPECT_TRUE(bitmap[64]);
    EXPECT_TRUE(bitmap[69]);
    EXPECT_EQ(bitmap.count(), 5);

    // a consecutive block running past the end must not be range-filled
    TargetBitmap tail(100);
    std::vector<uint32_t> block;
    for (uint32_t id = 60; id < 124; ++id) {
        block.push_back(id);
    }

    SetBitsetGrowing(&tail, block.data(), block.size());

    EXPECT_TRUE(tail[60]);
    EXPECT_TRUE(tail[99]);
    EXPECT_EQ(tail.count(), 40);
}

TEST(IndexUtilsTest, SetBitsetMatchesPerBitReference) {
    std::mt19937 rng(42);
    for (int round = 0; round < 64; ++round) {
        const size_t size = 1 + rng() % 2000;
        std::vector<uint32_t> doc_ids;
        // consecutive runs of random length interleaved with random ids,
        // the growing input additionally reaching past the bitset
        for (int piece = 0; piece < 8; ++piece) {
            const uint32_t start = rng() % size;
            const uint32_t len = rng() % 200;
            for (uint32_t k = 0; k < len; ++k) {
                doc_ids.push_back(start + k);
            }
            for (int k = 0; k < 16; ++k) {
                doc_ids.push_back(rng() % size);
            }
        }
        if (round % 2 == 1) {
            std::shuffle(doc_ids.begin(), doc_ids.end(), rng);
        }

        TargetBitmap expected(size);
        for (const auto id : doc_ids) {
            if (id < size) {
                expected[id] = true;
            }
        }

        TargetBitmap growing(size);
        SetBitsetGrowing(&growing, doc_ids.data(), doc_ids.size());

        std::vector<uint32_t> in_range;
        for (const auto id : doc_ids) {
            if (id < size) {
                in_range.push_back(id);
            }
        }
        TargetBitmap sealed(size);
        SetBitsetSealed(&sealed, in_range.data(), in_range.size());

        for (size_t id = 0; id < size; ++id) {
            ASSERT_EQ(static_cast<bool>(growing[id]),
                      static_cast<bool>(expected[id]))
                << round << " " << id;
            ASSERT_EQ(static_cast<bool>(sealed[id]),
                      static_cast<bool>(expected[id]))
                << round << " " << id;
        }
    }
}

TEST(IndexUtilsTest, SetBitsetHandlesEmptyInput) {
    TargetBitmap sealed(65);
    TargetBitmap growing(65);

    SetBitsetSealed(&sealed, nullptr, 0);
    SetBitsetGrowing(&growing, nullptr, 0);

    EXPECT_EQ(sealed.count(), 0);
    EXPECT_EQ(growing.count(), 0);
}

}  // namespace
}  // namespace milvus::index::test
