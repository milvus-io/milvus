// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <numeric>
#include <random>
#include <vector>

#include "segcore/InsertRecord.h"

using namespace milvus::segcore;

TEST(CompressedInt64PkArrayTest, Empty) {
    CompressedInt64PkArray arr;
    EXPECT_TRUE(arr.empty());
    EXPECT_EQ(arr.num_rows(), 0);
}

TEST(CompressedInt64PkArrayTest, SingleElement) {
    CompressedInt64PkArray arr;
    int64_t pk = 42;
    arr.build(&pk, 1);

    EXPECT_FALSE(arr.empty());
    EXPECT_EQ(arr.num_rows(), 1);
    EXPECT_EQ(arr.at(0), 42);
}

TEST(CompressedInt64PkArrayTest, AllIdentical) {
    // bit_width = 0, no data stored
    constexpr int64_t N = 200;
    std::vector<int64_t> pks(N, 12345);

    CompressedInt64PkArray arr;
    arr.build(pks.data(), N);

    EXPECT_EQ(arr.num_rows(), N);
    for (int64_t i = 0; i < N; ++i) {
        EXPECT_EQ(arr.at(i), 12345) << "mismatch at offset " << i;
    }
}

TEST(CompressedInt64PkArrayTest, Sequential) {
    constexpr int64_t N = 500;
    std::vector<int64_t> pks(N);
    std::iota(pks.begin(), pks.end(), 1000);  // 1000..1499

    CompressedInt64PkArray arr;
    arr.build(pks.data(), N);

    EXPECT_EQ(arr.num_rows(), N);
    for (int64_t i = 0; i < N; ++i) {
        EXPECT_EQ(arr.at(i), 1000 + i) << "mismatch at offset " << i;
    }
}

TEST(CompressedInt64PkArrayTest, NegativeValues) {
    std::vector<int64_t> pks = {-100, -50, 0, 50, 100, -200, 200};

    CompressedInt64PkArray arr;
    arr.build(pks.data(), pks.size());

    EXPECT_EQ(arr.num_rows(), static_cast<int64_t>(pks.size()));
    for (int64_t i = 0; i < static_cast<int64_t>(pks.size()); ++i) {
        EXPECT_EQ(arr.at(i), pks[i]) << "mismatch at offset " << i;
    }
}

TEST(CompressedInt64PkArrayTest, LargeRange) {
    // Large deltas requiring many bits
    std::vector<int64_t> pks = {
        0, 1, INT64_MAX / 2, INT64_MAX / 2 + 1, INT64_MAX - 1};

    CompressedInt64PkArray arr;
    arr.build(pks.data(), pks.size());

    for (int64_t i = 0; i < static_cast<int64_t>(pks.size()); ++i) {
        EXPECT_EQ(arr.at(i), pks[i]) << "mismatch at offset " << i;
    }
}

TEST(CompressedInt64PkArrayTest, RandomAccess) {
    // Random values spanning multiple blocks
    constexpr int64_t N = 1000;
    std::mt19937_64 rng(42);
    std::uniform_int_distribution<int64_t> dist(0, 1'000'000);

    std::vector<int64_t> pks(N);
    for (auto& pk : pks) {
        pk = dist(rng);
    }

    CompressedInt64PkArray arr;
    arr.build(pks.data(), N);

    // Verify all values
    for (int64_t i = 0; i < N; ++i) {
        EXPECT_EQ(arr.at(i), pks[i]) << "mismatch at offset " << i;
    }
}

TEST(CompressedInt64PkArrayTest, BulkAt) {
    constexpr int64_t N = 300;
    std::mt19937_64 rng(123);
    std::uniform_int_distribution<int64_t> dist(0, 100'000);

    std::vector<int64_t> pks(N);
    for (auto& pk : pks) {
        pk = dist(rng);
    }

    CompressedInt64PkArray arr;
    arr.build(pks.data(), N);

    // Query a subset of offsets in random order
    std::vector<int64_t> offsets = {0, 1, 127, 128, 129, 255, 256, 299};
    std::vector<int64_t> output(offsets.size());
    arr.bulk_at(offsets.data(), offsets.size(), output.data());

    for (size_t i = 0; i < offsets.size(); ++i) {
        EXPECT_EQ(output[i], pks[offsets[i]])
            << "bulk_at mismatch at query index " << i << " (offset "
            << offsets[i] << ")";
    }
}

TEST(CompressedInt64PkArrayTest, ExactlyOneBlock) {
    constexpr int64_t N = 128;  // kBlockSize
    std::vector<int64_t> pks(N);
    std::iota(pks.begin(), pks.end(), 500);

    CompressedInt64PkArray arr;
    arr.build(pks.data(), N);

    EXPECT_EQ(arr.num_rows(), N);
    for (int64_t i = 0; i < N; ++i) {
        EXPECT_EQ(arr.at(i), 500 + i);
    }
}

TEST(CompressedInt64PkArrayTest, BlockBoundary) {
    // 129 elements = 2 blocks (128 + 1)
    constexpr int64_t N = 129;
    std::vector<int64_t> pks(N);
    std::iota(pks.begin(), pks.end(), 0);

    CompressedInt64PkArray arr;
    arr.build(pks.data(), N);

    // Verify boundary elements
    EXPECT_EQ(arr.at(0), 0);      // first of block 0
    EXPECT_EQ(arr.at(127), 127);  // last of block 0
    EXPECT_EQ(arr.at(128), 128);  // first of block 1
}

TEST(CompressedInt64PkArrayTest, MemorySize) {
    constexpr int64_t N = 1000;
    std::vector<int64_t> pks(N);
    std::iota(pks.begin(), pks.end(), 0);

    CompressedInt64PkArray arr;
    arr.build(pks.data(), N);

    size_t mem = arr.memory_size();
    size_t uncompressed = N * sizeof(int64_t);
    // Compressed should be smaller than uncompressed for sequential data
    EXPECT_LT(mem, uncompressed)
        << "compressed=" << mem << " uncompressed=" << uncompressed;
}

TEST(CompressedInt64PkArrayTest, PowerOfTwoBitWidths) {
    // Test bit widths 1, 2, 4, 8, 16 by controlling max delta
    auto test_max_delta = [](uint64_t max_delta) {
        constexpr int64_t N = 128;
        std::vector<int64_t> pks(N, 0);
        pks[0] = 0;
        pks[1] = static_cast<int64_t>(max_delta);
        // Fill rest with values in range
        std::mt19937_64 rng(max_delta);
        std::uniform_int_distribution<int64_t> dist(
            0, static_cast<int64_t>(max_delta));
        for (int64_t i = 2; i < N; ++i) {
            pks[i] = dist(rng);
        }

        CompressedInt64PkArray arr;
        arr.build(pks.data(), N);

        for (int64_t i = 0; i < N; ++i) {
            EXPECT_EQ(arr.at(i), pks[i]) << "mismatch at offset " << i
                                         << " with max_delta=" << max_delta;
        }
    };

    test_max_delta(1);           // 1 bit
    test_max_delta(3);           // 2 bits
    test_max_delta(15);          // 4 bits
    test_max_delta(255);         // 8 bits
    test_max_delta(65535);       // 16 bits
    test_max_delta(0xFFFFFFFF);  // 32 bits
}

// Simulate what build_offset2pk does: build from unsorted PK data
// (as would happen in a non-sorted segment)
TEST(CompressedInt64PkArrayTest, UnsortedPks) {
    // Typical auto-id pattern: IDs assigned out of order due to concurrent inserts
    std::vector<int64_t> pks = {449595445547098119,
                                449595445547098120,
                                449595445547098121,
                                449595445547098115,
                                449595445547098116,
                                449595445547098117,
                                449595445547098112,
                                449595445547098113,
                                449595445547098114,
                                449595445547098118};

    CompressedInt64PkArray arr;
    arr.build(pks.data(), pks.size());

    for (int64_t i = 0; i < static_cast<int64_t>(pks.size()); ++i) {
        EXPECT_EQ(arr.at(i), pks[i]) << "mismatch at offset " << i;
    }
}

// Simulate sorted segment: PKs are in ascending order
TEST(CompressedInt64PkArrayTest, SortedPks) {
    constexpr int64_t N = 500;
    std::mt19937_64 rng(99);
    std::uniform_int_distribution<int64_t> dist(1, 1'000'000'000LL);

    std::vector<int64_t> pks(N);
    for (auto& pk : pks) {
        pk = dist(rng);
    }
    std::sort(pks.begin(), pks.end());

    CompressedInt64PkArray arr;
    arr.build(pks.data(), N);

    EXPECT_EQ(arr.num_rows(), N);
    for (int64_t i = 0; i < N; ++i) {
        EXPECT_EQ(arr.at(i), pks[i]) << "mismatch at offset " << i;
    }

    // Verify bulk_at with random offsets
    std::vector<int64_t> offsets(50);
    std::uniform_int_distribution<int64_t> offset_dist(0, N - 1);
    for (auto& o : offsets) {
        o = offset_dist(rng);
    }
    std::vector<int64_t> output(50);
    arr.bulk_at(offsets.data(), offsets.size(), output.data());
    for (size_t i = 0; i < offsets.size(); ++i) {
        EXPECT_EQ(output[i], pks[offsets[i]]);
    }
}

// Simulate sorted segment with duplicates (possible with non-unique PK)
TEST(CompressedInt64PkArrayTest, SortedWithDuplicates) {
    std::vector<int64_t> pks;
    for (int64_t v = 100; v < 200; ++v) {
        // Each value appears 3 times
        pks.push_back(v);
        pks.push_back(v);
        pks.push_back(v);
    }

    CompressedInt64PkArray arr;
    arr.build(pks.data(), pks.size());

    for (int64_t i = 0; i < static_cast<int64_t>(pks.size()); ++i) {
        EXPECT_EQ(arr.at(i), pks[i]) << "mismatch at offset " << i;
    }
}
