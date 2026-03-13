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
#include <cstdint>
#include <cstring>
#include <numeric>
#include <vector>

#include "cachinglayer/CacheSlot.h"
#include "common/Chunk.h"
#include "common/Types.h"
#include "segcore/TimestampData.h"

using namespace milvus;
using namespace milvus::segcore;

namespace {

// Helper: allocate a FixedWidthChunk holding `count` Timestamp values.
// Returns the raw buffer (caller owns) and the chunk pointer.
struct ChunkAlloc {
    char* buf;
    FixedWidthChunk* chunk;
};

ChunkAlloc
make_ts_chunk(const Timestamp* src, int64_t count) {
    auto byte_size = static_cast<uint64_t>(count * sizeof(Timestamp));
    char* buf = new char[byte_size];
    std::memcpy(buf, src, byte_size);
    auto* chunk = new FixedWidthChunk(static_cast<int32_t>(count),
                                      /*dim=*/1,
                                      buf,
                                      byte_size,
                                      /*element_size=*/sizeof(Timestamp),
                                      /*nullable=*/false,
                                      /*chunk_mmap_guard=*/nullptr);
    return {buf, chunk};
}

}  // namespace

// =====================================================================
// OwnedData tests
// =====================================================================

TEST(TimestampData, OwnedEmpty) {
    TimestampData td;
    ASSERT_TRUE(td.empty());
    ASSERT_EQ(td.size(), 0);
    ASSERT_EQ(td.num_chunks(), 0);
}

TEST(TimestampData, OwnedSingle) {
    std::vector<Timestamp> data = {10, 20, 30, 40, 50};
    TimestampData td;
    td.InitFromOwnedData(std::move(data));

    ASSERT_FALSE(td.empty());
    ASSERT_EQ(td.size(), 5);
    ASSERT_EQ(td.num_chunks(), 1);
    ASSERT_EQ(td.chunk_row_count(0), 5);
    ASSERT_EQ(td.chunk_start_offset(0), 0);

    EXPECT_EQ(td[0], 10);
    EXPECT_EQ(td[1], 20);
    EXPECT_EQ(td[2], 30);
    EXPECT_EQ(td[3], 40);
    EXPECT_EQ(td[4], 50);

    // chunk_data pointer should be valid
    const Timestamp* ptr = td.chunk_data(0);
    ASSERT_NE(ptr, nullptr);
    EXPECT_EQ(ptr[0], 10);
    EXPECT_EQ(ptr[4], 50);
}

TEST(TimestampData, OwnedLarge) {
    const int64_t N = 100000;
    std::vector<Timestamp> data(N);
    std::iota(data.begin(), data.end(), 1);  // 1, 2, ..., N

    TimestampData td;
    td.InitFromOwnedData(std::move(data));

    ASSERT_EQ(td.size(), N);
    ASSERT_EQ(td.num_chunks(), 1);

    EXPECT_EQ(td[0], 1);
    EXPECT_EQ(td[N / 2], N / 2 + 1);
    EXPECT_EQ(td[N - 1], N);
}

TEST(TimestampData, OwnedClear) {
    std::vector<Timestamp> data = {100, 200, 300};
    TimestampData td;
    td.InitFromOwnedData(std::move(data));

    ASSERT_EQ(td.size(), 3);
    td.clear();
    ASSERT_TRUE(td.empty());
    ASSERT_EQ(td.size(), 0);
    ASSERT_EQ(td.num_chunks(), 0);
}

TEST(TimestampData, MoveConstruct) {
    std::vector<Timestamp> data = {10, 20, 30};
    TimestampData td;
    td.InitFromOwnedData(std::move(data));

    TimestampData td2 = std::move(td);
    ASSERT_EQ(td2.size(), 3);
    EXPECT_EQ(td2[0], 10);
    EXPECT_EQ(td2[2], 30);
}

// =====================================================================
// PinnedChunks tests (single chunk)
// =====================================================================

TEST(TimestampData, PinnedSingleChunk) {
    std::vector<Timestamp> raw = {5, 15, 25, 35, 45};
    auto alloc = make_ts_chunk(raw.data(), raw.size());

    // Wrap in PinWrapper (no real RAII needed for test)
    std::vector<cachinglayer::PinWrapper<Chunk*>> pins;
    pins.emplace_back(static_cast<Chunk*>(alloc.chunk));

    TimestampData td;
    td.InitFromPinnedChunks(/*column=*/nullptr, std::move(pins));

    ASSERT_EQ(td.size(), 5);
    ASSERT_EQ(td.num_chunks(), 1);
    ASSERT_EQ(td.chunk_row_count(0), 5);
    ASSERT_EQ(td.chunk_start_offset(0), 0);

    EXPECT_EQ(td[0], 5);
    EXPECT_EQ(td[2], 25);
    EXPECT_EQ(td[4], 45);

    td.clear();
    delete alloc.chunk;
    delete[] alloc.buf;
}

// =====================================================================
// PinnedChunks tests (multi chunk)
// =====================================================================

TEST(TimestampData, PinnedMultiChunk) {
    // 3 chunks: [10,20,30], [40,50], [60,70,80,90]
    std::vector<Timestamp> c1 = {10, 20, 30};
    std::vector<Timestamp> c2 = {40, 50};
    std::vector<Timestamp> c3 = {60, 70, 80, 90};

    auto a1 = make_ts_chunk(c1.data(), c1.size());
    auto a2 = make_ts_chunk(c2.data(), c2.size());
    auto a3 = make_ts_chunk(c3.data(), c3.size());

    std::vector<cachinglayer::PinWrapper<Chunk*>> pins;
    pins.emplace_back(static_cast<Chunk*>(a1.chunk));
    pins.emplace_back(static_cast<Chunk*>(a2.chunk));
    pins.emplace_back(static_cast<Chunk*>(a3.chunk));

    TimestampData td;
    td.InitFromPinnedChunks(nullptr, std::move(pins));

    ASSERT_EQ(td.size(), 9);
    ASSERT_EQ(td.num_chunks(), 3);

    // chunk metadata
    EXPECT_EQ(td.chunk_start_offset(0), 0);
    EXPECT_EQ(td.chunk_row_count(0), 3);
    EXPECT_EQ(td.chunk_start_offset(1), 3);
    EXPECT_EQ(td.chunk_row_count(1), 2);
    EXPECT_EQ(td.chunk_start_offset(2), 5);
    EXPECT_EQ(td.chunk_row_count(2), 4);

    // sequential access across all chunks
    std::vector<Timestamp> expected = {10, 20, 30, 40, 50, 60, 70, 80, 90};
    for (int64_t i = 0; i < 9; ++i) {
        EXPECT_EQ(td[i], expected[i]) << "mismatch at index " << i;
    }

    // chunk_data pointers
    EXPECT_EQ(td.chunk_data(0)[0], 10);
    EXPECT_EQ(td.chunk_data(1)[0], 40);
    EXPECT_EQ(td.chunk_data(2)[3], 90);

    td.clear();
    delete a1.chunk;
    delete[] a1.buf;
    delete a2.chunk;
    delete[] a2.buf;
    delete a3.chunk;
    delete[] a3.buf;
}

TEST(TimestampData, PinnedManySmallChunks) {
    // Stress the vcid_to_cid_arr with many small chunks
    const int64_t num_chunks = 50;
    const int64_t rows_per_chunk = 100;
    const int64_t total = num_chunks * rows_per_chunk;

    std::vector<std::vector<Timestamp>> raw_chunks(num_chunks);
    std::vector<ChunkAlloc> allocs;
    std::vector<cachinglayer::PinWrapper<Chunk*>> pins;

    Timestamp val = 0;
    for (int64_t c = 0; c < num_chunks; ++c) {
        raw_chunks[c].resize(rows_per_chunk);
        for (int64_t r = 0; r < rows_per_chunk; ++r) {
            raw_chunks[c][r] = val++;
        }
        auto alloc = make_ts_chunk(raw_chunks[c].data(), rows_per_chunk);
        allocs.push_back(alloc);
        pins.emplace_back(static_cast<Chunk*>(alloc.chunk));
    }

    TimestampData td;
    td.InitFromPinnedChunks(nullptr, std::move(pins));

    ASSERT_EQ(td.size(), total);
    ASSERT_EQ(td.num_chunks(), num_chunks);

    // Verify every element via operator[]
    for (int64_t i = 0; i < total; ++i) {
        ASSERT_EQ(td[i], static_cast<Timestamp>(i)) << "mismatch at " << i;
    }

    // Verify chunk metadata
    for (int64_t c = 0; c < num_chunks; ++c) {
        EXPECT_EQ(td.chunk_start_offset(c), c * rows_per_chunk);
        EXPECT_EQ(td.chunk_row_count(c), rows_per_chunk);
    }

    td.clear();
    for (auto& a : allocs) {
        delete a.chunk;
        delete[] a.buf;
    }
}

TEST(TimestampData, PinnedUnevenChunks) {
    // Uneven chunk sizes: 1, 7, 3, 13, 2
    std::vector<int64_t> sizes = {1, 7, 3, 13, 2};
    int64_t total = 0;
    for (auto s : sizes) total += s;

    std::vector<std::vector<Timestamp>> raw_chunks;
    std::vector<ChunkAlloc> allocs;
    std::vector<cachinglayer::PinWrapper<Chunk*>> pins;

    Timestamp val = 100;
    for (auto sz : sizes) {
        std::vector<Timestamp> chunk_data(sz);
        for (int64_t r = 0; r < sz; ++r) {
            chunk_data[r] = val++;
        }
        auto alloc = make_ts_chunk(chunk_data.data(), sz);
        raw_chunks.push_back(std::move(chunk_data));
        allocs.push_back(alloc);
        pins.emplace_back(static_cast<Chunk*>(alloc.chunk));
    }

    TimestampData td;
    td.InitFromPinnedChunks(nullptr, std::move(pins));

    ASSERT_EQ(td.size(), total);
    ASSERT_EQ(td.num_chunks(), static_cast<int64_t>(sizes.size()));

    // Verify all elements
    val = 100;
    for (int64_t i = 0; i < total; ++i) {
        EXPECT_EQ(td[i], val++) << "mismatch at " << i;
    }

    // Verify boundary elements at chunk edges
    int64_t offset = 0;
    for (size_t c = 0; c < sizes.size(); ++c) {
        EXPECT_EQ(td.chunk_start_offset(c), offset);
        EXPECT_EQ(td.chunk_row_count(c), sizes[c]);
        offset += sizes[c];
    }

    td.clear();
    for (auto& a : allocs) {
        delete a.chunk;
        delete[] a.buf;
    }
}

// =====================================================================
// Edge cases
// =====================================================================

TEST(TimestampData, PinnedSingleElement) {
    std::vector<Timestamp> raw = {42};
    auto alloc = make_ts_chunk(raw.data(), 1);

    std::vector<cachinglayer::PinWrapper<Chunk*>> pins;
    pins.emplace_back(static_cast<Chunk*>(alloc.chunk));

    TimestampData td;
    td.InitFromPinnedChunks(nullptr, std::move(pins));

    ASSERT_EQ(td.size(), 1);
    EXPECT_EQ(td[0], 42);

    td.clear();
    delete alloc.chunk;
    delete[] alloc.buf;
}

TEST(TimestampData, OwnedSingleElement) {
    TimestampData td;
    td.InitFromOwnedData({999});

    ASSERT_EQ(td.size(), 1);
    EXPECT_EQ(td[0], 999);
}
