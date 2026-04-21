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

#include <memory>
#include <set>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/Chunk.h"
#include "common/GroupChunk.h"
#include "mmap/ChunkedColumn.h"
#include "mmap/ChunkedColumnGroup.h"
#include "test_utils/cachinglayer_test_utils.h"

namespace milvus {
namespace {

constexpr int32_t kElementSize = 4;
constexpr int64_t kTestFieldId = 1;

struct ColumnFixture {
    std::shared_ptr<ChunkedColumnInterface> column;
    std::shared_ptr<std::set<cachinglayer::cid_t>> fetched;
    // Keeps chunk data buffers alive for the column's lifetime.
    std::shared_ptr<void> buffer_holder;
};

struct ColumnSpec {
    std::vector<int64_t> rows_per_chunk;
    std::vector<std::vector<bool>> valid_patterns;  // empty => all valid
    bool nullable{true};
};

std::vector<char>
BuildChunkBuffer(int64_t row_num,
                 const std::vector<bool>* pattern,
                 bool nullable) {
    const int32_t bitmap_bytes = nullable ? (row_num + 7) / 8 : 0;
    std::vector<char> buf(bitmap_bytes + row_num * kElementSize, 0);
    if (nullable) {
        for (int64_t j = 0; j < row_num; ++j) {
            const bool v = pattern ? (*pattern)[j] : true;
            if (v) {
                buf[j >> 3] |= (1 << (j & 0x07));
            }
        }
    }
    return buf;
}

std::unique_ptr<FixedWidthChunk>
MakeFixedChunk(int64_t row_num, bool nullable, char* data, size_t size) {
    auto guard = std::make_shared<ChunkMmapGuard>(nullptr, 0, "");
    return std::make_unique<FixedWidthChunk>(
        row_num, 1, data, size, kElementSize, nullable, guard);
}

class CountingChunkTranslator : public TestChunkTranslator {
 public:
    CountingChunkTranslator(
        std::vector<int64_t> rows,
        std::string key,
        std::vector<std::unique_ptr<Chunk>>&& chunks,
        std::shared_ptr<std::set<cachinglayer::cid_t>> fetched)
        : TestChunkTranslator(
              std::move(rows), std::move(key), std::move(chunks)),
          fetched_(std::move(fetched)) {
    }

    std::vector<std::pair<cachinglayer::cid_t, std::unique_ptr<Chunk>>>
    get_cells(milvus::OpContext* ctx,
              const std::vector<cachinglayer::cid_t>& cids) override {
        for (auto c : cids) {
            fetched_->insert(c);
        }
        return TestChunkTranslator::get_cells(ctx, cids);
    }

 private:
    std::shared_ptr<std::set<cachinglayer::cid_t>> fetched_;
};

class CountingGroupChunkTranslator : public TestGroupChunkTranslator {
 public:
    CountingGroupChunkTranslator(
        size_t num_fields,
        std::vector<int64_t> rows,
        std::string key,
        std::vector<std::unique_ptr<GroupChunk>>&& chunks,
        std::shared_ptr<std::set<cachinglayer::cid_t>> fetched)
        : TestGroupChunkTranslator(
              num_fields, std::move(rows), std::move(key), std::move(chunks)),
          fetched_(std::move(fetched)) {
    }

    std::vector<std::pair<cachinglayer::cid_t, std::unique_ptr<GroupChunk>>>
    get_cells(milvus::OpContext* ctx,
              const std::vector<cachinglayer::cid_t>& cids) override {
        for (auto c : cids) {
            fetched_->insert(c);
        }
        return TestGroupChunkTranslator::get_cells(ctx, cids);
    }

 private:
    std::shared_ptr<std::set<cachinglayer::cid_t>> fetched_;
};

struct ChunkedColumnFactory {
    static ColumnFixture
    Create(const ColumnSpec& spec) {
        const auto n = spec.rows_per_chunk.size();
        auto buffers = std::make_shared<std::vector<std::vector<char>>>(n);
        std::vector<std::unique_ptr<Chunk>> chunks;
        chunks.reserve(n);
        for (size_t i = 0; i < n; ++i) {
            (*buffers)[i] = BuildChunkBuffer(
                spec.rows_per_chunk[i],
                spec.valid_patterns.empty() ? nullptr : &spec.valid_patterns[i],
                spec.nullable);
            chunks.push_back(MakeFixedChunk(spec.rows_per_chunk[i],
                                            spec.nullable,
                                            (*buffers)[i].data(),
                                            (*buffers)[i].size()));
        }
        auto fetched = std::make_shared<std::set<cachinglayer::cid_t>>();
        auto translator = std::make_unique<CountingChunkTranslator>(
            spec.rows_per_chunk, "cc_iface", std::move(chunks), fetched);
        FieldMeta fm(FieldName("t"),
                     FieldId(kTestFieldId),
                     DataType::INT32,
                     spec.nullable,
                     std::nullopt);
        auto slot = cachinglayer::Manager::GetInstance().CreateCacheSlot<Chunk>(
            std::move(translator), nullptr);
        auto column = std::make_shared<ChunkedColumn>(std::move(slot), fm);
        return {std::static_pointer_cast<ChunkedColumnInterface>(column),
                std::move(fetched),
                std::static_pointer_cast<void>(buffers)};
    }
};

struct ProxyChunkColumnFactory {
    static ColumnFixture
    Create(const ColumnSpec& spec) {
        const auto n = spec.rows_per_chunk.size();
        auto buffers = std::make_shared<std::vector<std::vector<char>>>(n);
        std::vector<std::unique_ptr<GroupChunk>> group_chunks;
        group_chunks.reserve(n);
        for (size_t i = 0; i < n; ++i) {
            (*buffers)[i] = BuildChunkBuffer(
                spec.rows_per_chunk[i],
                spec.valid_patterns.empty() ? nullptr : &spec.valid_patterns[i],
                spec.nullable);
            std::shared_ptr<Chunk> chunk =
                MakeFixedChunk(spec.rows_per_chunk[i],
                               spec.nullable,
                               (*buffers)[i].data(),
                               (*buffers)[i].size());
            std::unordered_map<FieldId, std::shared_ptr<Chunk>> fields;
            fields[FieldId(kTestFieldId)] = std::move(chunk);
            group_chunks.push_back(std::make_unique<GroupChunk>(fields));
        }
        auto fetched = std::make_shared<std::set<cachinglayer::cid_t>>();
        auto translator = std::make_unique<CountingGroupChunkTranslator>(
            /*num_fields=*/1,
            spec.rows_per_chunk,
            "pcc_iface",
            std::move(group_chunks),
            fetched);
        auto group =
            std::make_shared<ChunkedColumnGroup>(std::move(translator));
        FieldMeta fm(FieldName("t"),
                     FieldId(kTestFieldId),
                     DataType::INT32,
                     spec.nullable,
                     std::nullopt);
        auto column = std::make_shared<ProxyChunkColumn>(
            group, FieldId(kTestFieldId), fm);
        return {std::static_pointer_cast<ChunkedColumnInterface>(column),
                std::move(fetched),
                std::static_pointer_cast<void>(buffers)};
    }
};

}  // namespace

template <typename Factory>
class ChunkedColumnInterfaceTest : public ::testing::Test {};

using Factories =
    ::testing::Types<ChunkedColumnFactory, ProxyChunkColumnFactory>;
TYPED_TEST_SUITE(ChunkedColumnInterfaceTest, Factories);

TYPED_TEST(ChunkedColumnInterfaceTest,
           InitValidRowIdsPopulatesCountsWithoutPin) {
    ColumnSpec spec{{5, 3, 4},
                    {{true, false, true, true, false},
                     {false, false, false},
                     {true, true, true, true}},
                    true};
    auto fx = TypeParam::Create(spec);

    fx.column->InitValidRowIds({3, 0, 4});

    EXPECT_TRUE(fx.fetched->empty());

    const auto& cum = fx.column->GetNumValidRowsUntilChunk();
    ASSERT_EQ(cum.size(), 4u);
    EXPECT_EQ(cum[0], 0);
    EXPECT_EQ(cum[1], 3);
    EXPECT_EQ(cum[2], 3);
    EXPECT_EQ(cum[3], 7);

    const auto& per_chunk = fx.column->GetValidCountPerChunk();
    ASSERT_EQ(per_chunk.size(), 3u);
    EXPECT_EQ(per_chunk[0], 3);
    EXPECT_EQ(per_chunk[1], 0);
    EXPECT_EQ(per_chunk[2], 4);

    const auto& m = fx.column->GetOffsetMapping();
    EXPECT_TRUE(m.IsEnabled());
    EXPECT_EQ(m.GetTotalCount(), 12);
    EXPECT_EQ(m.GetValidCount(), 7);
    for (int64_t c = 0; c < 3; ++c) {
        EXPECT_FALSE(m.IsChunkSet(c)) << "chunk " << c;
    }
}

TYPED_TEST(ChunkedColumnInterfaceTest,
           EnsureChunkOffsetMappingFillsOnlyTouched) {
    ColumnSpec spec{{5, 3, 4},
                    {{true, false, true, true, false},
                     {false, false, false},
                     {true, true, true, true}},
                    true};
    auto fx = TypeParam::Create(spec);

    fx.column->InitValidRowIds({3, 0, 4});
    const auto& m = fx.column->GetOffsetMapping();

    fx.column->EnsureChunkOffsetMapping(2, nullptr);
    EXPECT_FALSE(m.IsChunkSet(0));
    EXPECT_FALSE(m.IsChunkSet(1));
    EXPECT_TRUE(m.IsChunkSet(2));
    EXPECT_EQ(fx.fetched->size(), 1u);
    EXPECT_EQ(fx.fetched->count(2), 1u);

    EXPECT_EQ(m.GetPhysicalOffset(8), 3);
    EXPECT_EQ(m.GetPhysicalOffset(9), 4);
    EXPECT_EQ(m.GetPhysicalOffset(10), 5);
    EXPECT_EQ(m.GetPhysicalOffset(11), 6);
    EXPECT_EQ(m.GetPhysicalOffset(0), -1);

    fx.column->EnsureChunkOffsetMapping(0, nullptr);
    EXPECT_TRUE(m.IsChunkSet(0));
    EXPECT_EQ(m.GetPhysicalOffset(0), 0);
    EXPECT_EQ(m.GetPhysicalOffset(2), 1);
    EXPECT_EQ(m.GetPhysicalOffset(3), 2);
    EXPECT_EQ(m.GetPhysicalOffset(1), -1);
    EXPECT_EQ(m.GetPhysicalOffset(4), -1);
    EXPECT_EQ(fx.fetched->size(), 2u);
    EXPECT_EQ(fx.fetched->count(1), 0u);

    // Idempotent.
    fx.column->EnsureChunkOffsetMapping(0, nullptr);
    EXPECT_EQ(fx.fetched->size(), 2u);
}

TYPED_TEST(ChunkedColumnInterfaceTest, EnsureChunkNoopOnEagerPath) {
    ColumnSpec spec{{4}, {{true, false, true, false}}, true};
    auto fx = TypeParam::Create(spec);

    fx.column->BuildValidRowIds(nullptr);
    const auto before = fx.column->GetOffsetMapping().GetValidCount();
    const auto fetched_before = fx.fetched->size();

    fx.column->EnsureChunkOffsetMapping(0, nullptr);

    EXPECT_EQ(fx.column->GetOffsetMapping().GetValidCount(), before);
    EXPECT_EQ(before, 2);
    EXPECT_EQ(fx.fetched->size(), fetched_before);
}

TYPED_TEST(ChunkedColumnInterfaceTest, InitValidRowIdsNonNullableIsNoop) {
    ColumnSpec spec{{5, 5}, {}, /*nullable=*/false};
    auto fx = TypeParam::Create(spec);

    fx.column->InitValidRowIds({5, 5});

    EXPECT_FALSE(fx.column->GetOffsetMapping().IsEnabled());
    EXPECT_TRUE(fx.column->GetValidCountPerChunk().empty());
    EXPECT_TRUE(fx.fetched->empty());
}

}  // namespace milvus
