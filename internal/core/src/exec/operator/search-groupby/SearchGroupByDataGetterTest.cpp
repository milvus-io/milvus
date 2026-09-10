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
#include <sys/mman.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/Chunk.h"
#include "common/GroupChunk.h"
#include "exec/operator/search-groupby/SearchGroupByOperator.h"
#include "mmap/ChunkedColumn.h"
#include "mmap/ChunkedColumnGroup.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "test_utils/cachinglayer_test_utils.h"

namespace milvus::exec {
namespace {

struct StringAccessStats {
    int full_views = 0;
    std::vector<int64_t> pinned_chunks;
    bool fail_pin = false;
};

template <typename Base>
class CountingStringColumn : public Base {
 public:
    template <typename... Args>
    CountingStringColumn(std::shared_ptr<StringAccessStats> stats,
                         Args&&... args)
        : Base(std::forward<Args>(args)...), stats_(std::move(stats)) {
    }

    PinWrapper<std::pair<std::vector<std::string_view>, ValidityView>>
    StringViews(
        milvus::OpContext* ctx,
        int64_t chunk_id,
        std::optional<std::pair<int64_t, int64_t>> range) const override {
        ++stats_->full_views;
        return Base::StringViews(ctx, chunk_id, range);
    }

    PinWrapper<Chunk*>
    GetChunk(milvus::OpContext* ctx, int64_t chunk_id) const override {
        if (stats_->fail_pin) {
            ThrowInfo(ErrorCode::FileReadFailed,
                      "injected string chunk failure");
        }
        stats_->pinned_chunks.push_back(chunk_id);
        return Base::GetChunk(ctx, chunk_id);
    }

 private:
    std::shared_ptr<StringAccessStats> stats_;
};

// Use real chunked columns/cache pins, without requiring binlog loading or ANN
// construction to exercise the group-by getter's random-access contract.
class StringColumnSegment : public segcore::ChunkedSegmentSealedImpl {
 public:
    StringColumnSegment(SchemaPtr schema,
                        FieldId field_id,
                        std::shared_ptr<ChunkedColumnInterface> column)
        : ChunkedSegmentSealedImpl(schema,
                                   empty_index_meta,
                                   segcore::SegcoreConfig::default_config(),
                                   100),
          field_id_(field_id),
          column_(std::move(column)) {
    }

    bool
    HasFieldData(FieldId field_id) const override {
        return field_id == field_id_;
    }

    std::shared_ptr<ChunkedColumnInterface>
    GetChunkedColumn(FieldId field_id) const override {
        return field_id == field_id_ ? column_ : nullptr;
    }

    std::pair<int64_t, int64_t>
    get_chunk_by_offset(FieldId, int64_t offset) const override {
        return column_->GetChunkIDByOffset(offset);
    }

 protected:
    PinWrapper<std::pair<std::vector<std::string_view>, ValidityView>>
    chunk_string_view_impl(
        milvus::OpContext* ctx,
        FieldId,
        int64_t chunk_id,
        std::optional<std::pair<int64_t, int64_t>> range) const override {
        return column_->StringViews(ctx, chunk_id, range);
    }

 private:
    FieldId field_id_;
    std::shared_ptr<ChunkedColumnInterface> column_;
};

class GroupByStringGetterTest
    : public ::testing::TestWithParam<std::tuple<bool, bool>> {
 protected:
    void
    SetUp() override {
        const auto [grouped, nullable] = GetParam();
        auto schema = std::make_shared<Schema>();
        auto pk = schema->AddDebugField("pk", DataType::INT64);
        schema->set_primary_field_id(pk);
        field_id_ = schema->AddDebugField("group", DataType::VARCHAR, nullable);
        const std::vector<int64_t> rows_per_chunk{32, 64, 96};
        std::vector<std::unique_ptr<Chunk>> chunks;
        std::vector<std::unique_ptr<GroupChunk>> group_chunks;
        int64_t first_row = 0;
        for (auto rows : rows_per_chunk) {
            const auto bitmap_bytes = nullable ? (rows + 7) / 8 : 0;
            const auto header_bytes =
                bitmap_bytes + (rows + 1) * sizeof(uint32_t);
            std::vector<std::string> values;
            size_t payload_bytes = 0;
            for (int64_t row = first_row; row < first_row + rows; ++row) {
                auto value = row == 0
                                 ? std::string{}
                                 : std::string(80, 'a') + std::to_string(row);
                if (row == 96) {
                    value = std::string("with\0nul", 8);
                }
                payload_bytes += value.size();
                values.push_back(value);
                expected_.push_back(nullable && row == 31
                                        ? std::nullopt
                                        : std::optional<std::string>(value));
            }
            std::vector<char> buffer(
                header_bytes + payload_bytes + MMAP_STRING_PADDING, 0);
            uint32_t offset = header_bytes;
            for (int64_t i = 0; i <= rows; ++i) {
                std::memcpy(buffer.data() + bitmap_bytes + i * sizeof(offset),
                            &offset,
                            sizeof(offset));
                if (i == rows) {
                    break;
                }
                if (nullable && expected_[first_row + i].has_value()) {
                    buffer[i / 8] |= 1 << (i % 8);
                }
                std::memcpy(
                    buffer.data() + offset, values[i].data(), values[i].size());
                offset += values[i].size();
            }
            auto* data = static_cast<char*>(mmap(nullptr,
                                                 buffer.size(),
                                                 PROT_READ | PROT_WRITE,
                                                 MAP_PRIVATE | MAP_ANONYMOUS,
                                                 -1,
                                                 0));
            ASSERT_NE(data, MAP_FAILED);
            auto guard =
                std::make_shared<ChunkMmapGuard>(data, buffer.size(), "");
            std::memcpy(data, buffer.data(), buffer.size());
            chunk_lifetimes_.push_back(guard);
            auto chunk = std::make_unique<StringChunk>(
                rows, data, buffer.size(), nullable, std::move(guard));
            if (grouped) {
                std::unordered_map<FieldId, std::shared_ptr<Chunk>> fields;
                fields.emplace(field_id_, std::move(chunk));
                group_chunks.push_back(
                    std::make_unique<GroupChunk>(std::move(fields)));
            } else {
                chunks.push_back(std::move(chunk));
            }
            first_row += rows;
        }

        const auto& field_meta = (*schema)[field_id_];
        stats_ = std::make_shared<StringAccessStats>();
        if (grouped) {
            auto translator = std::make_unique<TestGroupChunkTranslator>(
                1,
                rows_per_chunk,
                "group_by_string_group",
                std::move(group_chunks));
            auto group =
                std::make_shared<ChunkedColumnGroup>(std::move(translator));
            column_ = std::make_shared<CountingStringColumn<ProxyChunkColumn>>(
                stats_, std::move(group), field_id_, field_meta);
        } else {
            auto translator = std::make_unique<TestChunkTranslator>(
                rows_per_chunk, "group_by_string_column", std::move(chunks));
            auto slot =
                cachinglayer::Manager::GetInstance().CreateCacheSlot<Chunk>(
                    std::move(translator), nullptr);
            column_ = std::make_shared<
                CountingStringColumn<ChunkedVariableColumn<std::string>>>(
                stats_, std::move(slot), field_meta);
        }
        segment_ =
            std::make_unique<StringColumnSegment>(schema, field_id_, column_);
    }

    std::vector<std::weak_ptr<ChunkMmapGuard>> chunk_lifetimes_;
    std::vector<std::optional<std::string>> expected_;
    std::shared_ptr<StringAccessStats> stats_;
    std::shared_ptr<ChunkedColumnInterface> column_;
    std::unique_ptr<StringColumnSegment> segment_;
    FieldId field_id_{0};
};

TEST_P(GroupByStringGetterTest, SparseReadsDoNotMaterializeWholeChunks) {
    auto getter = GetDataGetter<std::string>(nullptr, *segment_, field_id_);
    for (auto offset : {96, 0, 31, 191, 96, 0}) {
        EXPECT_EQ(getter->Get(offset), expected_[offset]);
    }
    EXPECT_EQ(stats_->full_views, 0);
    // Chunk 1 is never touched; repeated reads reuse the two existing pins.
    EXPECT_EQ(stats_->pinned_chunks, (std::vector<int64_t>{2, 0}));
}

TEST_P(GroupByStringGetterTest, ReturnedStringsOwnTheirData) {
    std::optional<std::string> saved;
    {
        auto getter = GetDataGetter<std::string>(nullptr, *segment_, field_id_);
        saved = getter->Get(32);
        EXPECT_EQ(saved, expected_[32]);
        EXPECT_EQ(getter->Get(96), expected_[96]);
    }
    segment_.reset();
    column_.reset();
    EXPECT_TRUE(chunk_lifetimes_[1].expired());
    EXPECT_EQ(saved, expected_[32]);
}

TEST_P(GroupByStringGetterTest, CachedPinsProtectChunksFromEviction) {
    auto getter = GetDataGetter<std::string>(nullptr, *segment_, field_id_);
    EXPECT_EQ(getter->Get(32), expected_[32]);
    column_->ManualEvictCache();
    EXPECT_FALSE(chunk_lifetimes_[1].expired());
    EXPECT_EQ(getter->Get(32), expected_[32]);

    getter.reset();
    column_->ManualEvictCache();
    EXPECT_TRUE(chunk_lifetimes_[1].expired());
}

TEST_P(GroupByStringGetterTest, ChunkReadFailureKeepsItsCode) {
    auto getter = GetDataGetter<std::string>(nullptr, *segment_, field_id_);
    stats_->fail_pin = true;
    try {
        getter->Get(32);
        FAIL() << "expected injected chunk read failure";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::FileReadFailed);
    }
    // A failed pin must not leave an unusable entry in the getter cache.
    stats_->fail_pin = false;
    EXPECT_EQ(getter->Get(32), expected_[32]);
}

INSTANTIATE_TEST_SUITE_P(ColumnBackends,
                         GroupByStringGetterTest,
                         ::testing::Combine(::testing::Bool(),
                                            ::testing::Bool()));

}  // namespace
}  // namespace milvus::exec
