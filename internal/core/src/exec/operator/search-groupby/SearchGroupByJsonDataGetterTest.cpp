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
#include <arrow/array/builder_binary.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/Chunk.h"
#include "common/ChunkWriter.h"
#include "common/GroupChunk.h"
#include "exec/operator/search-groupby/SearchGroupByOperator.h"
#include "mmap/ChunkedColumn.h"
#include "mmap/ChunkedColumnGroup.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "test_utils/cachinglayer_test_utils.h"

namespace milvus::exec {
namespace {

struct JsonAccessStats {
    int full_views = 0;
    std::vector<int64_t> pinned_chunks;
    bool fail_pin = false;
};

template <typename Base>
class CountingJsonColumn : public Base {
 public:
    template <typename... Args>
    CountingJsonColumn(std::shared_ptr<JsonAccessStats> stats, Args&&... args)
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
            ThrowInfo(ErrorCode::FileReadFailed, "injected JSON chunk failure");
        }
        stats_->pinned_chunks.push_back(chunk_id);
        return Base::GetChunk(ctx, chunk_id);
    }

 private:
    std::shared_ptr<JsonAccessStats> stats_;
};

// Use real chunked columns/cache pins, without requiring binlog loading or ANN
// construction to exercise the group-by getter's random-access contract.
class JsonColumnSegment : public segcore::ChunkedSegmentSealedImpl {
 public:
    JsonColumnSegment(SchemaPtr schema,
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
        return field_id == field_id_ && !hide_column ? column_ : nullptr;
    }

    std::pair<int64_t, int64_t>
    get_chunk_by_offset(FieldId, int64_t offset) const override {
        return column_->GetChunkIDByOffset(offset);
    }

    bool hide_column = false;

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

class GroupByJsonGetterTest
    : public ::testing::TestWithParam<std::tuple<bool, bool>> {
 protected:
    void
    SetUp() override {
        const auto [grouped, nullable] = GetParam();
        auto schema = std::make_shared<Schema>();
        auto pk = schema->AddDebugField("pk", DataType::INT64);
        schema->set_primary_field_id(pk);
        field_id_ = schema->AddDebugField("group", DataType::JSON, nullable);
        const std::vector<int64_t> rows_per_chunk{32, 64, 4104};
        std::vector<std::unique_ptr<Chunk>> chunks;
        std::vector<std::unique_ptr<GroupChunk>> group_chunks;
        int64_t first_row = 0;
        for (auto rows : rows_per_chunk) {
            arrow::StringBuilder builder;
            for (int64_t row = first_row; row < first_row + rows; ++row) {
                if (nullable && row == 31) {
                    ASSERT_TRUE(builder.AppendNull().ok());
                    expected_.push_back(std::nullopt);
                } else {
                    ASSERT_TRUE(
                        builder.Append(documents_[row % documents_.size()])
                            .ok());
                    expected_.push_back(StringValue(row));
                }
            }
            std::shared_ptr<arrow::Array> array;
            ASSERT_TRUE(builder.Finish(&array).ok());
            const arrow::ArrayVector arrays{array};
            JSONChunkWriter writer(nullable);
            const auto [size, written_rows] = writer.calculate_size(arrays);
            ASSERT_EQ(written_rows, rows);
            auto target = std::make_shared<MemChunkTarget>(size);
            writer.write_to_target(arrays, target);
            ASSERT_EQ(target->tell(), size);
            auto* data = target->release();
            auto guard = std::make_shared<ChunkMmapGuard>(data, size, "");
            chunk_lifetimes_.push_back(guard);
            auto chunk = std::make_unique<JSONChunk>(
                rows, data, size, nullable, std::move(guard));
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
        stats_ = std::make_shared<JsonAccessStats>();
        if (grouped) {
            auto translator = std::make_unique<TestGroupChunkTranslator>(
                1,
                rows_per_chunk,
                "group_by_json_group",
                std::move(group_chunks));
            auto group =
                std::make_shared<ChunkedColumnGroup>(std::move(translator));
            column_ = std::make_shared<CountingJsonColumn<ProxyChunkColumn>>(
                stats_, std::move(group), field_id_, field_meta);
        } else {
            auto translator = std::make_unique<TestChunkTranslator>(
                rows_per_chunk, "group_by_json_column", std::move(chunks));
            auto slot =
                cachinglayer::Manager::GetInstance().CreateCacheSlot<Chunk>(
                    std::move(translator), nullptr);
            column_ = std::make_shared<
                CountingJsonColumn<ChunkedVariableColumn<Json>>>(
                stats_, std::move(slot), field_meta);
        }
        segment_ =
            std::make_unique<JsonColumnSegment>(schema, field_id_, column_);
    }

    template <typename T>
    std::shared_ptr<DataGetter<T>>
    MakeGetter(std::optional<DataType> type,
               bool strict_cast = false,
               const std::string& path = "/value") {
        return GetDataGetter<T, Json>(
            nullptr, *segment_, field_id_, path, type, strict_cast);
    }

    std::optional<std::string>
    StringValue(int64_t row) const {
        switch (row % documents_.size()) {
            case 0:
                return std::string{};
            case 1:
                return std::string("a long escaped string: ") +
                       std::string("\xe4\xb8\xad\n\0tail", 9);
            default:
                return std::nullopt;
        }
    }

    const std::vector<std::string> documents_{
        R"({"value":""})",
        R"({"value":"a long escaped string: \u4e2d\n\u0000tail"})",
        R"({"value":42})",
        R"({"value":true})",
        R"({"value":null})",
        R"({})",
        R"({"value":[1,"x",false]})",
        R"({"value":{"k":"v"}})",
        R"({"value":-17})",
        R"({"value":false})",
        R"({"value":2.5})",
        R"({"nested":{"a/b":{"~key":"nested"}}})",
    };
    std::vector<std::weak_ptr<ChunkMmapGuard>> chunk_lifetimes_;
    std::vector<std::optional<std::string>> expected_;
    std::shared_ptr<JsonAccessStats> stats_;
    std::shared_ptr<ChunkedColumnInterface> column_;
    std::unique_ptr<JsonColumnSegment> segment_;
    FieldId field_id_{0};
};

TEST_P(GroupByJsonGetterTest, SparseReadsDoNotMaterializeWholeChunks) {
    auto getter = MakeGetter<std::string>(DataType::VARCHAR);
    for (auto offset : {96, 0, 31, 4199, 96, 1}) {
        EXPECT_EQ(getter->Get(offset), expected_[offset]);
    }
    EXPECT_EQ(stats_->full_views, 0);
    // Chunk 1 is never touched; repeated reads reuse the two existing pins.
    EXPECT_EQ(stats_->pinned_chunks, (std::vector<int64_t>{2, 0}));
}

TEST_P(GroupByJsonGetterTest, TypedValuesAndNulls) {
    auto strings = MakeGetter<std::string>(DataType::VARCHAR);
    auto integers = MakeGetter<int64_t>(DataType::INT64);
    auto booleans = MakeGetter<bool>(DataType::BOOL);
    for (int64_t row = 0; row < 12; ++row) {
        EXPECT_EQ(strings->Get(row), StringValue(row));
        EXPECT_EQ(integers->Get(row),
                  row == 2   ? std::optional<int64_t>(42)
                  : row == 8 ? std::optional<int64_t>(-17)
                             : std::nullopt);
        EXPECT_EQ(booleans->Get(row),
                  row == 3   ? std::optional<bool>(true)
                  : row == 9 ? std::optional<bool>(false)
                             : std::nullopt);
    }
    EXPECT_EQ(MakeGetter<int8_t>(DataType::INT8)->Get(8), -17);
    EXPECT_EQ(MakeGetter<int16_t>(DataType::INT16)->Get(8), -17);
    EXPECT_EQ(MakeGetter<int32_t>(DataType::INT32)->Get(8), -17);
}

TEST_P(GroupByJsonGetterTest, UntypedValuesPreserveJsonRepresentation) {
    auto getter = MakeGetter<std::string>(std::nullopt);
    EXPECT_EQ(getter->Get(0), R"("")");
    EXPECT_EQ(getter->Get(2), "42");
    EXPECT_EQ(getter->Get(3), "true");
    EXPECT_EQ(getter->Get(4), "null");
    EXPECT_EQ(getter->Get(5), std::nullopt);
    EXPECT_EQ(getter->Get(6), R"([1,"x",false])");
    EXPECT_EQ(getter->Get(7), R"({"k":"v"})");
    EXPECT_EQ(getter->Get(10), "2.5");
    EXPECT_EQ(MakeGetter<std::string>(std::nullopt, false, "")->Get(2),
              documents_[2]);
}

TEST_P(GroupByJsonGetterTest, EscapedNestedPathAndStrictCast) {
    EXPECT_EQ(
        MakeGetter<std::string>(DataType::VARCHAR, false, "/nested/a~1b/~0key")
            ->Get(11),
        "nested");
    auto strings = MakeGetter<std::string>(DataType::VARCHAR, true);
    EXPECT_EQ(strings->Get(0), "");
    for (auto row : {2, 4, 5, 6, 7}) {
        try {
            strings->Get(row);
            FAIL() << "expected strict cast failure for row " << row;
        } catch (const SegcoreError& error) {
            EXPECT_EQ(error.get_error_code(), ErrorCode::UnexpectedError);
        }
    }
    EXPECT_THROW(MakeGetter<int64_t>(DataType::INT64, true)->Get(1),
                 SegcoreError);
    EXPECT_THROW(MakeGetter<bool>(DataType::BOOL, true)->Get(2), SegcoreError);
    EXPECT_THROW(MakeGetter<std::string>(std::nullopt, true)->Get(5),
                 SegcoreError);
    if (std::get<1>(GetParam())) {
        EXPECT_EQ(strings->Get(31), std::nullopt);
        EXPECT_EQ(MakeGetter<std::string>(std::nullopt, true)->Get(31),
                  std::nullopt);
    }
}

TEST_P(GroupByJsonGetterTest, LastRowRetainsSimdjsonPadding) {
    // The last JSON in the final chunk has only the writer's tail padding.
    // Its nested string must parse successfully without copying the document.
    auto getter =
        MakeGetter<std::string>(DataType::VARCHAR, true, "/nested/a~1b/~0key");
    EXPECT_EQ(getter->Get(4199), "nested");
}

TEST_P(GroupByJsonGetterTest, ReturnedStringsOwnTheirData) {
    std::optional<std::string> saved;
    {
        auto getter = MakeGetter<std::string>(DataType::VARCHAR);
        saved = getter->Get(1);
        EXPECT_EQ(saved, expected_[1]);
        // Another parse may overwrite simdjson's decoded string buffer.
        EXPECT_EQ(MakeGetter<std::string>(
                      DataType::VARCHAR, false, "/nested/a~1b/~0key")
                      ->Get(107),
                  "nested");
        EXPECT_EQ(getter->Get(96), expected_[96]);
    }
    segment_.reset();
    column_.reset();
    EXPECT_TRUE(chunk_lifetimes_[0].expired());
    EXPECT_EQ(saved, expected_[1]);
}

TEST_P(GroupByJsonGetterTest, CachedPinsProtectChunksFromEviction) {
    auto getter = MakeGetter<std::string>(DataType::VARCHAR);
    EXPECT_EQ(getter->Get(37), expected_[37]);
    column_->ManualEvictCache();
    EXPECT_FALSE(chunk_lifetimes_[1].expired());
    EXPECT_EQ(getter->Get(37), expected_[37]);
    EXPECT_EQ(stats_->pinned_chunks, (std::vector<int64_t>{1}));

    getter.reset();
    column_->ManualEvictCache();
    EXPECT_TRUE(chunk_lifetimes_[1].expired());
}

TEST_P(GroupByJsonGetterTest, ChunkReadFailureKeepsItsCode) {
    auto getter = MakeGetter<std::string>(DataType::VARCHAR);
    stats_->fail_pin = true;
    try {
        getter->Get(37);
        FAIL() << "expected injected chunk read failure";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::FileReadFailed);
    }
    // A failed pin must not leave an unusable entry in the getter cache.
    stats_->fail_pin = false;
    EXPECT_EQ(getter->Get(37), expected_[37]);
    EXPECT_EQ(stats_->pinned_chunks, (std::vector<int64_t>{1}));
}

TEST_P(GroupByJsonGetterTest, MissingColumnDoesNotPoisonCache) {
    auto getter = MakeGetter<std::string>(DataType::VARCHAR);
    segment_->hide_column = true;
    try {
        getter->Get(37);
        FAIL() << "expected missing JSON column failure";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::UnexpectedError);
    }
    segment_->hide_column = false;
    EXPECT_EQ(getter->Get(37), expected_[37]);
    EXPECT_EQ(stats_->pinned_chunks, (std::vector<int64_t>{1}));
}

INSTANTIATE_TEST_SUITE_P(ColumnBackends,
                         GroupByJsonGetterTest,
                         ::testing::Combine(::testing::Bool(),
                                            ::testing::Bool()));

}  // namespace
}  // namespace milvus::exec
