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

#include <cstdint>
#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "common/Common.h"
#include "common/Consts.h"
#include "common/IndexMeta.h"
#include "common/Schema.h"
#include "expr/ITypeExpr.h"
#include "query/ExecPlanNodeVisitor.h"
#include "segcore/SegcoreConfig.h"
#include "segcore/SegmentChunkReader.h"
#include "segcore/SegmentGrowingImpl.h"
#include "test_utils/DataGen.h"

namespace milvus::segcore {

TEST(SegmentChunkReader, NumericVariantMismatchIsSystemError) {
    const data_access_type value = int64_t{7};

    try {
        (void)get_from_variant<int32_t>(value);
        FAIL() << "expected a variant type mismatch";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::UnexpectedError);
    }
}

TEST(SegmentChunkReader, StringVariantMismatchIsSystemError) {
    const data_access_type value = true;

    try {
        (void)get_from_variant<std::string>(value);
        FAIL() << "expected a variant type mismatch";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::UnexpectedError);
    }
}

TEST(SegmentChunkReader, MoveCursorForSingleChunkStopsAtBatchBoundary) {
    constexpr int64_t batch_size = 64;
    for (const int64_t chunk_rows : {32, 64, 256}) {
        SCOPED_TRACE(chunk_rows);
        auto schema = std::make_shared<Schema>();
        auto pk = schema->AddDebugField("pk", DataType::INT64);
        schema->set_primary_field_id(pk);
        SegcoreConfig config;
        config.set_chunk_rows(chunk_rows);
        auto segment =
            CreateGrowingSegment(schema, empty_index_meta, 1, config);

        for (const int64_t active_count :
             {3 * batch_size, 3 * batch_size + 1}) {
            SCOPED_TRACE(active_count);
            const SegmentChunkReader reader(
                nullptr, segment.get(), active_count);
            const int64_t num_chunk = upper_div(active_count, chunk_rows);
            int64_t chunk_id = 0;
            int64_t chunk_pos = 0;
            for (int64_t batch = 1; batch <= 3; ++batch) {
                reader.MoveCursorForSingleChunk(
                    chunk_id, chunk_pos, num_chunk, batch_size);
                ASSERT_EQ(chunk_id * chunk_rows + chunk_pos,
                          batch * batch_size);
            }
        }
    }
}

TEST(SegmentChunkReader, GrowingConjunctSkipKeepsNextBatchCorrect) {
    constexpr int64_t batch_size = 64;
    constexpr int64_t row_count = 3 * batch_size + 1;
    struct BatchSizeGuard {
        int64_t saved = EXEC_EVAL_EXPR_BATCH_SIZE.exchange(batch_size);
        ~BatchSizeGuard() {
            EXEC_EVAL_EXPR_BATCH_SIZE.store(saved);
        }
    } batch_guard;

    for (const int64_t chunk_rows : {32, 64, 256}) {
        SCOPED_TRACE(chunk_rows);
        SegcoreConfig config;
        config.set_chunk_rows(chunk_rows);
        auto schema = std::make_shared<Schema>();
        auto pk = schema->AddDebugField("pk", DataType::INT64);
        schema->set_primary_field_id(pk);
        auto gate = schema->AddDebugField("gate", DataType::INT64);
        auto left = schema->AddDebugField("left", DataType::INT64);
        auto right = schema->AddDebugField("right", DataType::INT64);
        auto data = DataGen(schema, row_count);
        for (auto& field : *data.raw_->mutable_fields_data()) {
            auto* values =
                field.mutable_scalars()->mutable_long_data()->mutable_data();
            for (int64_t i = 0; i < row_count; ++i) {
                // Alternating comparison results expose an off-by-one cursor.
                values->at(i) = field.field_id() == right.get()
                                    ? (i % 2 == 0 ? i + 1 : i - 1)
                                    : i;
            }
        }
        auto segment =
            CreateGrowingSegment(schema, empty_index_meta, 1, config);
        ASSERT_FALSE(segment->is_chunked());
        ASSERT_EQ(segment->size_per_chunk(), chunk_rows);
        auto offset = segment->PreInsert(row_count);
        segment->Insert(offset,
                        row_count,
                        data.row_ids_.data(),
                        data.timestamps_.data(),
                        data.raw_);

        // Cover one or several fully pruned batches, including a single-row tail.
        for (const int64_t skipped_batches : {1, 2, 3}) {
            SCOPED_TRACE(skipped_batches);
            proto::plan::GenericValue bound;
            bound.set_int64_val(skipped_batches * batch_size);
            auto gate_expr =
                std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
                    milvus::expr::ColumnInfo(gate, DataType::INT64),
                    proto::plan::OpType::GreaterEqual,
                    bound);
            auto compare_expr = std::make_shared<milvus::expr::CompareExpr>(
                left,
                right,
                DataType::INT64,
                DataType::INT64,
                proto::plan::OpType::LessThan);
            auto filter = std::make_shared<milvus::expr::LogicalBinaryExpr>(
                milvus::expr::LogicalBinaryExpr::OpType::And,
                gate_expr,
                compare_expr);
            auto plan = std::make_shared<milvus::plan::FilterBitsNode>(
                DEFAULT_PLANNODE_ID, filter);
            auto result = milvus::query::ExecuteQueryExpr(
                plan, segment.get(), row_count, MAX_TIMESTAMP);
            ASSERT_EQ(result.size(), row_count);
            for (int64_t i = 0; i < row_count; ++i) {
                ASSERT_EQ(result[i],
                          i >= skipped_batches * batch_size && i % 2 == 0)
                    << "row " << i;
            }
        }
    }
}

}  // namespace milvus::segcore
