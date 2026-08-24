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
#include <functional>
#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "common/Common.h"
#include "common/Consts.h"
#include "common/IndexMeta.h"
#include "common/Schema.h"
#include "expr/ITypeExpr.h"
#include "knowhere/comp/index_param.h"
#include "pb/plan.pb.h"
#include "query/ExecPlanNodeVisitor.h"
#include "query/PlanNode.h"
#include "segcore/SegcoreConfig.h"
#include "segcore/SegmentChunkReader.h"
#include "segcore/SegmentGrowingImpl.h"
#include "test_utils/DataGen.h"
#include "test_utils/SegcoreConfigUtils.h"

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

// MoveCursorForSingleChunk used to lack a break: once processed_rows reached
// batch_size the condition stayed true for every remaining row, so the loops ran
// to the end of the segment and left the cursor on the last row instead of on
// batch_size. Compare MoveCursorForMultipleChunk (same class) and
// MoveCursorForData (exec/expression/Expr.h), which both stop.
TEST(SegmentChunkReader, MoveCursorForSingleChunkStopsAtBatchBoundary) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk);
    auto segment = CreateGrowingSegment(schema, empty_index_meta);

    const int64_t size_per_chunk = segment->size_per_chunk();
    ASSERT_GT(size_per_chunk, 0);
    const int64_t active_count = 2 * size_per_chunk + 1;
    const SegmentChunkReader reader(nullptr, segment.get(), active_count);

    const int64_t num_chunk =
        (active_count + size_per_chunk - 1) / size_per_chunk;
    const int64_t batch_size = size_per_chunk;

    int64_t chunk_id = 0;
    int64_t chunk_pos = 0;
    reader.MoveCursorForSingleChunk(chunk_id, chunk_pos, num_chunk, batch_size);

    const int64_t absolute_offset = chunk_id * size_per_chunk + chunk_pos;
    EXPECT_EQ(absolute_offset, batch_size)
        << "cursor must advance by exactly batch_size";
    EXPECT_LT(absolute_offset, active_count)
        << "cursor was pushed to the segment end";

    // A second move continues from where the first one stopped.
    reader.MoveCursorForSingleChunk(chunk_id, chunk_pos, num_chunk, batch_size);
    EXPECT_EQ(chunk_id * size_per_chunk + chunk_pos, 2 * batch_size);
}

// The same bug seen through the expression stack, which is what makes it a
// correctness bug rather than a wasted scan: PhyConjunctFilterExpr skips the
// remaining inputs of an all-false batch by calling MoveCursor() on them, so a
// cursor left at the segment end makes the *next* batch of the skipped
// expression read nothing. CompareExpr on a growing segment is the shortest
// route to MoveCursorForSingleChunk -- CompareExpr.h takes it whenever the
// segment is not chunked.
TEST(SegmentChunkReader, GrowingConjunctSkipKeepsNextBatchCorrect) {
    // Both knobs are process-global. Small values keep the fixture cheap while
    // still giving several batches, and several chunks per batch so the
    // chunk-by-chunk walk inside the helper actually runs.
    constexpr int64_t kBatch = 64;
    constexpr int64_t kChunkRows = 32;
    constexpr int64_t N = 4 * kBatch;

    struct BatchSizeGuard {
        int64_t saved;
        ~BatchSizeGuard() {
            EXEC_EVAL_EXPR_BATCH_SIZE.store(saved);
        }
    } batch_guard{EXEC_EVAL_EXPR_BATCH_SIZE.load()};
    EXEC_EVAL_EXPR_BATCH_SIZE.store(kBatch);

    ScopedSegcoreConfigRestore config_restore;
    auto& config = SegcoreConfig::default_config();
    config.set_chunk_rows(kChunkRows);

    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk);
    auto gate = schema->AddDebugField("gate", DataType::INT64);
    auto left = schema->AddDebugField("left", DataType::INT64);
    auto right = schema->AddDebugField("right", DataType::INT64);

    auto raw_data = DataGen(schema, N);
    auto set_col = [&](FieldId field_id,
                       const std::function<int64_t(int64_t)>& gen) {
        for (auto& field_data : *raw_data.raw_->mutable_fields_data()) {
            if (field_data.field_id() != field_id.get()) {
                continue;
            }
            auto* col = field_data.mutable_scalars()
                            ->mutable_long_data()
                            ->mutable_data();
            for (int64_t i = 0; i < N; ++i) {
                col->at(i) = gen(i);
            }
            return;
        }
        FAIL() << "field " << field_id.get() << " missing from generated data";
    };
    // gate is false for the whole first batch and true for every later one, so
    // exactly the first batch short-circuits and skips the compare.
    set_col(gate, [](int64_t i) { return i; });
    // left < right alternates row by row: any odd cursor shift shows up as an
    // inverted result, and a cursor parked at the segment end as no hits at all.
    set_col(left, [](int64_t i) { return i; });
    set_col(right, [](int64_t i) { return i % 2 == 0 ? i + 1 : i - 1; });

    auto segment = CreateGrowingSegment(schema, empty_index_meta, 1, config);
    ASSERT_FALSE(segment->is_chunked());
    ASSERT_EQ(segment->size_per_chunk(), kChunkRows);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);

    proto::plan::GenericValue gate_bound;
    gate_bound.set_int64_val(kBatch);
    auto gate_expr = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
        milvus::expr::ColumnInfo(gate, DataType::INT64),
        proto::plan::OpType::GreaterEqual,
        gate_bound);
    auto compare_expr = std::make_shared<milvus::expr::CompareExpr>(
        left,
        right,
        DataType::INT64,
        DataType::INT64,
        proto::plan::OpType::LessThan);
    auto filter = std::make_shared<milvus::expr::LogicalBinaryExpr>(
        milvus::expr::LogicalBinaryExpr::OpType::And, gate_expr, compare_expr);
    auto plan = std::make_shared<milvus::plan::FilterBitsNode>(
        DEFAULT_PLANNODE_ID, filter);

    auto final =
        milvus::query::ExecuteQueryExpr(plan, segment.get(), N, MAX_TIMESTAMP);
    ASSERT_EQ(final.size(), N);
    for (int64_t i = 0; i < N; ++i) {
        const bool expected = i >= kBatch && i % 2 == 0;
        ASSERT_EQ(final[i], expected) << "cursor desync at row " << i;
    }
}

}  // namespace milvus::segcore
