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
#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/builder.h"
#include "common/BitsetView.h"
#include "common/Consts.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "exec/QueryContext.h"
#include "exec/expression/Expr.h"
#include "expr/ITypeExpr.h"
#include "milvus-storage/reader.h"
#include "pb/plan.pb.h"
#include "plan/PlanNode.h"
#include "segcore/ArrowSegment.h"
#include "test_utils/GenExprProto.h"

using namespace milvus;
using namespace milvus::segcore;

// ================================================================
// MockChunkReader: in-memory ChunkReader backed by RecordBatches
// ================================================================

class MockChunkReader : public milvus_storage::api::ChunkReader {
 public:
    explicit MockChunkReader(
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches)
        : batches_(std::move(batches)) {
    }

    size_t
    total_number_of_chunks() const override {
        return batches_.size();
    }

    arrow::Result<std::vector<int64_t>>
    get_chunk_indices(
        const std::vector<int64_t>& row_indices) override {
        return arrow::Status::NotImplemented(
            "MockChunkReader::get_chunk_indices");
    }

    arrow::Result<std::shared_ptr<arrow::RecordBatch>>
    get_chunk(int64_t chunk_index) override {
        loaded_chunks_.insert(chunk_index);
        return batches_.at(chunk_index);
    }

    arrow::Result<std::vector<std::shared_ptr<arrow::RecordBatch>>>
    get_chunks(const std::vector<int64_t>& chunk_indices,
               size_t parallelism = 1) override {
        std::vector<std::shared_ptr<arrow::RecordBatch>> result;
        for (auto idx : chunk_indices) {
            loaded_chunks_.insert(idx);
            result.push_back(batches_.at(idx));
        }
        return result;
    }

    arrow::Result<std::vector<uint64_t>>
    get_chunk_size() override {
        std::vector<uint64_t> sizes;
        for (const auto& batch : batches_) {
            uint64_t size = 0;
            for (int i = 0; i < batch->num_columns(); ++i) {
                for (const auto& buf :
                     batch->column(i)->data()->buffers) {
                    if (buf) {
                        size += buf->size();
                    }
                }
            }
            sizes.push_back(size);
        }
        return sizes;
    }

    arrow::Result<std::vector<uint64_t>>
    get_chunk_rows() override {
        std::vector<uint64_t> rows;
        for (const auto& batch : batches_) {
            rows.push_back(
                static_cast<uint64_t>(batch->num_rows()));
        }
        return rows;
    }

    const std::set<int64_t>&
    loaded_chunks() const {
        return loaded_chunks_;
    }

    void
    clear_loaded() {
        loaded_chunks_.clear();
    }

 private:
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches_;
    std::set<int64_t> loaded_chunks_;
};

// ================================================================
// Helpers
// ================================================================

// Build a RecordBatch with a single int64 column
static std::shared_ptr<arrow::RecordBatch>
MakeInt64Batch(const std::string& name,
               const std::vector<int64_t>& values) {
    arrow::Int64Builder builder;
    EXPECT_TRUE(builder.AppendValues(values).ok());
    auto array = builder.Finish().ValueOrDie();
    auto schema =
        arrow::schema({arrow::field(name, arrow::int64())});
    return arrow::RecordBatch::Make(
        schema, values.size(), {array});
}

// Build a RecordBatch with a single string column
static std::shared_ptr<arrow::RecordBatch>
MakeStringBatch(const std::string& name,
                const std::vector<std::string>& values) {
    arrow::StringBuilder builder;
    EXPECT_TRUE(builder.AppendValues(values).ok());
    auto array = builder.Finish().ValueOrDie();
    auto schema =
        arrow::schema({arrow::field(name, arrow::utf8())});
    return arrow::RecordBatch::Make(
        schema, values.size(), {array});
}

// Build a RecordBatch with int64 "age" + string "name" columns
static std::shared_ptr<arrow::RecordBatch>
MakeIntStringBatch(const std::vector<int64_t>& ages,
                   const std::vector<std::string>& names) {
    arrow::Int64Builder int_builder;
    EXPECT_TRUE(int_builder.AppendValues(ages).ok());
    auto int_array = int_builder.Finish().ValueOrDie();

    arrow::StringBuilder str_builder;
    EXPECT_TRUE(str_builder.AppendValues(names).ok());
    auto str_array = str_builder.Finish().ValueOrDie();

    auto schema =
        arrow::schema({arrow::field("age", arrow::int64()),
                       arrow::field("name", arrow::utf8())});
    return arrow::RecordBatch::Make(
        schema, ages.size(), {int_array, str_array});
}

// Build a RecordBatch with a nullable int64 column
static std::shared_ptr<arrow::RecordBatch>
MakeNullableInt64Batch(const std::string& name,
                       const std::vector<int64_t>& values,
                       const std::vector<bool>& valid) {
    arrow::Int64Builder builder;
    EXPECT_TRUE(builder.AppendValues(values, valid).ok());
    auto array = builder.Finish().ValueOrDie();
    auto schema = arrow::schema(
        {arrow::field(name, arrow::int64(), /*nullable=*/true)});
    return arrow::RecordBatch::Make(
        schema, values.size(), {array});
}

// Evaluate a filter expression on a segment, return column vector.
// The result is a bitmap ColumnVector (IsBitmap() == true).
// Use BitsetTypeView(col_vec->GetRawData(), col_vec->size()) to read.
static ColumnVectorPtr
EvalFilter(const SegmentInternalInterface* segment,
           const std::shared_ptr<milvus::expr::ITypeExpr>& typed_expr,
           int64_t active_count) {
    // gen_filter_res expects a FilterBitsNode directly.
    auto filter_node = std::make_shared<milvus::plan::FilterBitsNode>(
        DEFAULT_PLANNODE_ID, typed_expr);
    return milvus::test::gen_filter_res(
        filter_node.get(), segment, active_count, MAX_TIMESTAMP);
}

// ================================================================
// Test 1: Metadata
// ================================================================

TEST(ArrowSegmentTest, Metadata) {
    auto schema = std::make_shared<Schema>();
    auto age_fid =
        schema->AddDebugField("age", DataType::INT64);

    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    batches.push_back(
        MakeInt64Batch("age", std::vector<int64_t>(100, 1)));
    batches.push_back(
        MakeInt64Batch("age", std::vector<int64_t>(200, 2)));
    batches.push_back(
        MakeInt64Batch("age", std::vector<int64_t>(50, 3)));

    auto reader = std::make_shared<MockChunkReader>(batches);
    ArrowSegment seg(reader, schema);

    EXPECT_EQ(seg.get_row_count(), 350);
    EXPECT_EQ(seg.get_real_count(), 350);
    EXPECT_EQ(seg.get_deleted_count(), 0);
    EXPECT_EQ(seg.num_chunk(age_fid), 3);
    EXPECT_EQ(seg.num_chunk_data(age_fid), 3);
    EXPECT_EQ(seg.chunk_size(age_fid, 0), 100);
    EXPECT_EQ(seg.chunk_size(age_fid, 1), 200);
    EXPECT_EQ(seg.chunk_size(age_fid, 2), 50);
    EXPECT_EQ(seg.num_rows_until_chunk(age_fid, 0), 0);
    EXPECT_EQ(seg.num_rows_until_chunk(age_fid, 1), 100);
    EXPECT_EQ(seg.num_rows_until_chunk(age_fid, 2), 300);

    auto [chunk_id, local_off] =
        seg.get_chunk_by_offset(age_fid, 150);
    EXPECT_EQ(chunk_id, 1);
    EXPECT_EQ(local_off, 50);

    EXPECT_TRUE(seg.is_chunked());
    EXPECT_EQ(seg.get_max_timestamp(), MAX_TIMESTAMP);
    EXPECT_FALSE(seg.HasIndex(age_fid));

    // No get_chunk() calls during construction
    EXPECT_TRUE(reader->loaded_chunks().empty());
}

// ================================================================
// Test 2: Metadata with n:1 merge
// ================================================================

TEST(ArrowSegmentTest, MetadataWithMerge) {
    auto schema = std::make_shared<Schema>();
    auto age_fid =
        schema->AddDebugField("age", DataType::INT64);

    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    for (int i = 0; i < 6; ++i) {
        batches.push_back(
            MakeInt64Batch("age", std::vector<int64_t>(50, i)));
    }

    auto reader = std::make_shared<MockChunkReader>(batches);
    // chunks_per_cell = 3 => 6 physical chunks merge to 2 logical
    ArrowSegment seg(reader, schema, /*segment_id=*/0,
                     /*chunks_per_cell=*/3);

    EXPECT_EQ(seg.get_row_count(), 300);
    EXPECT_EQ(seg.num_chunk_data(age_fid), 2);
    EXPECT_EQ(seg.chunk_size(age_fid, 0), 150);
    EXPECT_EQ(seg.chunk_size(age_fid, 1), 150);
    EXPECT_EQ(seg.num_rows_until_chunk(age_fid, 0), 0);
    EXPECT_EQ(seg.num_rows_until_chunk(age_fid, 1), 150);

    // No chunks loaded yet
    EXPECT_TRUE(reader->loaded_chunks().empty());
}

// ================================================================
// Test 3: Scalar filter (int64) -- age > 20
// ================================================================

TEST(ArrowSegmentTest, ScalarFilterInt64) {
    auto schema = std::make_shared<Schema>();
    auto age_fid =
        schema->AddDebugField("age", DataType::INT64);

    auto batch = MakeInt64Batch("age", {25, 30, 18, 40, 22});
    auto reader = std::make_shared<MockChunkReader>(
        std::vector<std::shared_ptr<arrow::RecordBatch>>{batch});
    ArrowSegment seg(reader, schema);

    // Build expression: age > 20
    proto::plan::GenericValue val;
    val.set_int64_val(20);
    auto expr =
        std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(age_fid, DataType::INT64),
            proto::plan::OpType::GreaterThan, val);

    auto col_vec = EvalFilter(&seg, expr, seg.get_row_count());
    ASSERT_NE(col_vec, nullptr);
    BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
    // rows: 25>20=T, 30>20=T, 18>20=F, 40>20=T, 22>20=T
    EXPECT_TRUE(view[0]);
    EXPECT_TRUE(view[1]);
    EXPECT_FALSE(view[2]);
    EXPECT_TRUE(view[3]);
    EXPECT_TRUE(view[4]);
}

// ================================================================
// Test 4: String filter (varchar) -- name == "alice"
// ================================================================

TEST(ArrowSegmentTest, StringFilterVarchar) {
    auto schema = std::make_shared<Schema>();
    auto name_fid =
        schema->AddDebugField("name", DataType::VARCHAR);

    auto batch = MakeStringBatch(
        "name", {"alice", "bob", "charlie", "dave", "eve"});
    auto reader = std::make_shared<MockChunkReader>(
        std::vector<std::shared_ptr<arrow::RecordBatch>>{batch});
    ArrowSegment seg(reader, schema);

    // Build expression: name == "alice"
    proto::plan::GenericValue val;
    val.set_string_val("alice");
    auto expr =
        std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(name_fid, DataType::VARCHAR),
            proto::plan::OpType::Equal, val);

    auto col_vec = EvalFilter(&seg, expr, seg.get_row_count());
    ASSERT_NE(col_vec, nullptr);
    BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
    EXPECT_TRUE(view[0]);   // "alice" == "alice"
    EXPECT_FALSE(view[1]);  // "bob"
    EXPECT_FALSE(view[2]);  // "charlie"
    EXPECT_FALSE(view[3]);  // "dave"
    EXPECT_FALSE(view[4]);  // "eve"
}

// ================================================================
// Test 5: Multi-chunk scalar filter -- age > 20
// ================================================================

TEST(ArrowSegmentTest, MultiChunkScalarFilter) {
    auto schema = std::make_shared<Schema>();
    auto age_fid =
        schema->AddDebugField("age", DataType::INT64);

    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    batches.push_back(MakeInt64Batch("age", {25, 30, 18}));
    batches.push_back(MakeInt64Batch("age", {40, 22, 35}));
    batches.push_back(MakeInt64Batch("age", {10, 50}));

    auto reader = std::make_shared<MockChunkReader>(batches);
    ArrowSegment seg(reader, schema);

    EXPECT_EQ(seg.get_row_count(), 8);
    EXPECT_EQ(seg.num_chunk_data(age_fid), 3);

    // Build expression: age > 20
    proto::plan::GenericValue val;
    val.set_int64_val(20);
    auto expr =
        std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(age_fid, DataType::INT64),
            proto::plan::OpType::GreaterThan, val);

    auto col_vec = EvalFilter(&seg, expr, seg.get_row_count());
    ASSERT_NE(col_vec, nullptr);
    BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
    // chunk 0: 25>20=T, 30>20=T, 18>20=F
    // chunk 1: 40>20=T, 22>20=T, 35>20=T
    // chunk 2: 10>20=F, 50>20=T
    EXPECT_TRUE(view[0]);
    EXPECT_TRUE(view[1]);
    EXPECT_FALSE(view[2]);
    EXPECT_TRUE(view[3]);
    EXPECT_TRUE(view[4]);
    EXPECT_TRUE(view[5]);
    EXPECT_FALSE(view[6]);
    EXPECT_TRUE(view[7]);

    // All 3 chunks were loaded
    EXPECT_EQ(reader->loaded_chunks().size(), 3u);
}

// ================================================================
// Test 6: Compound filter (AND) -- age > 20 AND name != "bob"
// ================================================================

TEST(ArrowSegmentTest, CompoundFilterAnd) {
    auto schema = std::make_shared<Schema>();
    auto age_fid =
        schema->AddDebugField("age", DataType::INT64);
    auto name_fid =
        schema->AddDebugField("name", DataType::VARCHAR);

    auto batch = MakeIntStringBatch(
        {25, 30, 18, 40, 22}, {"a", "bob", "c", "bob", "e"});
    auto reader = std::make_shared<MockChunkReader>(
        std::vector<std::shared_ptr<arrow::RecordBatch>>{batch});
    ArrowSegment seg(reader, schema);

    // age > 20
    proto::plan::GenericValue age_val;
    age_val.set_int64_val(20);
    auto age_expr =
        std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(age_fid, DataType::INT64),
            proto::plan::OpType::GreaterThan, age_val);

    // name != "bob"
    proto::plan::GenericValue name_val;
    name_val.set_string_val("bob");
    auto name_expr =
        std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(name_fid, DataType::VARCHAR),
            proto::plan::OpType::NotEqual, name_val);

    // AND
    auto and_expr =
        std::make_shared<milvus::expr::LogicalBinaryExpr>(
            milvus::expr::LogicalBinaryExpr::OpType::And,
            age_expr, name_expr);

    auto col_vec =
        EvalFilter(&seg, and_expr, seg.get_row_count());
    ASSERT_NE(col_vec, nullptr);
    BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
    // row 0: 25>20=T && "a"!="bob"=T  => T
    // row 1: 30>20=T && "bob"!="bob"=F => F
    // row 2: 18>20=F && ...            => F
    // row 3: 40>20=T && "bob"!="bob"=F => F
    // row 4: 22>20=T && "e"!="bob"=T   => T
    EXPECT_TRUE(view[0]);
    EXPECT_FALSE(view[1]);
    EXPECT_FALSE(view[2]);
    EXPECT_FALSE(view[3]);
    EXPECT_TRUE(view[4]);
}

// ================================================================
// Test 7: Nullable field -- age > 20 with nulls
// ================================================================

TEST(ArrowSegmentTest, NullableField) {
    auto schema = std::make_shared<Schema>();
    auto age_fid = schema->AddDebugField(
        "age", DataType::INT64, /*nullable=*/true);

    // values=[25, 0, 18, 0, 22], valid=[T, F, T, F, T]
    auto batch = MakeNullableInt64Batch(
        "age", {25, 0, 18, 0, 22},
        {true, false, true, false, true});
    auto reader = std::make_shared<MockChunkReader>(
        std::vector<std::shared_ptr<arrow::RecordBatch>>{batch});
    ArrowSegment seg(reader, schema);

    // Build expression: age > 20
    proto::plan::GenericValue val;
    val.set_int64_val(20);
    auto expr =
        std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(
                age_fid, DataType::INT64, {}, /*nullable=*/true),
            proto::plan::OpType::GreaterThan, val);

    auto col_vec = EvalFilter(&seg, expr, seg.get_row_count());
    ASSERT_NE(col_vec, nullptr);
    BitsetTypeView result(col_vec->GetRawData(), col_vec->size());
    // row 0: 25>20=T, valid
    // row 1: null => invalid
    // row 2: 18>20=F, valid
    // row 3: null => invalid
    // row 4: 22>20=T, valid
    EXPECT_TRUE(result[0]);
    EXPECT_FALSE(result[2]);
    EXPECT_TRUE(result[4]);

    // Check validity using TargetBitmapView
    auto* valid_raw = col_vec->GetValidRawData();
    ASSERT_NE(valid_raw, nullptr);
    TargetBitmapView valid_view(valid_raw, col_vec->size());
    EXPECT_TRUE(valid_view[0]);
    EXPECT_FALSE(valid_view[1]);
    EXPECT_TRUE(valid_view[2]);
    EXPECT_FALSE(valid_view[3]);
    EXPECT_TRUE(valid_view[4]);
}

// ================================================================
// Test 8: On-demand loading verification
// ================================================================

TEST(ArrowSegmentTest, OnDemandLoading) {
    auto schema = std::make_shared<Schema>();
    auto age_fid =
        schema->AddDebugField("age", DataType::INT64);

    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    for (int i = 0; i < 10; ++i) {
        batches.push_back(MakeInt64Batch(
            "age", std::vector<int64_t>(100, i * 10)));
    }

    auto reader = std::make_shared<MockChunkReader>(batches);
    ArrowSegment seg(reader, schema);

    // No chunks loaded during construction
    EXPECT_TRUE(reader->loaded_chunks().empty());
    EXPECT_EQ(seg.get_row_count(), 1000);

    // Build expression: age > 5 (match most rows)
    proto::plan::GenericValue val;
    val.set_int64_val(5);
    auto expr =
        std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(age_fid, DataType::INT64),
            proto::plan::OpType::GreaterThan, val);

    auto col_vec = EvalFilter(&seg, expr, seg.get_row_count());
    ASSERT_NE(col_vec, nullptr);

    // After evaluation, all 10 chunks should have been loaded
    EXPECT_EQ(reader->loaded_chunks().size(), 10u);
}

// ================================================================
// Test 9: Term-in filter -- id IN (2, 4)
// ================================================================

TEST(ArrowSegmentTest, TermInFilter) {
    auto schema = std::make_shared<Schema>();
    auto id_fid =
        schema->AddDebugField("id", DataType::INT64);

    auto batch = MakeInt64Batch("id", {1, 2, 3, 4, 5});
    auto reader = std::make_shared<MockChunkReader>(
        std::vector<std::shared_ptr<arrow::RecordBatch>>{batch});
    ArrowSegment seg(reader, schema);

    // Build expression: id IN (2, 4)
    std::vector<proto::plan::GenericValue> vals;
    {
        proto::plan::GenericValue v;
        v.set_int64_val(2);
        vals.push_back(v);
    }
    {
        proto::plan::GenericValue v;
        v.set_int64_val(4);
        vals.push_back(v);
    }
    auto expr = std::make_shared<milvus::expr::TermFilterExpr>(
        expr::ColumnInfo(id_fid, DataType::INT64), vals);

    auto col_vec = EvalFilter(&seg, expr, seg.get_row_count());
    ASSERT_NE(col_vec, nullptr);
    BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
    EXPECT_FALSE(view[0]);  // 1 not in {2,4}
    EXPECT_TRUE(view[1]);   // 2 in {2,4}
    EXPECT_FALSE(view[2]);  // 3 not in {2,4}
    EXPECT_TRUE(view[3]);   // 4 in {2,4}
    EXPECT_FALSE(view[4]);  // 5 not in {2,4}
}
