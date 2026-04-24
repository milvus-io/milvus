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

// Example: Write data to Parquet via milvus-storage Writer, then read
// it back through Reader/ChunkReader and run Milvus expression
// filtering via ArrowSegment.

#include <gtest/gtest.h>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "common/BitsetView.h"
#include "common/Consts.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "exec/QueryContext.h"
#include "exec/expression/Expr.h"
#include "expr/ITypeExpr.h"
#include "milvus-storage/column_groups.h"
#include "milvus-storage/properties.h"
#include "milvus-storage/reader.h"
#include "milvus-storage/writer.h"
#include "pb/plan.pb.h"
#include "plan/PlanNode.h"
#include "segcore/ArrowSegment.h"
#include "test_utils/GenExprProto.h"

using namespace milvus;
using namespace milvus::segcore;

// ================================================================
// Helpers
// ================================================================

static milvus_storage::api::Properties
MakeLocalProperties(const std::string& root) {
    milvus_storage::api::Properties props;
    milvus_storage::api::SetValue(props, "fs.root.path", root.c_str());
    return props;
}

static std::unique_ptr<milvus_storage::api::ColumnGroupPolicy>
MakeSinglePolicy(const std::shared_ptr<arrow::Schema>& schema) {
    milvus_storage::api::Properties props;
    milvus_storage::api::SetValue(props, "writer.policy", "single");
    milvus_storage::api::SetValue(props, "format", "parquet");
    auto result =
        milvus_storage::api::ColumnGroupPolicy::create_column_group_policy(
            props, schema);
    EXPECT_TRUE(result.ok()) << result.status().ToString();
    return std::move(result).ValueOrDie();
}

static ColumnVectorPtr
EvalFilter(const SegmentInternalInterface* segment,
           const std::shared_ptr<milvus::expr::ITypeExpr>& typed_expr,
           int64_t active_count) {
    auto filter_node = std::make_shared<milvus::plan::FilterBitsNode>(
        DEFAULT_PLANNODE_ID, typed_expr);
    return milvus::test::gen_filter_res(filter_node.get(), segment,
                                        active_count, MAX_TIMESTAMP);
}

static void
PrintFilterResults(const std::string& label,
                   BitsetTypeView view,
                   int64_t count) {
    std::printf("  %s => [", label.c_str());
    for (int64_t i = 0; i < count; ++i) {
        std::printf("%s%d", i ? ", " : "", view[i] ? 1 : 0);
    }
    std::printf("]\n");
}

// ================================================================
// Test
// ================================================================

TEST(ArrowSegmentParquetExample, ReadAndFilter) {
    // --- Arrow schema ---
    auto arrow_schema =
        arrow::schema({arrow::field("user_id", arrow::int64()),
                       arrow::field("age", arrow::int64()),
                       arrow::field("city", arrow::utf8())});

    // --- Build test data ---
    //                                                                 // idx
    // cities chosen to exercise LIKE patterns: "San%" matches 3 rows  // ---
    std::vector<int64_t> user_ids = {1,  2,  3,  4,  5,  6,  7,  8,  9,  10};
    std::vector<int64_t> ages     = {25, 30, 18, 40, 22, 35, 28, 19, 45, 33};
    std::vector<std::string> cities = {
        "San Francisco",  // 0
        "New York",       // 1
        "San Jose",       // 2
        "Boston",         // 3
        "New York",       // 4
        "San Diego",      // 5
        "Boston",         // 6
        "New York",       // 7
        "Portland",       // 8
        "Austin",         // 9
    };

    arrow::Int64Builder id_builder, age_builder;
    arrow::StringBuilder city_builder;
    ASSERT_TRUE(id_builder.AppendValues(user_ids).ok());
    ASSERT_TRUE(age_builder.AppendValues(ages).ok());
    ASSERT_TRUE(city_builder.AppendValues(cities).ok());

    auto batch = arrow::RecordBatch::Make(
        arrow_schema, 10,
        {id_builder.Finish().ValueOrDie(),
         age_builder.Finish().ValueOrDie(),
         city_builder.Finish().ValueOrDie()});

    // --- Step 1: Write via milvus-storage Writer ---
    std::string root = "/tmp/arrow_segment_parquet_test";
    std::string base_path = "example_data";
    auto properties = MakeLocalProperties(root);

    auto writer = milvus_storage::api::Writer::create(
        base_path, arrow_schema, MakeSinglePolicy(arrow_schema),
        properties);
    ASSERT_NE(writer, nullptr);
    ASSERT_TRUE(writer->write(batch).ok());

    auto cgs_result = writer->close();
    ASSERT_TRUE(cgs_result.ok()) << cgs_result.status().ToString();
    auto cgs = std::move(cgs_result).ValueOrDie();

    // --- Step 2: Read back via milvus-storage Reader ---
    auto reader = milvus_storage::api::Reader::create(
        cgs, arrow_schema, /*needed_columns=*/nullptr, properties);
    ASSERT_NE(reader, nullptr);

    auto chunk_reader_result = reader->get_chunk_reader(0);
    ASSERT_TRUE(chunk_reader_result.ok())
        << chunk_reader_result.status().ToString();
    auto chunk_reader = std::move(chunk_reader_result).ValueOrDie();

    // --- Step 3: Build Milvus schema + ArrowSegment ---
    auto schema = std::make_shared<Schema>();
    auto user_id_fid =
        schema->AddDebugField("user_id", DataType::INT64);
    auto age_fid =
        schema->AddDebugField("age", DataType::INT64);
    auto city_fid =
        schema->AddDebugField("city", DataType::VARCHAR);

    ArrowSegment seg(std::move(chunk_reader), schema);
    int64_t total = seg.get_row_count();
    std::printf("Loaded: %lld rows, %lld chunks\n",
                static_cast<long long>(total),
                static_cast<long long>(seg.num_chunk_data(age_fid)));
    EXPECT_EQ(total, 10);

    // --- Filter 1: age > 30 ---
    {
        proto::plan::GenericValue val;
        val.set_int64_val(30);
        auto expr =
            std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
                expr::ColumnInfo(age_fid, DataType::INT64),
                proto::plan::OpType::GreaterThan, val);

        auto col_vec = EvalFilter(&seg, expr, total);
        ASSERT_NE(col_vec, nullptr);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());

        // ages: [25, 30, 18, 40, 22, 35, 28, 19, 45, 33]
        std::printf("\nFilter: age > 30\n");
        PrintFilterResults("result", view, total);
        EXPECT_FALSE(view[0]);
        EXPECT_FALSE(view[1]);
        EXPECT_FALSE(view[2]);
        EXPECT_TRUE(view[3]);   // 40
        EXPECT_FALSE(view[4]);
        EXPECT_TRUE(view[5]);   // 35
        EXPECT_FALSE(view[6]);
        EXPECT_FALSE(view[7]);
        EXPECT_TRUE(view[8]);   // 45
        EXPECT_TRUE(view[9]);   // 33
    }

    // --- Filter 2: city == "New York" ---
    {
        proto::plan::GenericValue val;
        val.set_string_val("New York");
        auto expr =
            std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
                expr::ColumnInfo(city_fid, DataType::VARCHAR),
                proto::plan::OpType::Equal, val);

        auto col_vec = EvalFilter(&seg, expr, total);
        ASSERT_NE(col_vec, nullptr);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());

        std::printf("\nFilter: city == 'New York'\n");
        PrintFilterResults("result", view, total);
        EXPECT_FALSE(view[0]);  // San Francisco
        EXPECT_TRUE(view[1]);   // New York
        EXPECT_FALSE(view[2]);  // San Jose
        EXPECT_FALSE(view[3]);  // Boston
        EXPECT_TRUE(view[4]);   // New York
        EXPECT_FALSE(view[5]);  // San Diego
        EXPECT_FALSE(view[6]);  // Boston
        EXPECT_TRUE(view[7]);   // New York
        EXPECT_FALSE(view[8]);  // Portland
        EXPECT_FALSE(view[9]);  // Austin
    }

    // --- Filter 3: city LIKE "San%" (SQL LIKE via OpType::Match) ---
    // Matches: San Francisco, San Jose, San Diego
    {
        proto::plan::GenericValue val;
        val.set_string_val("San%");
        auto expr =
            std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
                expr::ColumnInfo(city_fid, DataType::VARCHAR),
                proto::plan::OpType::Match, val);

        auto col_vec = EvalFilter(&seg, expr, total);
        ASSERT_NE(col_vec, nullptr);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());

        std::printf("\nFilter: city LIKE 'San%%'\n");
        PrintFilterResults("result", view, total);
        EXPECT_TRUE(view[0]);   // San Francisco
        EXPECT_FALSE(view[1]);  // New York
        EXPECT_TRUE(view[2]);   // San Jose
        EXPECT_FALSE(view[3]);  // Boston
        EXPECT_FALSE(view[4]);  // New York
        EXPECT_TRUE(view[5]);   // San Diego
        EXPECT_FALSE(view[6]);  // Boston
        EXPECT_FALSE(view[7]);  // New York
        EXPECT_FALSE(view[8]);  // Portland
        EXPECT_FALSE(view[9]);  // Austin
    }

    // --- Filter 4: city LIKE "%ork" (suffix match) ---
    // Matches: New York (rows 1, 4, 7)
    {
        proto::plan::GenericValue val;
        val.set_string_val("%ork");
        auto expr =
            std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
                expr::ColumnInfo(city_fid, DataType::VARCHAR),
                proto::plan::OpType::Match, val);

        auto col_vec = EvalFilter(&seg, expr, total);
        ASSERT_NE(col_vec, nullptr);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());

        std::printf("\nFilter: city LIKE '%%ork'\n");
        PrintFilterResults("result", view, total);
        EXPECT_FALSE(view[0]);
        EXPECT_TRUE(view[1]);   // New York
        EXPECT_FALSE(view[2]);
        EXPECT_FALSE(view[3]);
        EXPECT_TRUE(view[4]);   // New York
        EXPECT_FALSE(view[5]);
        EXPECT_FALSE(view[6]);
        EXPECT_TRUE(view[7]);   // New York
        EXPECT_FALSE(view[8]);
        EXPECT_FALSE(view[9]);
    }

    // --- Filter 5: city LIKE "%sto%" (substring match) ---
    // Matches: Boston (rows 3, 6) — "Bo*sto*n" contains "sto"
    // Austin does NOT match — "Au*sti*n" has "sti", not "sto"
    {
        proto::plan::GenericValue val;
        val.set_string_val("%sto%");
        auto expr =
            std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
                expr::ColumnInfo(city_fid, DataType::VARCHAR),
                proto::plan::OpType::Match, val);

        auto col_vec = EvalFilter(&seg, expr, total);
        ASSERT_NE(col_vec, nullptr);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());

        std::printf("\nFilter: city LIKE '%%sto%%'\n");
        PrintFilterResults("result", view, total);
        EXPECT_FALSE(view[0]);
        EXPECT_FALSE(view[1]);
        EXPECT_FALSE(view[2]);
        EXPECT_TRUE(view[3]);   // Boston
        EXPECT_FALSE(view[4]);
        EXPECT_FALSE(view[5]);
        EXPECT_TRUE(view[6]);   // Boston
        EXPECT_FALSE(view[7]);
        EXPECT_FALSE(view[8]);
        EXPECT_FALSE(view[9]);  // Austin — no "sto"
    }

    // --- Filter 6: age > 25 AND city LIKE "San%" ---
    // San Francisco(25), San Jose(18), San Diego(35)
    // age>25: only San Francisco(25>25=F), San Diego(35>25=T)
    {
        proto::plan::GenericValue age_val;
        age_val.set_int64_val(25);
        auto age_expr =
            std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
                expr::ColumnInfo(age_fid, DataType::INT64),
                proto::plan::OpType::GreaterThan, age_val);

        proto::plan::GenericValue city_val;
        city_val.set_string_val("San%");
        auto city_expr =
            std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
                expr::ColumnInfo(city_fid, DataType::VARCHAR),
                proto::plan::OpType::Match, city_val);

        auto and_expr =
            std::make_shared<milvus::expr::LogicalBinaryExpr>(
                milvus::expr::LogicalBinaryExpr::OpType::And,
                age_expr, city_expr);

        auto col_vec = EvalFilter(&seg, and_expr, total);
        ASSERT_NE(col_vec, nullptr);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());

        std::printf("\nFilter: age > 25 AND city LIKE 'San%%'\n");
        PrintFilterResults("result", view, total);
        EXPECT_FALSE(view[0]);  // San Francisco, age=25, 25>25=F
        EXPECT_FALSE(view[1]);
        EXPECT_FALSE(view[2]);  // San Jose, age=18, F
        EXPECT_FALSE(view[3]);
        EXPECT_FALSE(view[4]);
        EXPECT_TRUE(view[5]);   // San Diego, age=35, T
        EXPECT_FALSE(view[6]);
        EXPECT_FALSE(view[7]);
        EXPECT_FALSE(view[8]);
        EXPECT_FALSE(view[9]);
    }

    // --- Filter 7: user_id IN (3, 7, 10) ---
    {
        std::vector<proto::plan::GenericValue> vals;
        for (int64_t v : {3, 7, 10}) {
            proto::plan::GenericValue gv;
            gv.set_int64_val(v);
            vals.push_back(gv);
        }
        auto expr = std::make_shared<milvus::expr::TermFilterExpr>(
            expr::ColumnInfo(user_id_fid, DataType::INT64), vals);

        auto col_vec = EvalFilter(&seg, expr, total);
        ASSERT_NE(col_vec, nullptr);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());

        std::printf("\nFilter: user_id IN (3, 7, 10)\n");
        PrintFilterResults("result", view, total);
        EXPECT_FALSE(view[0]);
        EXPECT_FALSE(view[1]);
        EXPECT_TRUE(view[2]);   // 3
        EXPECT_FALSE(view[3]);
        EXPECT_FALSE(view[4]);
        EXPECT_FALSE(view[5]);
        EXPECT_TRUE(view[6]);   // 7
        EXPECT_FALSE(view[7]);
        EXPECT_FALSE(view[8]);
        EXPECT_TRUE(view[9]);   // 10
    }

    // Cleanup
    std::filesystem::remove_all(root);
}
