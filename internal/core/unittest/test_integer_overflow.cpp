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

#include <folly/FBVector.h>
#include <gtest/gtest.h>
#include <algorithm>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include "bitset/bitset.h"
#include "common/IndexMeta.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "common/protobuf_utils.h"
#include "filemanager/InputStream.h"
#include "gtest/gtest.h"
#include "knowhere/comp/index_param.h"
#include "plan/PlanNode.h"
#include "query/ExecPlanNodeVisitor.h"
#include "query/Plan.h"
#include "query/PlanImpl.h"
#include "query/PlanNode.h"
#include "segcore/SegcoreConfig.h"
#include "segcore/SegmentGrowing.h"
#include "segcore/SegmentGrowingImpl.h"
#include "test_utils/DataGen.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;

TEST(Expr, IntegerOverflow) {
    // Test cases: string expression and expected predicate function
    std::vector<std::tuple<std::string, std::function<bool(int8_t)>>> testcases = {
        /////////////////////////////////////////////////////////// term
        // age in [20230704, 1, -1] - 20230704 overflows Int8
        {"age in [20230704, 1, -1]",
         [](int8_t v) { return v == 1 || v == -1; }},
        // NOT (age in [20230704, 1, -1])
        {"not (age in [20230704, 1, -1])",
         [](int8_t v) { return v != 1 && v != -1; }},

        /////////////////////////////////////////////////////////// unary range
        // age == 20230704 (overflow, always false)
        {"age == 20230704", [](int8_t v) { return false; }},
        // age != 20230704 (overflow, always true)
        {"age != 20230704", [](int8_t v) { return true; }},
        // age >= 20230704 (overflow positive, always false)
        {"age >= 20230704", [](int8_t v) { return false; }},
        // age >= -20230704 (overflow negative, always true)
        {"age >= -20230704", [](int8_t v) { return true; }},
        // age > 20230704 (overflow positive, always false)
        {"age > 20230704", [](int8_t v) { return false; }},
        // age > -20230704 (overflow negative, always true)
        {"age > -20230704", [](int8_t v) { return true; }},
        // age <= 20230704 (overflow positive, always true)
        {"age <= 20230704", [](int8_t v) { return true; }},
        // age <= -20230704 (overflow negative, always false)
        {"age <= -20230704", [](int8_t v) { return false; }},
        // age < 20230704 (overflow positive, always true)
        {"age < 20230704", [](int8_t v) { return true; }},
        // age < -20230704 (overflow negative, always false)
        {"age < -20230704", [](int8_t v) { return false; }},

        /////////////////////////////////////////////////////////// binary range
        // -20230704 < age < 1 (lower overflow, lower_inclusive=false, upper_inclusive=false)
        {"age > -20230704 and age < 1", [](int8_t v) { return v < 1; }},
        // -1 < age < 20230704 (upper overflow, lower_inclusive=false, upper_inclusive=false)
        {"age > -1 and age < 20230704", [](int8_t v) { return v > -1; }},
        // -20230704 < age < 20230704 (both overflow, always true)
        {"age > -20230704 and age < 20230704", [](int8_t v) { return true; }},

        // -20230704 <= age < 1 (lower overflow, lower_inclusive=true, upper_inclusive=false)
        {"age >= -20230704 and age < 1", [](int8_t v) { return v < 1; }},
        // -1 <= age < 20230704 (upper overflow, lower_inclusive=true, upper_inclusive=false)
        {"age >= -1 and age < 20230704", [](int8_t v) { return v >= -1; }},
        // -20230704 <= age < 20230704 (both overflow, always true)
        {"age >= -20230704 and age < 20230704", [](int8_t v) { return true; }},

        // -20230704 < age <= 1 (lower overflow, lower_inclusive=false, upper_inclusive=true)
        {"age > -20230704 and age <= 1", [](int8_t v) { return v <= 1; }},
        // -1 < age <= 20230704 (upper overflow, lower_inclusive=false, upper_inclusive=true)
        {"age > -1 and age <= 20230704", [](int8_t v) { return v > -1; }},
        // -20230704 < age <= 20230704 (both overflow, always true)
        {"age > -20230704 and age <= 20230704", [](int8_t v) { return true; }},

        // -20230704 <= age <= 1 (lower overflow, lower_inclusive=true, upper_inclusive=true)
        {"age >= -20230704 and age <= 1", [](int8_t v) { return v <= 1; }},
        // -1 <= age <= 20230704 (upper overflow, lower_inclusive=true, upper_inclusive=true)
        {"age >= -1 and age <= 20230704", [](int8_t v) { return v >= -1; }},
        // -20230704 <= age <= 20230704 (both overflow, always true)
        {"age >= -20230704 and age <= 20230704", [](int8_t v) { return true; }},

        /////////////////////////////////////////////////////////// binary arithmetic range
        // Add: age + 2560 == 2450
        {"age + 2560 == 2450", [](int8_t v) { return v + 2560 == 2450; }},
        // Add: age + 2560 != 2450
        {"age + 2560 != 2450", [](int8_t v) { return v + 2560 != 2450; }},
        // Sub: age - 2560 == 2450
        {"age - 2560 == 2450", [](int8_t v) { return v - 2560 == 2450; }},
        // Sub: age - 2560 != 2450
        {"age - 2560 != 2450", [](int8_t v) { return v - 2560 != 2450; }},
        // Mul: age * 256 == 16384
        {"age * 256 == 16384", [](int8_t v) { return v * 256 == 16384; }},
        // Mul: age * 256 != 16384
        {"age * 256 != 16384", [](int8_t v) { return v * 256 != 16384; }},
        // Div: age / 256 == 20230704 (result overflow)
        {"age / 256 == 20230704", [](int8_t v) { return v / 256 == 20230704; }},
        // Div: age / 256 != 20230704 (result overflow)
        {"age / 256 != 20230704", [](int8_t v) { return v / 256 != 20230704; }},
    };

    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i8_fid = schema->AddDebugField("age", DataType::INT8);
    auto i64_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<int8_t> age_col;
    int num_iters = 100;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_age_col = raw_data.get_col<int8_t>(i8_fid);
        age_col.insert(age_col.end(), new_age_col.begin(), new_age_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    ScopedSchemaHandle handle(*schema);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    for (auto [expr, ref_func] : testcases) {
        auto plan_str = handle.ParseSearch(
            expr, "fakevec", 10, "L2", "{\"nprobe\": 10}", 3);
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        BitsetType final;
        // vectorsearch node => mvcc node => filter node
        // just test filter node
        final = ExecuteQueryExpr(
            (plan->plan_node_->plannodes_->sources()[0])->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];

            auto val = age_col[i];
            auto ref = ref_func(val);
            ASSERT_EQ(ans, ref)
                << expr << "@" << i << "!!" << static_cast<int64_t>(val);
        }
    }
}

TEST(Expr, IntegerOverflowOnAllZeroSealedChunk) {
    // Overflowed literals on a sealed int8 chunk whose values are all 0: the
    // result must not depend on comparing the chunk's min/max against the
    // placeholder 0 (e.g. `age < 300` would prune the chunk to FALSE).
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i8_fid = schema->AddDebugField("age", DataType::INT8);
    auto i64_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    constexpr int64_t N = 1000;
    auto raw_data = DataGen(schema, N, 42);
    for (auto& field : *raw_data.raw_->mutable_fields_data()) {
        if (field.field_id() == i8_fid.get()) {
            for (auto& v :
                 *field.mutable_scalars()->mutable_int_data()->mutable_data()) {
                v = 0;
            }
        }
    }
    auto seg = CreateSealedWithFieldDataLoaded(schema, raw_data);

    const std::vector<std::pair<std::string, bool>> testcases = {
        {"age != 300", true},
        {"age < 300", true},
        {"age > -300", true},
        {"age == 300", false},
        {"age > 300", false},
    };
    ScopedSchemaHandle handle(*schema);
    for (const auto& [expr, all_true] : testcases) {
        auto plan_str = handle.ParseSearch(
            expr, "fakevec", 10, "L2", "{\"nprobe\": 10}", 3);
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        auto final = ExecuteQueryExpr(
            (plan->plan_node_->plannodes_->sources()[0])->sources()[0],
            seg.get(),
            N,
            MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N) << expr;
        EXPECT_EQ(final.count(), all_true ? static_cast<size_t>(N) : 0u)
            << expr;
    }
}
