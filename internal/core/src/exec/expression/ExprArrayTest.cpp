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
#include <regex>
#include <vector>

#include "common/Types.h"
#include "expr/ITypeExpr.h"
#include "index/IndexFactory.h"
#include "pb/plan.pb.h"
#include "plan/PlanNode.h"
#include "query/Plan.h"
#include "query/PlanNode.h"
#include "query/ExecPlanNodeVisitor.h"
#include "segcore/SegmentGrowingImpl.h"
#include "simdjson/padded_string.h"
#include "test_utils/DataGen.h"
#include "test_utils/GenExprProto.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;

TEST(Expr, TestArrayRange) {
    std::vector<std::tuple<std::string,
                           std::string,
                           std::function<bool(milvus::Array & array)>>>
        testcases = {
            // binary_range_expr: 1 < long_array[0] < 10000
            {"1 < long_array[0] < 10000",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return 1 < val && val < 10000;
             }},
            // binary_range_expr: 1 < long_array[1024] < 10000
            {"1 < long_array[1024] < 10000",
             "long",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<int64_t>(1024);
                 return 1 < val && val < 10000;
             }},
            // binary_range_expr: 1 <= long_array[0] < 10000
            {"1 <= long_array[0] < 10000",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return 1 <= val && val < 10000;
             }},
            // binary_range_expr: 1 <= long_array[1024] < 10000
            {"1 <= long_array[1024] < 10000",
             "long",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<int64_t>(1024);
                 return 1 <= val && val < 10000;
             }},
            // binary_range_expr: 1 < long_array[0] <= 10000
            {"1 < long_array[0] <= 10000",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return 1 < val && val <= 10000;
             }},
            // binary_range_expr: 1 < long_array[1024] <= 10000
            {"1 < long_array[1024] <= 10000",
             "long",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<int64_t>(1024);
                 return 1 < val && val <= 10000;
             }},
            // binary_range_expr: 1 <= long_array[0] <= 10000
            {"1 <= long_array[0] <= 10000",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return 1 <= val && val <= 10000;
             }},
            // binary_range_expr: 1 <= long_array[1024] <= 10000
            {"1 <= long_array[1024] <= 10000",
             "long",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<int64_t>(1024);
                 return 1 <= val && val <= 10000;
             }},
            // binary_range_expr: "aaa" <= string_array[0] <= "zzz"
            {R"("aaa" <= string_array[0] <= "zzz")",
             "string",
             [](milvus::Array& array) {
                 auto val = array.get_data<std::string_view>(0);
                 return "aaa" <= val && val <= "zzz";
             }},
            // binary_range_expr: 1.1 <= double_array[0] <= 2048.12
            {"1.1 <= double_array[0] <= 2048.12",
             "float",
             [](milvus::Array& array) {
                 auto val = array.get_data<double>(0);
                 return 1.1 <= val && val <= 2048.12;
             }},
            // unary_range_expr: long_array[0] >= 10000
            {"long_array[0] >= 10000",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val >= 10000;
             }},
            // unary_range_expr: long_array[0] > 2000
            {"long_array[0] > 2000",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val > 2000;
             }},
            // unary_range_expr: long_array[0] <= 2000
            {"long_array[0] <= 2000",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val <= 2000;
             }},
            // unary_range_expr: long_array[0] < 2000
            {"long_array[0] < 2000",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val < 2000;
             }},
            // unary_range_expr: long_array[0] == 2000
            {"long_array[0] == 2000",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val == 2000;
             }},
            // unary_range_expr: long_array[0] != 2000
            {"long_array[0] != 2000",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val != 2000;
             }},
            // unary_range_expr: bool_array[0] == false
            {"bool_array[0] == false",
             "bool",
             [](milvus::Array& array) {
                 auto val = array.get_data<bool>(0);
                 return !val;
             }},
            // unary_range_expr: string_array[0] == "abc"
            {R"(string_array[0] == "abc")",
             "string",
             [](milvus::Array& array) {
                 auto val = array.get_data<std::string_view>(0);
                 return val == "abc";
             }},
            // unary_range_expr: double_array[0] == 2.2
            {"double_array[0] == 2.2",
             "float",
             [](milvus::Array& array) {
                 auto val = array.get_data<double>(0);
                 return val == 2.2;
             }},
            // unary_range_expr: double_array[1024] == 2.2
            {"double_array[1024] == 2.2",
             "float",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<double>(1024);
                 return val == 2.2;
             }},
            // unary_range_expr: double_array[1024] != 2.2
            {"double_array[1024] != 2.2",
             "float",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<double>(1024);
                 return val != 2.2;
             }},
            // unary_range_expr: double_array[1024] >= 2.2
            {"double_array[1024] >= 2.2",
             "float",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<double>(1024);
                 return val >= 2.2;
             }},
            // unary_range_expr: double_array[1024] > 2.2
            {"double_array[1024] > 2.2",
             "float",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<double>(1024);
                 return val > 2.2;
             }},
            // unary_range_expr: double_array[1024] <= 2.2
            {"double_array[1024] <= 2.2",
             "float",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<double>(1024);
                 return val <= 2.2;
             }},
            // unary_range_expr: double_array[1024] < 2.2
            {"double_array[1024] < 2.2",
             "float",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<double>(1024);
                 return val < 2.2;
             }},

        };
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto long_array_fid =
        schema->AddDebugField("long_array", DataType::ARRAY, DataType::INT64);
    auto bool_array_fid =
        schema->AddDebugField("bool_array", DataType::ARRAY, DataType::BOOL);
    auto string_array_fid = schema->AddDebugField(
        "string_array", DataType::ARRAY, DataType::VARCHAR);
    auto float_array_fid =
        schema->AddDebugField("double_array", DataType::ARRAY, DataType::FLOAT);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::map<std::string, std::vector<ScalarFieldProto>> array_cols;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_long_array_col =
            raw_data.get_col<ScalarFieldProto>(long_array_fid);
        auto new_bool_array_col =
            raw_data.get_col<ScalarFieldProto>(bool_array_fid);
        auto new_string_array_col =
            raw_data.get_col<ScalarFieldProto>(string_array_fid);
        auto new_float_array_col =
            raw_data.get_col<ScalarFieldProto>(float_array_fid);

        array_cols["long"].insert(array_cols["long"].end(),
                                  new_long_array_col.begin(),
                                  new_long_array_col.end());
        array_cols["bool"].insert(array_cols["bool"].end(),
                                  new_bool_array_col.begin(),
                                  new_bool_array_col.end());
        array_cols["string"].insert(array_cols["string"].end(),
                                    new_string_array_col.begin(),
                                    new_string_array_col.end());
        array_cols["float"].insert(array_cols["float"].end(),
                                   new_float_array_col.begin(),
                                   new_float_array_col.end());

        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    ScopedSchemaHandle schema_handle(*schema);
    for (auto [expr, array_type, ref_func] : testcases) {
        auto plan_str = schema_handle.ParseSearch(
            expr, "fakevec", 10, "L2", R"({"nprobe": 10})", 3);
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Array(array_cols[array_type][i]);
            auto ref = ref_func(array);
            ASSERT_EQ(ans, ref);
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref);
            }
        }
    }
}

TEST(Expr, TestArrayEqual) {
    std::vector<
        std::tuple<std::string, std::function<bool(std::vector<int64_t>)>>>
        testcases = {
            // unary_range_expr: long_array == [1, 2, 3]
            {"long_array == [1, 2, 3]",
             [](std::vector<int64_t> v) {
                 if (v.size() != 3) {
                     return false;
                 }
                 for (int i = 0; i < 3; ++i) {
                     if (v[i] != i + 1) {
                         return false;
                     }
                 }
                 return true;
             }},
            // unary_range_expr: long_array != [1, 2, 3]
            {"long_array != [1, 2, 3]",
             [](std::vector<int64_t> v) {
                 if (v.size() != 3) {
                     return true;
                 }
                 for (int i = 0; i < 3; ++i) {
                     if (v[i] != i + 1) {
                         return true;
                     }
                 }
                 return false;
             }},
        };
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto long_array_fid =
        schema->AddDebugField("long_array", DataType::ARRAY, DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<ScalarFieldProto> long_array_col;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter, 0, 1, 3);
        auto new_long_array_col =
            raw_data.get_col<ScalarFieldProto>(long_array_fid);
        long_array_col.insert(long_array_col.end(),
                              new_long_array_col.begin(),
                              new_long_array_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    ScopedSchemaHandle schema_handle(*schema);
    for (auto [expr, ref_func] : testcases) {
        auto plan_str = schema_handle.ParseSearch(
            expr, "fakevec", 10, "L2", R"({"nprobe": 10})", 3);
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Array(long_array_col[i]);
            std::vector<int64_t> array_values(array.length());
            for (int j = 0; j < array.length(); ++j) {
                array_values.push_back(array.get_data<int64_t>(j));
            }
            auto ref = ref_func(array_values);
            ASSERT_EQ(ans, ref);
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref);
            }
        }
    }
}

TEST(Expr, TestArrayNullExpr) {
    std::vector<std::tuple<std::string, std::function<bool(bool)>>> testcases =
        {
            // null_expr: long_array is null
            {"long_array is null", [](bool v) { return !v; }},
        };
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto long_array_fid = schema->AddDebugField(
        "long_array", DataType::ARRAY, DataType::INT64, true);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<ScalarFieldProto> long_array_col;
    int num_iters = 1;
    FixedVector<bool> valid_data;

    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter, 0, 1, 3);
        auto new_long_array_col =
            raw_data.get_col<ScalarFieldProto>(long_array_fid);
        long_array_col.insert(long_array_col.end(),
                              new_long_array_col.begin(),
                              new_long_array_col.end());
        auto new_valid_col = raw_data.get_col_valid(long_array_fid);
        valid_data.insert(
            valid_data.end(), new_valid_col.begin(), new_valid_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    ScopedSchemaHandle schema_handle(*schema);
    for (auto [expr, ref_func] : testcases) {
        auto plan_str = schema_handle.ParseSearch(
            expr, "fakevec", 10, "L2", R"({"nprobe": 10})", 3);
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);
        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto valid = valid_data[i];
            auto ref = ref_func(valid);
            ASSERT_EQ(ans, ref);
        }
    }
}

TEST(Expr, PraseArrayContainsExpr) {
    // Test expressions for array_contains operations
    std::vector<const char*> exprs{
        // json_contains_expr: array_contains(array, 1)
        "array_contains(array, 1)",
        // json_contains_expr: array_contains_all(array, [1, 2, 3])
        "array_contains_all(array, [1, 2, 3])",
        // json_contains_expr: array_contains_any(array, [1, 2, 3])
        "array_contains_any(array, [1, 2, 3])",
    };

    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    schema->AddField(FieldName("array"),
                     FieldId(101),
                     DataType::ARRAY,
                     DataType::INT64,
                     false);
    ScopedSchemaHandle schema_handle(*schema);

    for (auto& expr : exprs) {
        auto plan_str = schema_handle.ParseSearch(
            expr, "fakevec", 10, "L2", R"({"nprobe": 10})", 3);
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
    }
}

template <typename T>
struct ArrayTestcase {
    std::vector<T> term;
    std::vector<std::string> nested_path;
};

TEST(Expr, TestArrayContains) {
    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto int_array_fid =
        schema->AddDebugField("int_array", DataType::ARRAY, DataType::INT8);
    auto long_array_fid =
        schema->AddDebugField("long_array", DataType::ARRAY, DataType::INT64);
    auto bool_array_fid =
        schema->AddDebugField("bool_array", DataType::ARRAY, DataType::BOOL);
    auto float_array_fid =
        schema->AddDebugField("float_array", DataType::ARRAY, DataType::FLOAT);
    auto double_array_fid = schema->AddDebugField(
        "double_array", DataType::ARRAY, DataType::DOUBLE);
    auto string_array_fid = schema->AddDebugField(
        "string_array", DataType::ARRAY, DataType::VARCHAR);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::map<std::string, std::vector<ScalarFieldProto>> array_cols;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_int_array_col =
            raw_data.get_col<ScalarFieldProto>(int_array_fid);
        auto new_long_array_col =
            raw_data.get_col<ScalarFieldProto>(long_array_fid);
        auto new_bool_array_col =
            raw_data.get_col<ScalarFieldProto>(bool_array_fid);
        auto new_float_array_col =
            raw_data.get_col<ScalarFieldProto>(float_array_fid);
        auto new_double_array_col =
            raw_data.get_col<ScalarFieldProto>(double_array_fid);
        auto new_string_array_col =
            raw_data.get_col<ScalarFieldProto>(string_array_fid);

        array_cols["int"].insert(array_cols["int"].end(),
                                 new_int_array_col.begin(),
                                 new_int_array_col.end());
        array_cols["long"].insert(array_cols["long"].end(),
                                  new_long_array_col.begin(),
                                  new_long_array_col.end());
        array_cols["bool"].insert(array_cols["bool"].end(),
                                  new_bool_array_col.begin(),
                                  new_bool_array_col.end());
        array_cols["float"].insert(array_cols["float"].end(),
                                   new_float_array_col.begin(),
                                   new_float_array_col.end());
        array_cols["double"].insert(array_cols["double"].end(),
                                    new_double_array_col.begin(),
                                    new_double_array_col.end());
        array_cols["string"].insert(array_cols["string"].end(),
                                    new_string_array_col.begin(),
                                    new_string_array_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());

    std::vector<ArrayTestcase<bool>> bool_testcases{{{true, true}, {}},
                                                    {{false, false}, {}}};

    for (auto testcase : bool_testcases) {
        auto check = [&](const std::vector<bool>& values) {
            for (auto const& e : testcase.term) {
                if (std::find(values.begin(), values.end(), e) !=
                    values.end()) {
                    return true;
                }
            }
            return false;
        };
        std::vector<proto::plan::GenericValue> values;
        for (const auto& val : testcase.term) {
            proto::plan::GenericValue gen_val;
            gen_val.set_bool_val(val);
            values.push_back(gen_val);
        }
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            expr::ColumnInfo(bool_array_fid, DataType::ARRAY, DataType::BOOL),
            proto::plan::JSONContainsExpr_JSONOp_Contains,
            true,
            values);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Array(array_cols["bool"][i]);
            std::vector<bool> res;
            for (int j = 0; j < array.length(); ++j) {
                res.push_back(array.get_data<bool>(j));
            }
            ASSERT_EQ(ans, check(res)) << "@" << i;
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res)) << "@" << i;
            }
        }
    }

    std::vector<ArrayTestcase<double>> double_testcases{
        {{1.123, 10.34}, {"double"}},
        {{10.34, 100.234}, {"double"}},
        {{100.234, 1000.4546}, {"double"}},
        {{1000.4546, 1.123}, {"double"}},
        {{1000.4546, 10.34}, {"double"}},
        {{1.123, 100.234}, {"double"}},
    };

    for (auto testcase : double_testcases) {
        auto check = [&](const std::vector<double>& values) {
            for (auto const& e : testcase.term) {
                if (std::find(values.begin(), values.end(), e) !=
                    values.end()) {
                    return true;
                }
            }
            return false;
        };

        std::vector<proto::plan::GenericValue> values;
        for (const auto& val : testcase.term) {
            proto::plan::GenericValue gen_val;
            gen_val.set_float_val(val);
            values.push_back(gen_val);
        }
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            expr::ColumnInfo(
                double_array_fid, DataType::ARRAY, DataType::DOUBLE),
            proto::plan::JSONContainsExpr_JSONOp_Contains,
            true,
            values);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Array(array_cols["double"][i]);
            std::vector<double> res;
            for (int j = 0; j < array.length(); ++j) {
                res.push_back(array.get_data<double>(j));
            }
            ASSERT_EQ(ans, check(res));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res));
            }
        }
    }

    for (auto testcase : double_testcases) {
        auto check = [&](const std::vector<float>& values) {
            for (auto const& e : testcase.term) {
                if (std::find(values.begin(), values.end(), e) !=
                    values.end()) {
                    return true;
                }
            }
            return false;
        };
        std::vector<proto::plan::GenericValue> values;
        for (const auto& val : testcase.term) {
            proto::plan::GenericValue gen_val;
            gen_val.set_float_val(val);
            values.push_back(gen_val);
        }
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            expr::ColumnInfo(float_array_fid, DataType::ARRAY, DataType::FLOAT),
            proto::plan::JSONContainsExpr_JSONOp_Contains,
            true,
            values);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Array(array_cols["float"][i]);
            std::vector<float> res;
            for (int j = 0; j < array.length(); ++j) {
                res.push_back(array.get_data<float>(j));
            }
            ASSERT_EQ(ans, check(res));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res));
            }
        }
    }

    std::vector<ArrayTestcase<int64_t>> testcases{
        {{1, 10}, {"int"}},
        {{10, 100}, {"int"}},
        {{100, 1000}, {"int"}},
        {{1000, 10}, {"int"}},
        {{2, 4, 6, 8, 10}, {"int"}},
        {{1, 2, 3, 4, 5}, {"int"}},
    };

    for (auto testcase : testcases) {
        auto check = [&](const std::vector<int64_t>& values) {
            for (auto const& e : testcase.term) {
                if (std::find(values.begin(), values.end(), e) ==
                    values.end()) {
                    return false;
                }
            }
            return true;
        };

        std::vector<proto::plan::GenericValue> values;
        for (const auto& val : testcase.term) {
            proto::plan::GenericValue gen_val;
            gen_val.set_int64_val(val);
            values.push_back(gen_val);
        }
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            expr::ColumnInfo(int_array_fid, DataType::ARRAY, DataType::INT8),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
            true,
            values);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Array(array_cols["int"][i]);
            std::vector<int64_t> res;
            for (int j = 0; j < array.length(); ++j) {
                res.push_back(array.get_data<int64_t>(j));
            }
            ASSERT_EQ(ans, check(res));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res));
            }
        }
    }

    for (auto testcase : testcases) {
        auto check = [&](const std::vector<int64_t>& values) {
            for (auto const& e : testcase.term) {
                if (std::find(values.begin(), values.end(), e) ==
                    values.end()) {
                    return false;
                }
            }
            return true;
        };

        std::vector<proto::plan::GenericValue> values;
        for (const auto& val : testcase.term) {
            proto::plan::GenericValue gen_val;
            gen_val.set_int64_val(val);
            values.push_back(gen_val);
        }
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            expr::ColumnInfo(long_array_fid, DataType::ARRAY, DataType::INT64),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
            true,
            values);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Array(array_cols["long"][i]);
            std::vector<int64_t> res;
            for (int j = 0; j < array.length(); ++j) {
                res.push_back(array.get_data<int64_t>(j));
            }
            ASSERT_EQ(ans, check(res));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res));
            }
        }
    }

    std::vector<ArrayTestcase<std::string>> testcases_string = {
        {{"1sads", "10dsf"}, {"string"}},
        {{"10dsf", "100"}, {"string"}},
        {{"100", "10dsf", "1sads"}, {"string"}},
        {{"100ddfdsssdfdsfsd0", "100"}, {"string"}},
    };

    for (auto testcase : testcases_string) {
        auto check = [&](const std::vector<std::string_view>& values) {
            for (auto const& e : testcase.term) {
                if (std::find(values.begin(), values.end(), e) ==
                    values.end()) {
                    return false;
                }
            }
            return true;
        };

        std::vector<proto::plan::GenericValue> values;
        for (const auto& val : testcase.term) {
            proto::plan::GenericValue gen_val;
            gen_val.set_string_val(val);
            values.push_back(gen_val);
        }
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            expr::ColumnInfo(
                string_array_fid, DataType::ARRAY, DataType::VARCHAR),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
            true,
            values);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Array(array_cols["string"][i]);
            std::vector<std::string_view> res;
            for (int j = 0; j < array.length(); ++j) {
                res.push_back(array.get_data<std::string_view>(j));
            }
            ASSERT_EQ(ans, check(res));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res));
            }
        }
    }
}

TEST(Expr, TestArrayContainsEmptyValues) {
    auto schema = std::make_shared<Schema>();
    auto int_array_fid =
        schema->AddDebugField("int_array", DataType::ARRAY, DataType::INT8);
    auto long_array_fid =
        schema->AddDebugField("long_array", DataType::ARRAY, DataType::INT64);
    auto bool_array_fid =
        schema->AddDebugField("bool_array", DataType::ARRAY, DataType::BOOL);
    auto float_array_fid =
        schema->AddDebugField("float_array", DataType::ARRAY, DataType::FLOAT);
    auto double_array_fid = schema->AddDebugField(
        "double_array", DataType::ARRAY, DataType::DOUBLE);
    auto string_array_fid = schema->AddDebugField(
        "string_array", DataType::ARRAY, DataType::VARCHAR);
    schema->set_primary_field_id(schema->AddDebugField("id", DataType::INT64));
    std::vector<FieldId> fields = {
        int_array_fid,
        long_array_fid,
        bool_array_fid,
        float_array_fid,
        double_array_fid,
        string_array_fid,
    };

    auto dummy_seg = CreateGrowingSegment(schema, empty_index_meta);

    int N = 1000;
    std::vector<int> age_col;
    int num_iters = 100;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        dummy_seg->PreInsert(N);
        dummy_seg->Insert(iter * N,
                          N,
                          raw_data.row_ids_.data(),
                          raw_data.timestamps_.data(),
                          raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(dummy_seg.get());
    std::vector<proto::plan::GenericValue> empty_values;

    for (auto field_id : fields) {
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            expr::ColumnInfo(field_id, DataType::ARRAY),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
            true,
            empty_values);

        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);
        for (int i = 0; i < N * num_iters; ++i) {
            ASSERT_EQ(final[i], false);
        }
    }
}

TEST(Expr, TestArrayBinaryArith) {
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto int_array_fid =
        schema->AddDebugField("int_array", DataType::ARRAY, DataType::INT8);
    auto long_array_fid =
        schema->AddDebugField("long_array", DataType::ARRAY, DataType::INT64);
    auto float_array_fid =
        schema->AddDebugField("float_array", DataType::ARRAY, DataType::FLOAT);
    auto double_array_fid = schema->AddDebugField(
        "double_array", DataType::ARRAY, DataType::DOUBLE);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::map<std::string, std::vector<ScalarFieldProto>> array_cols;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_int_array_col =
            raw_data.get_col<ScalarFieldProto>(int_array_fid);
        auto new_long_array_col =
            raw_data.get_col<ScalarFieldProto>(long_array_fid);
        auto new_float_array_col =
            raw_data.get_col<ScalarFieldProto>(float_array_fid);
        auto new_double_array_col =
            raw_data.get_col<ScalarFieldProto>(double_array_fid);

        array_cols["int"].insert(array_cols["int"].end(),
                                 new_int_array_col.begin(),
                                 new_int_array_col.end());
        array_cols["long"].insert(array_cols["long"].end(),
                                  new_long_array_col.begin(),
                                  new_long_array_col.end());
        array_cols["float"].insert(array_cols["float"].end(),
                                   new_float_array_col.begin(),
                                   new_float_array_col.end());
        array_cols["double"].insert(array_cols["double"].end(),
                                    new_double_array_col.begin(),
                                    new_double_array_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    ScopedSchemaHandle schema_handle(*schema);

    std::vector<std::tuple<std::string,
                           std::string,
                           std::function<bool(milvus::Array & array)>>>
        testcases = {
            // int_array[0] + 2 == 5
            {"int_array[0] + 2 == 5",
             "int",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val + 2 == 5;
             }},
            // int_array[0] + 2 != 5
            {"int_array[0] + 2 != 5",
             "int",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val + 2 != 5;
             }},
            // int_array[0] + 2 > 5
            {"int_array[0] + 2 > 5",
             "int",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val + 2 > 5;
             }},
            // int_array[0] + 2 >= 5
            {"int_array[0] + 2 >= 5",
             "int",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val + 2 >= 5;
             }},
            // int_array[0] + 2 < 5
            {"int_array[0] + 2 < 5",
             "int",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val + 2 < 5;
             }},
            // int_array[0] + 2 <= 5
            {"int_array[0] + 2 <= 5",
             "int",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val + 2 <= 5;
             }},
            // long_array[0] - 1 == 144
            {"long_array[0] - 1 == 144",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val - 1 == 144;
             }},
            // long_array[0] - 1 != 144
            {"long_array[0] - 1 != 144",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val - 1 != 144;
             }},
            // long_array[0] - 1 > 144
            {"long_array[0] - 1 > 144",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val - 1 > 144;
             }},
            // long_array[0] - 1 >= 144
            {"long_array[0] - 1 >= 144",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val - 1 >= 144;
             }},
            // long_array[0] - 1 < 144
            {"long_array[0] - 1 < 144",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val - 1 < 144;
             }},
            // long_array[0] - 1 <= 144
            {"long_array[0] - 1 <= 144",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val - 1 <= 144;
             }},
            // float_array[0] + 2.2 == 133.2
            {"float_array[0] + 2.2 == 133.2",
             "float",
             [](milvus::Array& array) {
                 auto val = array.get_data<double>(0);
                 return val + 2.2 == 133.2;
             }},
            // float_array[0] + 2.2 != 133.2
            {"float_array[0] + 2.2 != 133.2",
             "float",
             [](milvus::Array& array) {
                 auto val = array.get_data<double>(0);
                 return val + 2.2 != 133.2;
             }},
            // double_array[0] - 11.1 == 125.7
            {"double_array[0] - 11.1 == 125.7",
             "double",
             [](milvus::Array& array) {
                 auto val = array.get_data<double>(0);
                 return val - 11.1 == 125.7;
             }},
            // double_array[0] - 11.1 != 125.7
            {"double_array[0] - 11.1 != 125.7",
             "double",
             [](milvus::Array& array) {
                 auto val = array.get_data<double>(0);
                 return val - 11.1 != 125.7;
             }},
            // long_array[0] * 2 == 8
            {"long_array[0] * 2 == 8",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val * 2 == 8;
             }},
            // long_array[0] * 2 != 20
            {"long_array[0] * 2 != 20",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val * 2 != 20;
             }},
            // long_array[0] * 2 > 20
            {"long_array[0] * 2 > 20",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val * 2 > 20;
             }},
            // long_array[0] * 2 >= 20
            {"long_array[0] * 2 >= 20",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val * 2 >= 20;
             }},
            // long_array[0] * 2 < 20
            {"long_array[0] * 2 < 20",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val * 2 < 20;
             }},
            // long_array[0] * 2 <= 20
            {"long_array[0] * 2 <= 20",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val * 2 <= 20;
             }},
            // long_array[0] / 2 == 8
            {"long_array[0] / 2 == 8",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val / 2 == 8;
             }},
            // long_array[0] / 2 != 20
            {"long_array[0] / 2 != 20",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val / 2 != 20;
             }},
            // long_array[0] / 2 > 20
            {"long_array[0] / 2 > 20",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val / 2 > 20;
             }},
            // long_array[0] / 2 >= 20
            {"long_array[0] / 2 >= 20",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val / 2 >= 20;
             }},
            // long_array[0] / 2 < 20
            {"long_array[0] / 2 < 20",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val / 2 < 20;
             }},
            // long_array[0] / 2 <= 20
            {"long_array[0] / 2 <= 20",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val / 2 <= 20;
             }},
            // long_array[0] % 3 == 0
            {"long_array[0] % 3 == 0",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val % 3 == 0;
             }},
            // long_array[0] % 3 != 2
            {"long_array[0] % 3 != 2",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val % 3 != 2;
             }},
            // long_array[0] % 3 > 2
            {"long_array[0] % 3 > 2",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val % 3 > 2;
             }},
            // long_array[0] % 3 >= 2
            {"long_array[0] % 3 >= 2",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val % 3 >= 2;
             }},
            // long_array[0] % 3 < 2
            {"long_array[0] % 3 < 2",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val % 3 < 2;
             }},
            // long_array[0] % 3 <= 2
            {"long_array[0] % 3 <= 2",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val % 3 <= 2;
             }},
            // float_array[1024] + 2.2 == 133.2
            {"float_array[1024] + 2.2 == 133.2",
             "float",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<double>(1024);
                 return val + 2.2 == 133.2;
             }},
            // float_array[1024] + 2.2 != 133.2
            {"float_array[1024] + 2.2 != 133.2",
             "float",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<double>(1024);
                 return val + 2.2 != 133.2;
             }},
            // double_array[1024] - 11.1 == 125.7
            {"double_array[1024] - 11.1 == 125.7",
             "double",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<double>(1024);
                 return val - 11.1 == 125.7;
             }},
            // double_array[1024] - 11.1 != 125.7
            {"double_array[1024] - 11.1 != 125.7",
             "double",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<double>(1024);
                 return val - 11.1 != 125.7;
             }},
            // long_array[1024] * 2 == 8
            {"long_array[1024] * 2 == 8",
             "long",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<int64_t>(1024);
                 return val * 2 == 8;
             }},
            // long_array[1024] * 2 != 20
            {"long_array[1024] * 2 != 20",
             "long",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<int64_t>(1024);
                 return val * 2 != 20;
             }},
            // long_array[1024] / 2 == 8
            {"long_array[1024] / 2 == 8",
             "long",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<int64_t>(1024);
                 return val / 2 == 8;
             }},
            // long_array[1024] / 2 != 20
            {"long_array[1024] / 2 != 20",
             "long",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<int64_t>(1024);
                 return val / 2 != 20;
             }},
            // long_array[1024] % 3 == 0
            {"long_array[1024] % 3 == 0",
             "long",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<int64_t>(1024);
                 return val % 3 == 0;
             }},
            // long_array[1024] % 3 != 2
            {"long_array[1024] % 3 != 2",
             "long",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<int64_t>(1024);
                 return val % 3 != 2;
             }},
            // array_length(int_array) == 10
            {"array_length(int_array) == 10",
             "int",
             [](milvus::Array& array) { return array.length() == 10; }},
            // array_length(int_array) != 8
            {"array_length(int_array) != 8",
             "int",
             [](milvus::Array& array) { return array.length() != 8; }},
            // array_length(int_array) > 8
            {"array_length(int_array) > 8",
             "int",
             [](milvus::Array& array) { return array.length() > 8; }},
            // array_length(int_array) >= 8
            {"array_length(int_array) >= 8",
             "int",
             [](milvus::Array& array) { return array.length() >= 8; }},
            // array_length(int_array) < 8
            {"array_length(int_array) < 8",
             "int",
             [](milvus::Array& array) { return array.length() < 8; }},
            // array_length(int_array) <= 8
            {"array_length(int_array) <= 8",
             "int",
             [](milvus::Array& array) { return array.length() <= 8; }},
        };

    for (auto [expr, array_type, ref_func] : testcases) {
        auto plan_str = schema_handle.ParseSearch(
            expr, "fakevec", 10, "L2", R"({"nprobe": 10})", 3);
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Array(array_cols[array_type][i]);
            auto ref = ref_func(array);
            ASSERT_EQ(ans, ref);
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref);
            }
        }
    }
}

template <typename T>
struct UnaryRangeTestcase {
    milvus::OpType op_type;
    T value;
    std::vector<std::string> nested_path;
    std::function<bool(milvus::Array&)> check_func;
};

TEST(Expr, TestArrayStringMatch) {
    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto string_array_fid = schema->AddDebugField(
        "string_array", DataType::ARRAY, DataType::VARCHAR);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::map<std::string, std::vector<ScalarFieldProto>> array_cols;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_string_array_col =
            raw_data.get_col<ScalarFieldProto>(string_array_fid);
        array_cols["string"].insert(array_cols["string"].end(),
                                    new_string_array_col.begin(),
                                    new_string_array_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());

    std::vector<UnaryRangeTestcase<std::string>> prefix_testcases{
        {OpType::PrefixMatch,
         "abc",
         {"0"},
         [](milvus::Array& array) {
             return PrefixMatch(array.get_data<std::string_view>(0), "abc");
         }},
        {OpType::PrefixMatch,
         "def",
         {"1"},
         [](milvus::Array& array) {
             return PrefixMatch(array.get_data<std::string_view>(1), "def");
         }},
        {OpType::PrefixMatch,
         "def",
         {"1024"},
         [](milvus::Array& array) {
             if (array.length() <= 1024) {
                 return false;
             }
             return PrefixMatch(array.get_data<std::string_view>(1024), "def");
         }},
    };
    //vector_anns:<field_id:201 predicates:<unary_range_expr:<column_info:<field_id:131 data_type:Array nested_path:"0" element_type:VarChar > op:PrefixMatch value:<string_val:"abc" > > > query_info:<> placeholder_tag:"$0" >
    for (auto& testcase : prefix_testcases) {
        proto::plan::GenericValue value;
        value.set_string_val(testcase.value);
        auto expr = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
            milvus::expr::ColumnInfo(
                string_array_fid, DataType::ARRAY, testcase.nested_path),
            testcase.op_type,
            value,
            std::vector<proto::plan::GenericValue>{});
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Array(array_cols["string"][i]);
            ASSERT_EQ(ans, testcase.check_func(array));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], testcase.check_func(array));
            }
        }
    }
}

TEST(Expr, TestArrayInTerm) {
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto long_array_fid =
        schema->AddDebugField("long_array", DataType::ARRAY, DataType::INT64);
    auto bool_array_fid =
        schema->AddDebugField("bool_array", DataType::ARRAY, DataType::BOOL);
    auto float_array_fid =
        schema->AddDebugField("float_array", DataType::ARRAY, DataType::FLOAT);
    auto string_array_fid = schema->AddDebugField(
        "string_array", DataType::ARRAY, DataType::VARCHAR);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::map<std::string, std::vector<ScalarFieldProto>> array_cols;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_long_array_col =
            raw_data.get_col<ScalarFieldProto>(long_array_fid);
        auto new_bool_array_col =
            raw_data.get_col<ScalarFieldProto>(bool_array_fid);
        auto new_float_array_col =
            raw_data.get_col<ScalarFieldProto>(float_array_fid);
        auto new_string_array_col =
            raw_data.get_col<ScalarFieldProto>(string_array_fid);
        array_cols["long"].insert(array_cols["long"].end(),
                                  new_long_array_col.begin(),
                                  new_long_array_col.end());
        array_cols["bool"].insert(array_cols["bool"].end(),
                                  new_bool_array_col.begin(),
                                  new_bool_array_col.end());
        array_cols["float"].insert(array_cols["float"].end(),
                                   new_float_array_col.begin(),
                                   new_float_array_col.end());
        array_cols["string"].insert(array_cols["string"].end(),
                                    new_string_array_col.begin(),
                                    new_string_array_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    ScopedSchemaHandle schema_handle(*schema);

    std::vector<std::tuple<std::string,
                           std::string,
                           std::function<bool(milvus::Array & array)>>>
        testcases = {
            // term_expr: long_array[0] in [1, 2, 3]
            {"long_array[0] in [1, 2, 3]",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val == 1 || val == 2 || val == 3;
             }},
            // term_expr: long_array[0] in [] (empty list)
            {"long_array[0] in []",
             "long",
             [](milvus::Array& array) { return false; }},
            // term_expr: bool_array[0] in [false, false]
            {"bool_array[0] in [false, false]",
             "bool",
             [](milvus::Array& array) {
                 auto val = array.get_data<bool>(0);
                 return !val;
             }},
            // term_expr: bool_array[0] in [] (empty list)
            {"bool_array[0] in []",
             "bool",
             [](milvus::Array& array) { return false; }},
            // term_expr: float_array[0] in [1.23, 124.31]
            {"float_array[0] in [1.23, 124.31]",
             "float",
             [](milvus::Array& array) {
                 auto val = array.get_data<double>(0);
                 return val == 1.23 || val == 124.31;
             }},
            // term_expr: float_array[0] in [] (empty list)
            {"float_array[0] in []",
             "float",
             [](milvus::Array& array) { return false; }},
            // term_expr: string_array[0] in ["abc", "idhgf1s"]
            {R"(string_array[0] in ["abc", "idhgf1s"])",
             "string",
             [](milvus::Array& array) {
                 auto val = array.get_data<std::string_view>(0);
                 return val == "abc" || val == "idhgf1s";
             }},
            // term_expr: string_array[0] in [] (empty list)
            {R"(string_array[0] in [])",
             "string",
             [](milvus::Array& array) { return false; }},
            // term_expr: string_array[1024] in ["abc", "idhgf1s"]
            {R"(string_array[1024] in ["abc", "idhgf1s"])",
             "string",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<std::string_view>(1024);
                 return val == "abc" || val == "idhgf1s";
             }},
        };

    for (auto [expr, array_type, ref_func] : testcases) {
        auto plan_str = schema_handle.ParseSearch(
            expr, "fakevec", 10, "L2", R"({"nprobe": 10})", 3);
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Array(array_cols[array_type][i]);
            ASSERT_EQ(ans, ref_func(array));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref_func(array));
            }
        }
    }
}

TEST(Expr, TestTermInArray) {
    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto long_array_fid =
        schema->AddDebugField("long_array", DataType::ARRAY, DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::map<std::string, std::vector<ScalarFieldProto>> array_cols;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_long_array_col =
            raw_data.get_col<ScalarFieldProto>(long_array_fid);
        array_cols["long"].insert(array_cols["long"].end(),
                                  new_long_array_col.begin(),
                                  new_long_array_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());

    struct TermTestCases {
        std::vector<int64_t> values;
        std::vector<std::string> nested_path;
        std::function<bool(milvus::Array&)> check_func;
    };
    std::vector<TermTestCases> testcases = {
        {{100},
         {},
         [](milvus::Array& array) {
             for (int i = 0; i < array.length(); ++i) {
                 auto val = array.get_data<int64_t>(i);
                 if (val == 100) {
                     return true;
                 }
             }
             return false;
         }},
        {{1024},
         {},
         [](milvus::Array& array) {
             for (int i = 0; i < array.length(); ++i) {
                 auto val = array.get_data<int64_t>(i);
                 if (val == 1024) {
                     return true;
                 }
             }
             return false;
         }},
    };

    for (auto& testcase : testcases) {
        std::vector<proto::plan::GenericValue> values;
        for (auto& v : testcase.values) {
            proto::plan::GenericValue val;
            val.set_int64_val(v);
            values.emplace_back(val);
        }
        auto expr = std::make_shared<milvus::expr::TermFilterExpr>(
            milvus::expr::ColumnInfo(
                long_array_fid, DataType::ARRAY, testcase.nested_path),
            values,
            true);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Array(array_cols["long"][i]);
            ASSERT_EQ(ans, testcase.check_func(array));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], testcase.check_func(array));
            }
        }
    }
}
