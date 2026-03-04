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

#include <boost/format.hpp>
#include <boost/optional/optional.hpp>
#include <folly/FBVector.h>
#include <stddef.h>
#include <algorithm>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "ExprTestBase.h"
#include "NamedType/named_type_impl.hpp"
#include "bitset/bitset.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/IndexMeta.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "common/Vector.h"
#include "common/protobuf_utils.h"
#include "exec/expression/EvalCtx.h"
#include "exec/expression/function/FunctionFactory.h"
#include "expr/ITypeExpr.h"
#include "gtest/gtest.h"
#include "index/Index.h"
#include "index/ScalarIndexSort.h"
#include "knowhere/comp/index_param.h"
#include "pb/plan.pb.h"
#include "plan/PlanNode.h"
#include "query/ExecPlanNodeVisitor.h"
#include "query/Plan.h"
#include "query/PlanImpl.h"
#include "query/PlanNode.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/SegcoreConfig.h"
#include "segcore/SegmentGrowing.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/SegmentSealed.h"
#include "segcore/Types.h"
#include "test_utils/DataGen.h"
#include "test_utils/GenExprProto.h"
#include "test_utils/cachinglayer_test_utils.h"
#include "test_utils/storage_test_utils.h"

EXPR_TEST_INSTANTIATE();

TEST_P(ExprTest, TestCall) {
    milvus::exec::expression::FunctionFactory& factory =
        milvus::exec::expression::FunctionFactory::Instance();
    factory.Initialize();

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto varchar_fid = schema->AddDebugField("address", DataType::VARCHAR);
    schema->set_primary_field_id(varchar_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> address_col;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_address_col = raw_data.get_col<std::string>(varchar_fid);
        address_col.insert(
            address_col.end(), new_address_col.begin(), new_address_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    SetSchema(schema);

    std::tuple<std::string, std::function<bool(std::string&)>> test_cases[] = {
        {"empty(address)", [](std::string& v) { return v.empty(); }},
        {"starts_with(address, address)", [](std::string&) { return true; }}};

    for (auto& [expr_str, ref_func] : test_cases) {
        auto plan_str = create_search_plan_from_expr(expr_str);
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

            ASSERT_EQ(ans, ref_func(address_col[i]))
                << "@" << i << "!!" << address_col[i];
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref_func(address_col[i]))
                    << "@" << i << "!!" << address_col[i];
            }
        }
    }

    // Test incorrect function calls (wrong number of arguments)
    std::string incorrect_test_cases[] = {
        "empty(address, address)",  // empty() takes 1 arg, not 2
        "starts_with(address)"      // starts_with() takes 2 args, not 1
    };
    for (auto& expr_str : incorrect_test_cases) {
        EXPECT_ANY_THROW({
            auto plan_str = create_search_plan_from_expr(expr_str);
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        });
    }
}

TEST_P(ExprTest, TestCompare) {
    // Test cases: expression string and expected comparison function
    std::vector<std::tuple<std::string, std::function<bool(int, int64_t)>>>
        testcases = {
            {"age1 < age2", [](int a, int64_t b) { return a < b; }},
            {"age1 <= age2", [](int a, int64_t b) { return a <= b; }},
            {"age1 > age2", [](int a, int64_t b) { return a > b; }},
            {"age1 >= age2", [](int a, int64_t b) { return a >= b; }},
            {"age1 == age2", [](int a, int64_t b) { return a == b; }},
            {"age1 != age2", [](int a, int64_t b) { return a != b; }},
        };

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto i32_fid = schema->AddDebugField("age1", DataType::INT32);
    auto i64_fid = schema->AddDebugField("age2", DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<int> age1_col;
    std::vector<int64_t> age2_col;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_age1_col = raw_data.get_col<int>(i32_fid);
        auto new_age2_col = raw_data.get_col<int64_t>(i64_fid);
        age1_col.insert(
            age1_col.end(), new_age1_col.begin(), new_age1_col.end());
        age2_col.insert(
            age2_col.end(), new_age2_col.begin(), new_age2_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);
    SetSchema(schema);
    for (auto [expr_str, ref_func] : testcases) {
        auto plan_str = create_search_plan_from_expr(expr_str);
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

            auto val1 = age1_col[i];
            auto val2 = age2_col[i];
            auto ref = ref_func(val1, val2);
            ASSERT_EQ(ans, ref) << expr_str << "@" << i << "!!"
                                << boost::format("[%1%, %2%]") % val1 % val2;
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref)
                    << expr_str << "@" << i << "!!"
                    << boost::format("[%1%, %2%]") % val1 % val2;
            }
        }
    }
}

TEST_P(ExprTest, TestCompareNullable) {
    // Test cases: expression string and expected comparison function
    // Compares age1 (non-nullable INT32) with nullable (nullable INT64)
    std::vector<
        std::tuple<std::string, std::function<bool(int, int64_t, bool)>>>
        testcases = {
            {"age1 < nullable",
             [](int a, int64_t b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a < b;
             }},
            {"age1 <= nullable",
             [](int a, int64_t b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a <= b;
             }},
            {"age1 > nullable",
             [](int a, int64_t b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a > b;
             }},
            {"age1 >= nullable",
             [](int a, int64_t b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a >= b;
             }},
            {"age1 == nullable",
             [](int a, int64_t b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a == b;
             }},
            {"age1 != nullable",
             [](int a, int64_t b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a != b;
             }},
        };

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto i32_fid = schema->AddDebugField("age1", DataType::INT32);
    auto i64_fid = schema->AddDebugField("age2", DataType::INT64);
    auto nullable_fid =
        schema->AddDebugField("nullable", DataType::INT64, true);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<int> age1_col;
    std::vector<int64_t> nullable_col;
    FixedVector<bool> valid_data_col;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_age1_col = raw_data.get_col<int>(i32_fid);
        auto new_nullable_col = raw_data.get_col<int64_t>(nullable_fid);
        valid_data_col = raw_data.get_col_valid(nullable_fid);
        age1_col.insert(
            age1_col.end(), new_age1_col.begin(), new_age1_col.end());
        nullable_col.insert(nullable_col.end(),
                            new_nullable_col.begin(),
                            new_nullable_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);
    SetSchema(schema);
    for (auto [expr_str, ref_func] : testcases) {
        auto plan_str = create_search_plan_from_expr(expr_str);
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

            auto val1 = age1_col[i];
            auto val2 = nullable_col[i];
            auto ref = ref_func(val1, val2, valid_data_col[i]);
            ASSERT_EQ(ans, ref) << expr_str << "@" << i << "!!"
                                << boost::format("[%1%, %2%]") % val1 % val2;
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref)
                    << expr_str << "@" << i << "!!"
                    << boost::format("[%1%, %2%]") % val1 % val2;
            }
        }
    }
}

TEST_P(ExprTest, TestCompareNullable2) {
    // Test cases: expression string and expected comparison function
    // Compares nullable (nullable INT64) with age1 (non-nullable INT32) - swapped operand order
    std::vector<
        std::tuple<std::string, std::function<bool(int, int64_t, bool)>>>
        testcases = {
            {"nullable < age1",
             [](int a, int64_t b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a < b;
             }},
            {"nullable <= age1",
             [](int a, int64_t b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a <= b;
             }},
            {"nullable > age1",
             [](int a, int64_t b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a > b;
             }},
            {"nullable >= age1",
             [](int a, int64_t b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a >= b;
             }},
            {"nullable == age1",
             [](int a, int64_t b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a == b;
             }},
            {"nullable != age1",
             [](int a, int64_t b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a != b;
             }},
        };

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto i32_fid = schema->AddDebugField("age1", DataType::INT32);
    auto i64_fid = schema->AddDebugField("age2", DataType::INT64);
    auto nullable_fid =
        schema->AddDebugField("nullable", DataType::INT64, true);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<int> age1_col;
    std::vector<int64_t> nullable_col;
    FixedVector<bool> valid_data_col;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_age1_col = raw_data.get_col<int>(i32_fid);
        auto new_nullable_col = raw_data.get_col<int64_t>(nullable_fid);
        valid_data_col = raw_data.get_col_valid(nullable_fid);
        age1_col.insert(
            age1_col.end(), new_age1_col.begin(), new_age1_col.end());
        nullable_col.insert(nullable_col.end(),
                            new_nullable_col.begin(),
                            new_nullable_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);
    SetSchema(schema);
    for (auto [expr_str, ref_func] : testcases) {
        auto plan_str = create_search_plan_from_expr(expr_str);
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

            auto val2 = age1_col[i];
            auto val1 = nullable_col[i];
            auto ref = ref_func(val1, val2, valid_data_col[i]);
            ASSERT_EQ(ans, ref) << expr_str << "@" << i << "!!"
                                << boost::format("[%1%, %2%]") % val1 % val2;
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref)
                    << expr_str << "@" << i << "!!"
                    << boost::format("[%1%, %2%]") % val1 % val2;
            }
        }
    }
}

TEST_P(ExprTest, TestCompareWithScalarIndex) {
    std::vector<std::tuple<std::string, std::function<bool(int, int64_t)>>>
        testcases = {
            {"age32 < age64", [](int a, int64_t b) { return a < b; }},
            {"age32 <= age64", [](int a, int64_t b) { return a <= b; }},
            {"age32 > age64", [](int a, int64_t b) { return a > b; }},
            {"age32 >= age64", [](int a, int64_t b) { return a >= b; }},
            {"age32 == age64", [](int a, int64_t b) { return a == b; }},
            {"age32 != age64", [](int a, int64_t b) { return a != b; }},
        };

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto i32_fid = schema->AddDebugField("age32", DataType::INT32);
    auto i64_fid = schema->AddDebugField("age64", DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateSealedSegment(schema);
    int N = 1000;
    auto raw_data = DataGen(schema, N);
    segcore::LoadIndexInfo load_index_info;

    // load index for int32 field
    auto age32_col = raw_data.get_col<int32_t>(i32_fid);
    age32_col[0] = 1000;
    auto age32_index = milvus::index::CreateScalarIndexSort<int32_t>();
    age32_index->Build(N, age32_col.data());
    load_index_info.field_id = i32_fid.get();
    load_index_info.field_type = DataType::INT32;
    load_index_info.index_params = GenIndexParams(age32_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(age32_index));
    seg->LoadIndex(load_index_info);

    // load index for int64 field
    auto age64_col = raw_data.get_col<int64_t>(i64_fid);
    age64_col[0] = 2000;
    auto age64_index = milvus::index::CreateScalarIndexSort<int64_t>();
    age64_index->Build(N, age64_col.data());
    load_index_info.field_id = i64_fid.get();
    load_index_info.field_type = DataType::INT64;
    load_index_info.index_params = GenIndexParams(age64_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(age64_index));
    seg->LoadIndex(load_index_info);

    query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);
    SetSchema(schema);
    for (auto [expr_str, ref_func] : testcases) {
        auto plan_str = create_search_plan_from_expr(expr_str);
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg.get(),
            N,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N / 2);
        for (auto i = 0; i < N; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg.get(),
            N,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N / 2);

        for (int i = 0; i < N; ++i) {
            auto ans = final[i];
            auto val1 = age32_col[i];
            auto val2 = age64_col[i];
            auto ref = ref_func(val1, val2);
            ASSERT_EQ(ans, ref) << expr_str << "@" << i << "!!"
                                << boost::format("[%1%, %2%]") % val1 % val2;
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref)
                    << expr_str << "@" << i << "!!"
                    << boost::format("[%1%, %2%]") % val1 % val2;
            }
        }
    }
}

TEST_P(ExprTest, TestCompareWithScalarIndexNullable) {
    std::vector<
        std::tuple<std::string, std::function<bool(int, int64_t, bool)>>>
        testcases = {
            {"nullable < age64",
             [](int a, int64_t b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a < b;
             }},
            {"nullable <= age64",
             [](int a, int64_t b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a <= b;
             }},
            {"nullable > age64",
             [](int a, int64_t b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a > b;
             }},
            {"nullable >= age64",
             [](int a, int64_t b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a >= b;
             }},
            {"nullable == age64",
             [](int a, int64_t b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a == b;
             }},
            {"nullable != age64",
             [](int a, int64_t b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a != b;
             }},
        };

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto nullable_fid =
        schema->AddDebugField("nullable", DataType::INT32, true);
    auto i64_fid = schema->AddDebugField("age64", DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateSealedSegment(schema);
    int N = 1000;
    auto raw_data = DataGen(schema, N);
    segcore::LoadIndexInfo load_index_info;

    // load index for int32 field
    auto nullable_col = raw_data.get_col<int32_t>(nullable_fid);
    nullable_col[0] = 1000;
    auto valid_data_col = raw_data.get_col_valid(nullable_fid);
    auto nullable_index = milvus::index::CreateScalarIndexSort<int32_t>();
    nullable_index->Build(N, nullable_col.data(), valid_data_col.data());
    load_index_info.field_id = nullable_fid.get();
    load_index_info.field_type = DataType::INT32;
    load_index_info.index_params = GenIndexParams(nullable_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(nullable_index));
    seg->LoadIndex(load_index_info);

    // load index for int64 field
    auto age64_col = raw_data.get_col<int64_t>(i64_fid);
    age64_col[0] = 2000;
    auto age64_index = milvus::index::CreateScalarIndexSort<int64_t>();
    age64_index->Build(N, age64_col.data());
    load_index_info.field_id = i64_fid.get();
    load_index_info.field_type = DataType::INT64;
    load_index_info.index_params = GenIndexParams(age64_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(age64_index));
    seg->LoadIndex(load_index_info);

    query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);
    SetSchema(schema);
    for (auto [expr_str, ref_func] : testcases) {
        auto plan_str = create_search_plan_from_expr(expr_str);
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg.get(),
            N,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N / 2);
        for (auto i = 0; i < N; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg.get(),
            N,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N / 2);

        for (int i = 0; i < N; ++i) {
            auto ans = final[i];
            auto val1 = nullable_col[i];
            auto val2 = age64_col[i];
            auto ref = ref_func(val1, val2, valid_data_col[i]);
            ASSERT_EQ(ans, ref) << expr_str << "@" << i << "!!"
                                << boost::format("[%1%, %2%]") % val1 % val2;
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref)
                    << expr_str << "@" << i << "!!"
                    << boost::format("[%1%, %2%]") % val1 % val2;
            }
        }
    }
}

TEST_P(ExprTest, TestCompareWithScalarIndexNullable2) {
    std::vector<
        std::tuple<std::string, std::function<bool(int, int64_t, bool)>>>
        testcases = {
            {"age64 < nullable",
             [](int a, int64_t b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a < b;
             }},
            {"age64 <= nullable",
             [](int a, int64_t b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a <= b;
             }},
            {"age64 > nullable",
             [](int a, int64_t b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a > b;
             }},
            {"age64 >= nullable",
             [](int a, int64_t b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a >= b;
             }},
            {"age64 == nullable",
             [](int a, int64_t b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a == b;
             }},
            {"age64 != nullable",
             [](int a, int64_t b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a != b;
             }},
        };

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto nullable_fid =
        schema->AddDebugField("nullable", DataType::INT32, true);
    auto i64_fid = schema->AddDebugField("age64", DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateSealedSegment(schema);
    int N = 1000;
    auto raw_data = DataGen(schema, N);
    segcore::LoadIndexInfo load_index_info;

    // load index for int32 field
    auto nullable_col = raw_data.get_col<int32_t>(nullable_fid);
    nullable_col[0] = 1000;
    auto valid_data_col = raw_data.get_col_valid(nullable_fid);
    auto nullable_index = milvus::index::CreateScalarIndexSort<int32_t>();
    nullable_index->Build(N, nullable_col.data(), valid_data_col.data());
    load_index_info.field_id = nullable_fid.get();
    load_index_info.field_type = DataType::INT32;
    load_index_info.index_params = GenIndexParams(nullable_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(nullable_index));
    seg->LoadIndex(load_index_info);

    // load index for int64 field
    auto age64_col = raw_data.get_col<int64_t>(i64_fid);
    age64_col[0] = 2000;
    auto age64_index = milvus::index::CreateScalarIndexSort<int64_t>();
    age64_index->Build(N, age64_col.data());
    load_index_info.field_id = i64_fid.get();
    load_index_info.field_type = DataType::INT64;
    load_index_info.index_params = GenIndexParams(age64_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(age64_index));
    seg->LoadIndex(load_index_info);

    query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);
    SetSchema(schema);
    for (auto [expr_str, ref_func] : testcases) {
        auto plan_str = create_search_plan_from_expr(expr_str);
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg.get(),
            N,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N / 2);
        for (auto i = 0; i < N; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg.get(),
            N,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N / 2);

        for (int i = 0; i < N; ++i) {
            auto ans = final[i];
            auto val2 = nullable_col[i];
            auto val1 = age64_col[i];
            auto ref = ref_func(val1, val2, valid_data_col[i]);
            ASSERT_EQ(ans, ref) << expr_str << "@" << i << "!!"
                                << boost::format("[%1%, %2%]") % val1 % val2;
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref)
                    << expr_str << "@" << i << "!!"
                    << boost::format("[%1%, %2%]") % val1 % val2;
            }
        }
    }
}

TEST_P(ExprTest, test_term_pk_with_sorted) {
    auto schema = std::make_shared<Schema>();
    schema->AddField(FieldName("Timestamp"),
                     FieldId(1),
                     DataType::INT64,
                     false,
                     std::nullopt);
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    schema->set_primary_field_id(int64_fid);

    auto seg = CreateSealedSegment(
        schema, nullptr, 1, SegcoreConfig::default_config(), true);
    int N = 100000;
    auto raw_data = DataGen(schema, N);
    LoadGeneratedDataIntoSegment(raw_data, seg.get(), true);

    std::vector<proto::plan::GenericValue> retrieve_ints;
    for (int i = 0; i < 10; ++i) {
        proto::plan::GenericValue val;
        val.set_int64_val(i);
        retrieve_ints.push_back(val);
    }
    auto expr = std::make_shared<expr::TermFilterExpr>(
        expr::ColumnInfo(int64_fid, DataType::INT64), retrieve_ints);
    query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);
    BitsetType final;
    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    EXPECT_EQ(final.size(), N);
    for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(final[i], true);
    }
    for (int i = 10; i < N; ++i) {
        EXPECT_EQ(final[i], false);
    }
    retrieve_ints.clear();
    for (int i = 0; i < 10; ++i) {
        proto::plan::GenericValue val;
        val.set_int64_val(i + N);
        retrieve_ints.push_back(val);
    }
    expr = std::make_shared<expr::TermFilterExpr>(
        expr::ColumnInfo(int64_fid, DataType::INT64), retrieve_ints);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    EXPECT_EQ(final.size(), N);

    // specify some offsets and do scalar filtering on these offsets
    milvus::exec::OffsetVector offsets;
    offsets.reserve(N / 2);
    for (auto i = 0; i < N; ++i) {
        if (i % 2 == 0) {
            offsets.emplace_back(i);
        }
    }
    auto col_vec = milvus::test::gen_filter_res(
        plan.get(), seg.get(), N, MAX_TIMESTAMP, &offsets);
    BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
    EXPECT_EQ(view.size(), N / 2);

    for (int i = 0; i < N; ++i) {
        EXPECT_EQ(final[i], false);
        if (i % 2 == 0) {
            EXPECT_EQ(view[int(i / 2)], false);
        }
    }
}

TEST_P(ExprTest, TestSealedSegmentGetBatchSize) {
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto bool_fid = schema->AddDebugField("bool", DataType::BOOL);
    auto bool_1_fid = schema->AddDebugField("bool1", DataType::BOOL);
    auto int8_fid = schema->AddDebugField("int8", DataType::INT8);
    auto int8_1_fid = schema->AddDebugField("int81", DataType::INT8);
    auto int16_fid = schema->AddDebugField("int16", DataType::INT16);
    auto int16_1_fid = schema->AddDebugField("int161", DataType::INT16);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32);
    auto int32_1_fid = schema->AddDebugField("int321", DataType::INT32);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    auto int64_1_fid = schema->AddDebugField("int641", DataType::INT64);
    auto float_fid = schema->AddDebugField("float", DataType::FLOAT);
    auto float_1_fid = schema->AddDebugField("float1", DataType::FLOAT);
    auto double_fid = schema->AddDebugField("double", DataType::DOUBLE);
    auto double_1_fid = schema->AddDebugField("double1", DataType::DOUBLE);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto str2_fid = schema->AddDebugField("string2", DataType::VARCHAR);
    auto str3_fid = schema->AddDebugField("string3", DataType::VARCHAR);
    schema->set_primary_field_id(str1_fid);

    auto seg = CreateSealedSegment(schema);
    size_t N = 1000;
    auto raw_data = DataGen(schema, N);
    LoadGeneratedDataIntoSegment(raw_data, seg.get(), true);

    query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);
    auto build_expr = [&](enum DataType type) -> expr::TypedExprPtr {
        switch (type) {
            case DataType::BOOL: {
                auto compare_expr = std::make_shared<expr::CompareExpr>(
                    bool_fid,
                    bool_1_fid,
                    DataType::BOOL,
                    DataType::BOOL,
                    proto::plan::OpType::LessThan);
                return compare_expr;
            }
            case DataType::INT8: {
                auto compare_expr =
                    std::make_shared<expr::CompareExpr>(int8_fid,
                                                        int8_1_fid,
                                                        DataType::INT8,
                                                        DataType::INT8,
                                                        OpType::LessThan);
                return compare_expr;
            }
            case DataType::INT16: {
                auto compare_expr =
                    std::make_shared<expr::CompareExpr>(int16_fid,
                                                        int16_1_fid,
                                                        DataType::INT16,
                                                        DataType::INT16,
                                                        OpType::LessThan);
                return compare_expr;
            }
            case DataType::INT32: {
                auto compare_expr =
                    std::make_shared<expr::CompareExpr>(int32_fid,
                                                        int32_1_fid,
                                                        DataType::INT32,
                                                        DataType::INT32,
                                                        OpType::LessThan);
                return compare_expr;
            }
            case DataType::INT64: {
                auto compare_expr =
                    std::make_shared<expr::CompareExpr>(int64_fid,
                                                        int64_1_fid,
                                                        DataType::INT64,
                                                        DataType::INT64,
                                                        OpType::LessThan);
                return compare_expr;
            }
            case DataType::FLOAT: {
                auto compare_expr =
                    std::make_shared<expr::CompareExpr>(float_fid,
                                                        float_1_fid,
                                                        DataType::FLOAT,
                                                        DataType::FLOAT,
                                                        OpType::LessThan);
                return compare_expr;
            }
            case DataType::DOUBLE: {
                auto compare_expr =
                    std::make_shared<expr::CompareExpr>(double_fid,
                                                        double_1_fid,
                                                        DataType::DOUBLE,
                                                        DataType::DOUBLE,
                                                        OpType::LessThan);
                return compare_expr;
            }
            case DataType::VARCHAR: {
                auto compare_expr =
                    std::make_shared<expr::CompareExpr>(str2_fid,
                                                        str3_fid,
                                                        DataType::VARCHAR,
                                                        DataType::VARCHAR,
                                                        OpType::LessThan);
                return compare_expr;
            }
            default:
                return std::make_shared<expr::CompareExpr>(int8_fid,
                                                           int8_1_fid,
                                                           DataType::INT8,
                                                           DataType::INT8,
                                                           OpType::LessThan);
        }
    };
    auto expr = build_expr(DataType::BOOL);
    BitsetType final;
    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    expr = build_expr(DataType::INT8);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    expr = build_expr(DataType::INT16);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    expr = build_expr(DataType::INT32);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    expr = build_expr(DataType::INT64);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    expr = build_expr(DataType::FLOAT);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    expr = build_expr(DataType::DOUBLE);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
}

TEST_P(ExprTest, TestReorder) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("id", DataType::INT64);
    auto bool_fid = schema->AddDebugField("bool", DataType::BOOL);
    auto bool_1_fid = schema->AddDebugField("bool1", DataType::BOOL);
    auto int8_fid = schema->AddDebugField("int8", DataType::INT8);
    auto int8_1_fid = schema->AddDebugField("int81", DataType::INT8);
    auto int16_fid = schema->AddDebugField("int16", DataType::INT16);
    auto int16_1_fid = schema->AddDebugField("int161", DataType::INT16);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32);
    auto int32_1_fid = schema->AddDebugField("int321", DataType::INT32);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    auto int64_1_fid = schema->AddDebugField("int641", DataType::INT64);
    auto float_fid = schema->AddDebugField("float", DataType::FLOAT);
    auto float_1_fid = schema->AddDebugField("float1", DataType::FLOAT);
    auto double_fid = schema->AddDebugField("double", DataType::DOUBLE);
    auto double_1_fid = schema->AddDebugField("double1", DataType::DOUBLE);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto str2_fid = schema->AddDebugField("string2", DataType::VARCHAR);
    auto json_fid = schema->AddDebugField("json", DataType::JSON, false);
    auto str_array_fid =
        schema->AddDebugField("str_array", DataType::ARRAY, DataType::VARCHAR);
    schema->set_primary_field_id(pk);

    auto seg = CreateSealedSegment(schema);
    size_t N = 1000;
    auto raw_data = DataGen(schema, N);
    LoadGeneratedDataIntoSegment(raw_data, seg.get(), true);

    query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);

    auto build_expr = [&](int index) -> expr::TypedExprPtr {
        switch (index) {
            case 0: {
                proto::plan::GenericValue val1;
                val1.set_string_val("xxx");
                auto expr1 = std::make_shared<expr::UnaryRangeFilterExpr>(
                    expr::ColumnInfo(str1_fid, DataType::VARCHAR),
                    proto::plan::OpType::Equal,
                    val1,
                    std::vector<proto::plan::GenericValue>{});
                proto::plan::GenericValue val2;
                val2.set_int64_val(100);
                auto expr2 = std::make_shared<expr::UnaryRangeFilterExpr>(
                    expr::ColumnInfo(int64_fid, DataType::INT64),
                    proto::plan::OpType::LessThan,
                    val2,
                    std::vector<proto::plan::GenericValue>{});
                auto expr3 = std::make_shared<expr::LogicalBinaryExpr>(
                    expr::LogicalBinaryExpr::OpType::And, expr1, expr2);
                return expr3;
            };
            case 1: {
                proto::plan::GenericValue val1;
                val1.set_string_val("xxx");
                auto expr1 = std::make_shared<expr::UnaryRangeFilterExpr>(
                    expr::ColumnInfo(json_fid, DataType::JSON, {"int"}),
                    proto::plan::OpType::Equal,
                    val1,
                    std::vector<proto::plan::GenericValue>{});
                proto::plan::GenericValue val2;
                val2.set_int64_val(100);
                auto expr2 = std::make_shared<expr::UnaryRangeFilterExpr>(
                    expr::ColumnInfo(int64_fid, DataType::INT64),
                    proto::plan::OpType::LessThan,
                    val2,
                    std::vector<proto::plan::GenericValue>{});
                auto expr3 = std::make_shared<expr::LogicalBinaryExpr>(
                    expr::LogicalBinaryExpr::OpType::And, expr1, expr2);
                return expr3;
            };
            case 2: {
                proto::plan::GenericValue val1;
                val1.set_string_val("12");
                auto expr1 = std::make_shared<expr::UnaryRangeFilterExpr>(
                    expr::ColumnInfo(str_array_fid, DataType::ARRAY, {"0"}),
                    proto::plan::OpType::Match,
                    val1,
                    std::vector<proto::plan::GenericValue>{});
                proto::plan::GenericValue val2;
                val2.set_int64_val(100);
                auto expr2 = std::make_shared<expr::UnaryRangeFilterExpr>(
                    expr::ColumnInfo(int64_fid, DataType::INT64),
                    proto::plan::OpType::LessThan,
                    val2,
                    std::vector<proto::plan::GenericValue>{});
                auto expr3 = std::make_shared<expr::LogicalBinaryExpr>(
                    expr::LogicalBinaryExpr::OpType::And, expr1, expr2);
                return expr3;
            };
            case 3: {
                auto expr1 =
                    std::make_shared<expr::CompareExpr>(int64_fid,
                                                        int64_1_fid,
                                                        DataType::INT64,
                                                        DataType::INT64,
                                                        OpType::LessThan);
                proto::plan::GenericValue val2;
                val2.set_int64_val(100);
                auto expr2 = std::make_shared<expr::UnaryRangeFilterExpr>(
                    expr::ColumnInfo(int64_fid, DataType::INT64),
                    proto::plan::OpType::LessThan,
                    val2,
                    std::vector<proto::plan::GenericValue>{});
                auto expr3 = std::make_shared<expr::LogicalBinaryExpr>(
                    expr::LogicalBinaryExpr::OpType::And, expr1, expr2);
                return expr3;
            };
            default:
                ThrowInfo(ErrorCode::UnexpectedError, "not implement");
        }
    };
    BitsetType final;
    auto expr = build_expr(0);
    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    expr = build_expr(1);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    expr = build_expr(2);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    expr = build_expr(3);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
}

TEST_P(ExprTest, TestCompareExprNullable) {
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto bool_fid = schema->AddDebugField("bool", DataType::BOOL);
    auto bool_nullable_fid =
        schema->AddDebugField("bool1", DataType::BOOL, true);
    auto int8_fid = schema->AddDebugField("int8", DataType::INT8);
    auto int8_nullable_fid =
        schema->AddDebugField("int81", DataType::INT8, true);
    auto int16_fid = schema->AddDebugField("int16", DataType::INT16);
    auto int16_nullable_fid =
        schema->AddDebugField("int161", DataType::INT16, true);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32);
    auto int32_nullable_fid =
        schema->AddDebugField("int321", DataType::INT32, true);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    auto int64_nullable_fid =
        schema->AddDebugField("int641", DataType::INT64, true);
    auto float_fid = schema->AddDebugField("float", DataType::FLOAT);
    auto float_nullable_fid =
        schema->AddDebugField("float1", DataType::FLOAT, true);
    auto double_fid = schema->AddDebugField("double", DataType::DOUBLE);
    auto double_nullable_fid =
        schema->AddDebugField("double1", DataType::DOUBLE, true);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto str2_fid = schema->AddDebugField("string2", DataType::VARCHAR);
    auto str_nullable_fid =
        schema->AddDebugField("string3", DataType::VARCHAR, true);
    schema->set_primary_field_id(str1_fid);

    auto seg = CreateSealedSegment(schema);
    size_t N = 1000;
    auto raw_data = DataGen(schema, N);
    LoadGeneratedDataIntoSegment(raw_data, seg.get(), true);

    query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);
    auto build_expr = [&](enum DataType type) -> expr::TypedExprPtr {
        switch (type) {
            case DataType::BOOL: {
                auto compare_expr = std::make_shared<expr::CompareExpr>(
                    bool_fid,
                    bool_nullable_fid,
                    DataType::BOOL,
                    DataType::BOOL,
                    proto::plan::OpType::LessThan);
                return compare_expr;
            }
            case DataType::INT8: {
                auto compare_expr =
                    std::make_shared<expr::CompareExpr>(int8_fid,
                                                        int8_nullable_fid,
                                                        DataType::INT8,
                                                        DataType::INT8,
                                                        OpType::LessThan);
                return compare_expr;
            }
            case DataType::INT16: {
                auto compare_expr =
                    std::make_shared<expr::CompareExpr>(int16_fid,
                                                        int16_nullable_fid,
                                                        DataType::INT16,
                                                        DataType::INT16,
                                                        OpType::LessThan);
                return compare_expr;
            }
            case DataType::INT32: {
                auto compare_expr =
                    std::make_shared<expr::CompareExpr>(int32_fid,
                                                        int32_nullable_fid,
                                                        DataType::INT32,
                                                        DataType::INT32,
                                                        OpType::LessThan);
                return compare_expr;
            }
            case DataType::INT64: {
                auto compare_expr =
                    std::make_shared<expr::CompareExpr>(int64_fid,
                                                        int64_nullable_fid,
                                                        DataType::INT64,
                                                        DataType::INT64,
                                                        OpType::LessThan);
                return compare_expr;
            }
            case DataType::FLOAT: {
                auto compare_expr =
                    std::make_shared<expr::CompareExpr>(float_fid,
                                                        float_nullable_fid,
                                                        DataType::FLOAT,
                                                        DataType::FLOAT,
                                                        OpType::LessThan);
                return compare_expr;
            }
            case DataType::DOUBLE: {
                auto compare_expr =
                    std::make_shared<expr::CompareExpr>(double_fid,
                                                        double_nullable_fid,
                                                        DataType::DOUBLE,
                                                        DataType::DOUBLE,
                                                        OpType::LessThan);
                return compare_expr;
            }
            case DataType::VARCHAR: {
                auto compare_expr =
                    std::make_shared<expr::CompareExpr>(str2_fid,
                                                        str_nullable_fid,
                                                        DataType::VARCHAR,
                                                        DataType::VARCHAR,
                                                        OpType::LessThan);
                return compare_expr;
            }
            default:
                return std::make_shared<expr::CompareExpr>(int8_fid,
                                                           int8_nullable_fid,
                                                           DataType::INT8,
                                                           DataType::INT8,
                                                           OpType::LessThan);
        }
    };
    auto expr = build_expr(DataType::BOOL);
    BitsetType final;
    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    expr = build_expr(DataType::INT8);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    expr = build_expr(DataType::INT16);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    expr = build_expr(DataType::INT32);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    expr = build_expr(DataType::INT64);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    expr = build_expr(DataType::FLOAT);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    expr = build_expr(DataType::DOUBLE);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
}

TEST_P(ExprTest, TestCompareExprNullable2) {
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto bool_fid = schema->AddDebugField("bool", DataType::BOOL);
    auto bool_nullable_fid =
        schema->AddDebugField("bool1", DataType::BOOL, true);
    auto int8_fid = schema->AddDebugField("int8", DataType::INT8);
    auto int8_nullable_fid =
        schema->AddDebugField("int81", DataType::INT8, true);
    auto int16_fid = schema->AddDebugField("int16", DataType::INT16);
    auto int16_nullable_fid =
        schema->AddDebugField("int161", DataType::INT16, true);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32);
    auto int32_nullable_fid =
        schema->AddDebugField("int321", DataType::INT32, true);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    auto int64_nullable_fid =
        schema->AddDebugField("int641", DataType::INT64, true);
    auto float_fid = schema->AddDebugField("float", DataType::FLOAT);
    auto float_nullable_fid =
        schema->AddDebugField("float1", DataType::FLOAT, true);
    auto double_fid = schema->AddDebugField("double", DataType::DOUBLE);
    auto double_nullable_fid =
        schema->AddDebugField("double1", DataType::DOUBLE, true);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto str2_fid = schema->AddDebugField("string2", DataType::VARCHAR);
    auto str_nullable_fid =
        schema->AddDebugField("string3", DataType::VARCHAR, true);
    schema->set_primary_field_id(str1_fid);

    auto seg = CreateSealedSegment(schema);
    size_t N = 1000;
    auto raw_data = DataGen(schema, N);
    LoadGeneratedDataIntoSegment(raw_data, seg.get(), true);

    query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);
    auto build_expr = [&](enum DataType type) -> expr::TypedExprPtr {
        switch (type) {
            case DataType::BOOL: {
                auto compare_expr = std::make_shared<expr::CompareExpr>(
                    bool_fid,
                    bool_nullable_fid,
                    DataType::BOOL,
                    DataType::BOOL,
                    proto::plan::OpType::LessThan);
                return compare_expr;
            }
            case DataType::INT8: {
                auto compare_expr =
                    std::make_shared<expr::CompareExpr>(int8_nullable_fid,
                                                        int8_fid,
                                                        DataType::INT8,
                                                        DataType::INT8,
                                                        OpType::LessThan);
                return compare_expr;
            }
            case DataType::INT16: {
                auto compare_expr =
                    std::make_shared<expr::CompareExpr>(int16_nullable_fid,
                                                        int16_fid,
                                                        DataType::INT16,
                                                        DataType::INT16,
                                                        OpType::LessThan);
                return compare_expr;
            }
            case DataType::INT32: {
                auto compare_expr =
                    std::make_shared<expr::CompareExpr>(int32_nullable_fid,
                                                        int32_fid,
                                                        DataType::INT32,
                                                        DataType::INT32,
                                                        OpType::LessThan);
                return compare_expr;
            }
            case DataType::INT64: {
                auto compare_expr =
                    std::make_shared<expr::CompareExpr>(int64_nullable_fid,
                                                        int64_fid,
                                                        DataType::INT64,
                                                        DataType::INT64,
                                                        OpType::LessThan);
                return compare_expr;
            }
            case DataType::FLOAT: {
                auto compare_expr =
                    std::make_shared<expr::CompareExpr>(float_nullable_fid,
                                                        float_fid,
                                                        DataType::FLOAT,
                                                        DataType::FLOAT,
                                                        OpType::LessThan);
                return compare_expr;
            }
            case DataType::DOUBLE: {
                auto compare_expr =
                    std::make_shared<expr::CompareExpr>(double_nullable_fid,
                                                        double_fid,
                                                        DataType::DOUBLE,
                                                        DataType::DOUBLE,
                                                        OpType::LessThan);
                return compare_expr;
            }
            case DataType::VARCHAR: {
                auto compare_expr =
                    std::make_shared<expr::CompareExpr>(str_nullable_fid,
                                                        str2_fid,
                                                        DataType::VARCHAR,
                                                        DataType::VARCHAR,
                                                        OpType::LessThan);
                return compare_expr;
            }
            default:
                return std::make_shared<expr::CompareExpr>(int8_nullable_fid,
                                                           int8_fid,
                                                           DataType::INT8,
                                                           DataType::INT8,
                                                           OpType::LessThan);
        }
    };
    auto expr = build_expr(DataType::BOOL);
    BitsetType final;
    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    expr = build_expr(DataType::INT8);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    expr = build_expr(DataType::INT16);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    expr = build_expr(DataType::INT32);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    expr = build_expr(DataType::INT64);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    expr = build_expr(DataType::FLOAT);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    expr = build_expr(DataType::DOUBLE);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
}
