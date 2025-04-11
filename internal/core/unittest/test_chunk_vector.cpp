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

#include <gtest/gtest.h>
#include <stdio.h>

#include "common/Types.h"
#include "knowhere/comp/index_param.h"
#include "segcore/SegmentGrowing.h"
#include "segcore/SegmentGrowingImpl.h"
#include "pb/schema.pb.h"
#include "test_utils/DataGen.h"
#include "query/Plan.h"

using namespace milvus::segcore;
using namespace milvus;
namespace pb = milvus::proto;
class ChunkVectorTest : public ::testing::TestWithParam<bool> {
 public:
    void
    SetUp() override {
        auto& mmap_config =
            milvus::storage::MmapManager::GetInstance().GetMmapConfig();
        mmap_config.SetEnableGrowingMmap(true);
    }
    void
    TearDown() override {
        auto& mmap_config =
            milvus::storage::MmapManager::GetInstance().GetMmapConfig();
        mmap_config.SetEnableGrowingMmap(false);
    }
    knowhere::MetricType metric_type = "IP";
    milvus::segcore::SegcoreConfig config;
};

TEST_F(ChunkVectorTest, FillDataWithMmap) {
    auto schema = std::make_shared<Schema>();
    auto bool_field = schema->AddDebugField("bool", DataType::BOOL);
    auto int8_field = schema->AddDebugField("int8", DataType::INT8);
    auto int16_field = schema->AddDebugField("int16", DataType::INT16);
    auto int32_field = schema->AddDebugField("int32", DataType::INT32);
    auto int64_field = schema->AddDebugField("int64", DataType::INT64);
    auto float_field = schema->AddDebugField("float", DataType::FLOAT);
    auto double_field = schema->AddDebugField("double", DataType::DOUBLE);
    auto varchar_field = schema->AddDebugField("varchar", DataType::VARCHAR);
    auto json_field = schema->AddDebugField("json", DataType::JSON);
    auto int_array_field =
        schema->AddDebugField("int_array", DataType::ARRAY, DataType::INT8);
    auto long_array_field =
        schema->AddDebugField("long_array", DataType::ARRAY, DataType::INT64);
    auto bool_array_field =
        schema->AddDebugField("bool_array", DataType::ARRAY, DataType::BOOL);
    auto string_array_field = schema->AddDebugField(
        "string_array", DataType::ARRAY, DataType::VARCHAR);
    auto double_array_field = schema->AddDebugField(
        "double_array", DataType::ARRAY, DataType::DOUBLE);
    auto float_array_field =
        schema->AddDebugField("float_array", DataType::ARRAY, DataType::FLOAT);
    auto fp32_vec = schema->AddDebugField(
        "fp32_vec", DataType::VECTOR_FLOAT, 128, metric_type);
    auto fp16_vec = schema->AddDebugField(
        "fp16_vec", DataType::VECTOR_FLOAT16, 128, metric_type);
    auto bf16_vec = schema->AddDebugField(
        "bf16_vec", DataType::VECTOR_BFLOAT16, 128, metric_type);
    auto sparse_vec = schema->AddDebugField(
        "sparse_vec", DataType::VECTOR_SPARSE_FLOAT, 128, metric_type);
    auto int8_vec = schema->AddDebugField(
        "int8_vec", DataType::VECTOR_INT8, 128, metric_type);
    schema->set_primary_field_id(int64_field);

    std::map<std::string, std::string> index_params = {
        {"index_type", "HNSW"}, {"metric_type", metric_type}, {"nlist", "128"}};
    std::map<std::string, std::string> type_params = {{"dim", "128"}};
    FieldIndexMeta fieldIndexMeta(
        fp32_vec, std::move(index_params), std::move(type_params));

    std::map<FieldId, FieldIndexMeta> filedMap = {{fp32_vec, fieldIndexMeta}};
    IndexMetaPtr metaPtr =
        std::make_shared<CollectionIndexMeta>(100000, std::move(filedMap));
    auto segment_growing = CreateGrowingSegment(schema, metaPtr, 1, config);
    auto segment = dynamic_cast<SegmentGrowingImpl*>(segment_growing.get());
    int64_t per_batch = 1000;
    int64_t n_batch = 3;
    int64_t dim = 128;
    for (int64_t i = 0; i < n_batch; i++) {
        auto dataset = DataGen(schema, per_batch);

        auto offset = segment->PreInsert(per_batch);
        segment->Insert(offset,
                        per_batch,
                        dataset.row_ids_.data(),
                        dataset.timestamps_.data(),
                        dataset.raw_);
        auto num_inserted = (i + 1) * per_batch;
        auto ids_ds = GenRandomIds(num_inserted);
        auto bool_result =
            segment->bulk_subscript(bool_field, ids_ds->GetIds(), num_inserted);
        auto int8_result =
            segment->bulk_subscript(int8_field, ids_ds->GetIds(), num_inserted);
        auto int16_result = segment->bulk_subscript(
            int16_field, ids_ds->GetIds(), num_inserted);
        auto int32_result = segment->bulk_subscript(
            int32_field, ids_ds->GetIds(), num_inserted);
        auto int64_result = segment->bulk_subscript(
            int64_field, ids_ds->GetIds(), num_inserted);
        auto float_result = segment->bulk_subscript(
            float_field, ids_ds->GetIds(), num_inserted);
        auto double_result = segment->bulk_subscript(
            double_field, ids_ds->GetIds(), num_inserted);
        auto varchar_result = segment->bulk_subscript(
            varchar_field, ids_ds->GetIds(), num_inserted);
        auto json_result =
            segment->bulk_subscript(json_field, ids_ds->GetIds(), num_inserted);
        auto int_array_result = segment->bulk_subscript(
            int_array_field, ids_ds->GetIds(), num_inserted);
        auto long_array_result = segment->bulk_subscript(
            long_array_field, ids_ds->GetIds(), num_inserted);
        auto bool_array_result = segment->bulk_subscript(
            bool_array_field, ids_ds->GetIds(), num_inserted);
        auto string_array_result = segment->bulk_subscript(
            string_array_field, ids_ds->GetIds(), num_inserted);
        auto double_array_result = segment->bulk_subscript(
            double_array_field, ids_ds->GetIds(), num_inserted);
        auto float_array_result = segment->bulk_subscript(
            float_array_field, ids_ds->GetIds(), num_inserted);
        auto fp32_vec_result =
            segment->bulk_subscript(fp32_vec, ids_ds->GetIds(), num_inserted);
        auto fp16_vec_result =
            segment->bulk_subscript(fp16_vec, ids_ds->GetIds(), num_inserted);
        auto bf16_vec_result =
            segment->bulk_subscript(bf16_vec, ids_ds->GetIds(), num_inserted);
        auto sparse_vec_result =
            segment->bulk_subscript(sparse_vec, ids_ds->GetIds(), num_inserted);
        auto int8_vec_result =
            segment->bulk_subscript(int8_vec, ids_ds->GetIds(), num_inserted);

        EXPECT_EQ(bool_result->scalars().bool_data().data_size(), num_inserted);
        EXPECT_EQ(int8_result->scalars().int_data().data_size(), num_inserted);
        EXPECT_EQ(int16_result->scalars().int_data().data_size(), num_inserted);
        EXPECT_EQ(int32_result->scalars().int_data().data_size(), num_inserted);
        EXPECT_EQ(int64_result->scalars().long_data().data_size(),
                  num_inserted);
        EXPECT_EQ(float_result->scalars().float_data().data_size(),
                  num_inserted);
        EXPECT_EQ(double_result->scalars().double_data().data_size(),
                  num_inserted);
        EXPECT_EQ(varchar_result->scalars().string_data().data_size(),
                  num_inserted);
        EXPECT_EQ(json_result->scalars().json_data().data_size(), num_inserted);
        EXPECT_EQ(fp32_vec_result->vectors().float_vector().data_size(),
                  num_inserted * dim);
        EXPECT_EQ(fp16_vec_result->vectors().float16_vector().size(),
                  num_inserted * dim * 2);
        EXPECT_EQ(bf16_vec_result->vectors().bfloat16_vector().size(),
                  num_inserted * dim * 2);
        EXPECT_EQ(
            sparse_vec_result->vectors().sparse_float_vector().contents_size(),
            num_inserted);
        EXPECT_EQ(int8_vec_result->vectors().int8_vector().size(),
                  num_inserted * dim);
        EXPECT_EQ(int_array_result->scalars().array_data().data_size(),
                  num_inserted);
        EXPECT_EQ(long_array_result->scalars().array_data().data_size(),
                  num_inserted);
        EXPECT_EQ(bool_array_result->scalars().array_data().data_size(),
                  num_inserted);
        EXPECT_EQ(string_array_result->scalars().array_data().data_size(),
                  num_inserted);
        EXPECT_EQ(double_array_result->scalars().array_data().data_size(),
                  num_inserted);
        EXPECT_EQ(float_array_result->scalars().array_data().data_size(),
                  num_inserted);
        // checking dense/sparse vector
        auto fp32_vec_res =
            fp32_vec_result.get()->mutable_vectors()->float_vector().data();
        auto fp16_vec_res = (float16*)fp16_vec_result.get()
                                ->mutable_vectors()
                                ->float16_vector()
                                .data();
        auto bf16_vec_res = (bfloat16*)bf16_vec_result.get()
                                ->mutable_vectors()
                                ->bfloat16_vector()
                                .data();
        auto sparse_vec_res = SparseBytesToRows(
            sparse_vec_result->vectors().sparse_float_vector().contents());
        auto int8_vec_res = (int8*)int8_vec_result.get()
                                ->mutable_vectors()
                                ->int8_vector()
                                .data();
        EXPECT_TRUE(fp32_vec_res.size() == num_inserted * dim);
        auto fp32_vec_gt = dataset.get_col<float>(fp32_vec);
        auto fp16_vec_gt = dataset.get_col<float16>(fp16_vec);
        auto bf16_vec_gt = dataset.get_col<bfloat16>(bf16_vec);
        auto sparse_vec_gt =
            dataset.get_col<knowhere::sparse::SparseRow<float>>(sparse_vec);
        auto int8_vec_gt = dataset.get_col<int8>(int8_vec);

        for (size_t i = 0; i < num_inserted; ++i) {
            auto id = ids_ds->GetIds()[i];
            // check dense vector
            EXPECT_TRUE(memcmp((void*)(&fp32_vec_res[i * dim]),
                               (void*)(&fp32_vec_gt[(id % per_batch) * dim]),
                               sizeof(float) * dim) == 0);
            EXPECT_TRUE(memcmp((void*)(&fp16_vec_res[i * dim]),
                               (void*)(&fp16_vec_gt[(id % per_batch) * dim]),
                               sizeof(float16) * dim) == 0);
            EXPECT_TRUE(memcmp((void*)(&bf16_vec_res[i * dim]),
                               (void*)(&bf16_vec_gt[(id % per_batch) * dim]),
                               sizeof(bfloat16) * dim) == 0);
            EXPECT_TRUE(memcmp((void*)(&int8_vec_res[i * dim]),
                               (void*)(&int8_vec_gt[(id % per_batch) * dim]),
                               sizeof(int8) * dim) == 0);
            //check sparse vector
            auto actual_row = sparse_vec_res[i];
            auto expected_row = sparse_vec_gt[(id % per_batch)];
            EXPECT_TRUE(actual_row.size() == expected_row.size());
            for (size_t j = 0; j < actual_row.size(); ++j) {
                EXPECT_TRUE(actual_row[j].id == expected_row[j].id);
                EXPECT_TRUE(actual_row[j].val == expected_row[j].val);
            }
        }
    }
}

INSTANTIATE_TEST_SUITE_P(IsSparse, ChunkVectorTest, ::testing::Bool());
TEST_P(ChunkVectorTest, SearchWithMmap) {
    auto is_sparse = GetParam();
    auto data_type =
        is_sparse ? DataType::VECTOR_SPARSE_FLOAT : DataType::VECTOR_FLOAT;
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    auto random = schema->AddDebugField("random", DataType::DOUBLE);
    auto vec = schema->AddDebugField("embeddings", data_type, 128, metric_type);
    schema->set_primary_field_id(pk);

    auto segment = CreateGrowingSegment(schema, empty_index_meta, 11, config);
    auto segmentImplPtr = dynamic_cast<SegmentGrowingImpl*>(segment.get());

    milvus::proto::plan::PlanNode plan_node;
    auto vector_anns = plan_node.mutable_vector_anns();
    if (is_sparse) {
        vector_anns->set_vector_type(
            milvus::proto::plan::VectorType::SparseFloatVector);
    } else {
        vector_anns->set_vector_type(
            milvus::proto::plan::VectorType::FloatVector);
    }
    vector_anns->set_placeholder_tag("$0");
    vector_anns->set_field_id(102);
    auto query_info = vector_anns->mutable_query_info();
    query_info->set_topk(5);
    query_info->set_round_decimal(3);
    query_info->set_metric_type(metric_type);
    query_info->set_search_params(R"({"nprobe": 16})");
    auto plan_str = plan_node.SerializeAsString();

    int64_t per_batch = 10000;
    int64_t n_batch = 3;
    int64_t top_k = 5;
    for (int64_t i = 0; i < n_batch; i++) {
        auto dataset = DataGen(schema, per_batch);
        auto offset = segment->PreInsert(per_batch);
        auto pks = dataset.get_col<int64_t>(pk);
        segment->Insert(offset,
                        per_batch,
                        dataset.row_ids_.data(),
                        dataset.timestamps_.data(),
                        dataset.raw_);
        const VectorBase* field_data = nullptr;
        if (is_sparse) {
            field_data = segmentImplPtr->get_insert_record()
                             .get_data<milvus::SparseFloatVector>(vec);
        } else {
            field_data = segmentImplPtr->get_insert_record()
                             .get_data<milvus::FloatVector>(vec);
        }
        auto inserted = (i + 1) * per_batch;

        auto num_queries = 5;
        auto ph_group_raw =
            is_sparse ? CreateSparseFloatPlaceholderGroup(num_queries)
                      : CreatePlaceholderGroup(num_queries, 128, 1024);

        auto plan = milvus::query::CreateSearchPlanByExpr(
            *schema, plan_str.data(), plan_str.size());
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
        milvus::Timestamp timestamp = 1000000;
        auto sr = segment->Search(plan.get(), ph_group.get(), timestamp);
        EXPECT_EQ(sr->total_nq_, num_queries);
        EXPECT_EQ(sr->unity_topK_, top_k);
        EXPECT_EQ(sr->distances_.size(), num_queries * top_k);
        EXPECT_EQ(sr->seg_offsets_.size(), num_queries * top_k);
        for (auto i = 0; i < num_queries; i++) {
            for (auto k = 0; k < top_k; k++) {
                EXPECT_NE(sr->seg_offsets_.data()[i * top_k + k], -1);
                EXPECT_FALSE(std::isnan(sr->distances_.data()[i * top_k + k]));
            }
        }
    }
}
TEST_F(ChunkVectorTest, QueryWithMmap) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    schema->AddDebugField("age", DataType::FLOAT);
    auto i64_fid = schema->AddDebugField("counter", DataType::INT64);
    schema->set_primary_field_id(i64_fid);
    const char* raw_plan = R"(vector_anns: <
                                    field_id: 100
                                    predicates: <
                                      term_expr: <
                                        column_info: <
                                          field_id: 102
                                          data_type: Int64
                                        >
                                        values: <
                                          int64_val: 1
                                        >
                                        values: <
                                          int64_val: 2
                                        >
                                      >
                                    >
                                    query_info: <
                                      topk: 5
                                      round_decimal: 3
                                      metric_type: "L2"
                                      search_params: "{\"nprobe\": 10}"
                                    >
                                    placeholder_tag: "$0"
     >)";
    int64_t N = 4000;
    auto dataset = DataGen(schema, N);
    auto segment = CreateGrowingSegment(schema, empty_index_meta, 11, config);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
    auto plan = milvus::query::CreateSearchPlanByExpr(
        *schema, plan_str.data(), plan_str.size());
    auto num_queries = 3;
    auto ph_group_raw =
        milvus::segcore::CreatePlaceholderGroup(num_queries, 16, 1024);
    auto ph_group = milvus::query::ParsePlaceholderGroup(
        plan.get(), ph_group_raw.SerializeAsString());
    milvus::Timestamp timestamp = 1000000;

    auto sr = segment->Search(plan.get(), ph_group.get(), timestamp);
    int topk = 5;
    auto json = SearchResultToJson(*sr);
    ASSERT_EQ(sr->total_nq_, num_queries);
    ASSERT_EQ(sr->unity_topK_, topk);
}

// TEST_F(ChunkVectorTest, ArrayExprWithMmap) {
//     auto schema = std::make_shared<Schema>();
//     auto i64_fid = schema->AddDebugField("id", DataType::INT64);
//     auto long_array_fid =
//         schema->AddDebugField("long_array", DataType::ARRAY, DataType::INT64);
//     auto bool_array_fid =
//         schema->AddDebugField("bool_array", DataType::ARRAY, DataType::BOOL);
//     auto float_array_fid =
//         schema->AddDebugField("float_array", DataType::ARRAY, DataType::FLOAT);
//     auto string_array_fid = schema->AddDebugField(
//         "string_array", DataType::ARRAY, DataType::VARCHAR);
//     schema->set_primary_field_id(i64_fid);

//     auto seg = CreateGrowingSegment(schema, empty_index_meta, 22, config);
//     int N = 1000;
//     std::map<std::string, std::vector<ScalarArray>> array_cols;
//     int num_iters = 1;
//     for (int iter = 0; iter < num_iters; ++iter) {
//         auto raw_data = DataGen(schema, N, iter);
//         auto new_long_array_col = raw_data.get_col<ScalarArray>(long_array_fid);
//         auto new_bool_array_col = raw_data.get_col<ScalarArray>(bool_array_fid);
//         auto new_float_array_col =
//             raw_data.get_col<ScalarArray>(float_array_fid);
//         auto new_string_array_col =
//             raw_data.get_col<ScalarArray>(string_array_fid);
//         array_cols["long"].insert(array_cols["long"].end(),
//                                   new_long_array_col.begin(),
//                                   new_long_array_col.end());
//         array_cols["bool"].insert(array_cols["bool"].end(),
//                                   new_bool_array_col.begin(),
//                                   new_bool_array_col.end());
//         array_cols["float"].insert(array_cols["float"].end(),
//                                    new_float_array_col.begin(),
//                                    new_float_array_col.end());
//         array_cols["string"].insert(array_cols["string"].end(),
//                                     new_string_array_col.begin(),
//                                     new_string_array_col.end());
//         seg->PreInsert(N);
//         seg->Insert(iter * N,
//                     N,
//                     raw_data.row_ids_.data(),
//                     raw_data.timestamps_.data(),
//                     raw_data.raw_);
//     }

//     auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
//     query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);

//     std::vector<std::tuple<std::string,
//                            std::string,
//                            std::function<bool(milvus::Array & array)>>>
//         testcases = {
//             {R"(term_expr: <
//               column_info: <
//                 field_id: 101
//                 data_type: Array
//                 nested_path:"0"
//                 element_type:Int64
//               >
//               values:<int64_val:1 > values:<int64_val:2 > values:<int64_val:3 >
//         >)",
//              "long",
//              [](milvus::Array& array) {
//                  auto val = array.get_data<int64_t>(0);
//                  return val == 1 || val == 2 || val == 3;
//              }},
//             {R"(term_expr: <
//               column_info: <
//                 field_id: 101
//                 data_type: Array
//                 nested_path:"0"
//                 element_type:Int64
//               >
//         >)",
//              "long",
//              [](milvus::Array& array) { return false; }},
//             {R"(term_expr: <
//               column_info: <
//                 field_id: 102
//                 data_type: Array
//                 nested_path:"0"
//                 element_type:Bool
//               >
//                 values:<bool_val:false > values:<bool_val:false >
//         >)",
//              "bool",
//              [](milvus::Array& array) {
//                  auto val = array.get_data<bool>(0);
//                  return !val;
//              }},
//             {R"(term_expr: <
//               column_info: <
//                 field_id: 102
//                 data_type: Array
//                 nested_path:"0"
//                 element_type:Bool
//               >
//         >)",
//              "bool",
//              [](milvus::Array& array) { return false; }},
//             {R"(term_expr: <
//               column_info: <
//                 field_id: 103
//                 data_type: Array
//                 nested_path:"0"
//                 element_type:Float
//               >
//                 values:<float_val:1.23 > values:<float_val:124.31 >
//         >)",
//              "float",
//              [](milvus::Array& array) {
//                  auto val = array.get_data<double>(0);
//                  return val == 1.23 || val == 124.31;
//              }},
//             {R"(term_expr: <
//               column_info: <
//                 field_id: 103
//                 data_type: Array
//                 nested_path:"0"
//                 element_type:Float
//               >
//         >)",
//              "float",
//              [](milvus::Array& array) { return false; }},
//             {R"(term_expr: <
//               column_info: <
//                 field_id: 104
//                 data_type: Array
//                 nested_path:"0"
//                 element_type:VarChar
//               >
//                 values:<string_val:"abc" > values:<string_val:"idhgf1s" >
//         >)",
//              "string",
//              [](milvus::Array& array) {
//                  auto val = array.get_data<std::string_view>(0);
//                  return val == "abc" || val == "idhgf1s";
//              }},
//             {R"(term_expr: <
//               column_info: <
//                 field_id: 104
//                 data_type: Array
//                 nested_path:"0"
//                 element_type:VarChar
//               >
//         >)",
//              "string",
//              [](milvus::Array& array) { return false; }},
//             {R"(term_expr: <
//               column_info: <
//                 field_id: 104
//                 data_type: Array
//                 nested_path:"1024"
//                 element_type:VarChar
//               >
//                 values:<string_val:"abc" > values:<string_val:"idhgf1s" >
//         >)",
//              "string",
//              [](milvus::Array& array) {
//                  if (array.length() <= 1024) {
//                      return false;
//                  }
//                  auto val = array.get_data<std::string_view>(1024);
//                  return val == "abc" || val == "idhgf1s";
//              }},
//         };

//     std::string raw_plan_tmp = R"(vector_anns: <
//                                     field_id: 100
//                                     predicates: <
//                                       @@@@
//                                     >
//                                     query_info: <
//                                       topk: 10
//                                       round_decimal: 3
//                                       metric_type: "L2"
//                                       search_params: "{\"nprobe\": 10}"
//                                     >
//                                     placeholder_tag: "$0"
//      >)";

//     for (auto [clause, array_type, ref_func] : testcases) {
//         auto loc = raw_plan_tmp.find("@@@@");
//         auto raw_plan = raw_plan_tmp;
//         raw_plan.replace(loc, 4, clause);
//         auto plan_str = translate_text_plan_to_binary_plan(raw_plan.c_str());
//         auto plan =
//             milvus::query::CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
//         BitsetType final;
//         visitor.ExecuteExprNode(plan->plan_node_->filter_plannode_.value(),
//                                 seg_promote,
//                                 N * num_iters,
//                                 final);
//         EXPECT_EQ(final.size(), N * num_iters);

//         for (int i = 0; i < N * num_iters; ++i) {
//             auto ans = final[i];
//             auto array = milvus::Array(array_cols[array_type][i]);
//             ASSERT_EQ(ans, ref_func(array));
//         }
//     }
// }
