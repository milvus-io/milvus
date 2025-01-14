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

#include "common/LoadInfo.h"
#include "common/Types.h"
#include "index/IndexFactory.h"
#include "knowhere/comp/index_param.h"
#include "segcore/reduce/Reduce.h"
#include "segcore/reduce_c.h"
#include "test_utils/DataGen.h"
#include "test_utils/PbHelper.h"
#include "test_utils/indexbuilder_test_utils.h"

#include "pb/schema.pb.h"
#include "pb/plan.pb.h"
#include "query/Plan.h"
#include "query/Utils.h"
#include "query/PlanImpl.h"
#include "query/PlanNode.h"
#include "query/PlanProto.h"
#include "query/SearchBruteForce.h"
#include "query/ExecPlanNodeVisitor.h"
#include "segcore/Collection.h"
#include "segcore/SegmentSealed.h"
#include "segcore/SegmentGrowing.h"
#include "segcore/SegmentGrowingImpl.h"
#include "test_utils/AssertUtils.h"
#include "test_utils/DataGen.h"
#include "test_utils/GenExprProto.h"

using namespace milvus;
using namespace milvus::index;
using namespace milvus::query;
using namespace milvus::segcore;
using namespace knowhere;

using milvus::index::VectorIndex;
using milvus::segcore::LoadIndexInfo;

const int64_t ROW_COUNT = 100 * 1000;

// TEST(Float16, Insert) {
//     int64_t N = ROW_COUNT;
//     constexpr int64_t size_per_chunk = 32 * 1024;
//     auto schema = std::make_shared<Schema>();
//     auto float16_vec_fid = schema->AddDebugField(
//         "float16vec", DataType::VECTOR_FLOAT16, 32, knowhere::metric::L2);
//     auto i64_fid = schema->AddDebugField("counter", DataType::INT64);
//     schema->set_primary_field_id(i64_fid);

//     auto dataset = DataGen(schema, N);
//     // auto seg_conf = SegcoreConfig::default_config();
//     auto segment = CreateGrowingSegment(schema, empty_index_meta);
//     segment->PreInsert(N);
//     segment->Insert(0,
//                     N,
//                     dataset.row_ids_.data(),
//                     dataset.timestamps_.data(),
//                     dataset.raw_);
//     auto float16_ptr = dataset.get_col<float16>(float16_vec_fid);
//     SegmentInternalInterface& interface = *segment;
//     auto num_chunk = interface.num_chunk();
//     ASSERT_EQ(num_chunk, upper_div(N, size_per_chunk));
//     auto row_count = interface.get_row_count();
//     ASSERT_EQ(N, row_count);
//     for (auto chunk_id = 0; chunk_id < num_chunk; ++chunk_id) {
//         auto float16_span = interface.chunk_data<milvus::Float16Vector>(
//             float16_vec_fid, chunk_id);
//         auto begin = chunk_id * size_per_chunk;
//         auto end = std::min((chunk_id + 1) * size_per_chunk, N);
//         auto size_of_chunk = end - begin;
//         for (int i = 0; i < size_of_chunk; ++i) {
//             // std::cout << float16_span.data()[i] << " " << float16_ptr[i + begin * 32] << std::endl;
//             ASSERT_EQ(float16_span.data()[i], float16_ptr[i + begin * 32]);
//         }
//     }
// }

TEST(Float16, ExecWithoutPredicateFlat) {
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT16, 32, knowhere::metric::L2);
    schema->AddDebugField("age", DataType::FLOAT);
    auto i64_fid = schema->AddDebugField("counter", DataType::INT64);
    schema->set_primary_field_id(i64_fid);
    const char* raw_plan = R"(vector_anns: <
                                    field_id: 100
                                    query_info: <
                                      topk: 5
                                      round_decimal: 3
                                      metric_type: "L2"
                                      search_params: "{\"nprobe\": 10}"
                                    >
                                    placeholder_tag: "$0"
        >)";
    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
    auto plan =
        CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
    int64_t N = ROW_COUNT;
    auto dataset = DataGen(schema, N);
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);
    auto vec_ptr = dataset.get_col<float16>(vec_fid);

    auto num_queries = 5;
    auto ph_group_raw =
        CreatePlaceholderGroup<milvus::Float16Vector>(num_queries, 32, 1024);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
    Timestamp timestamp = 1000000;
    auto sr = segment->Search(plan.get(), ph_group.get(), timestamp);
    std::vector<std::vector<std::string>> results;
    auto json = SearchResultToJson(*sr);
    std::cout << json.dump(2);
}

TEST(Float16, GetVector) {
    auto metricType = knowhere::metric::L2;
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    auto random = schema->AddDebugField("random", DataType::DOUBLE);
    auto vec = schema->AddDebugField(
        "embeddings", DataType::VECTOR_FLOAT16, 128, metricType);
    schema->set_primary_field_id(pk);
    std::map<std::string, std::string> index_params = {
        {"index_type", "IVF_FLAT"},
        {"metric_type", metricType},
        {"nlist", "128"}};
    std::map<std::string, std::string> type_params = {{"dim", "128"}};
    FieldIndexMeta fieldIndexMeta(
        vec, std::move(index_params), std::move(type_params));
    auto config = SegcoreConfig::default_config();
    config.set_chunk_rows(1024);
    config.set_enable_interim_segment_index(true);
    std::map<FieldId, FieldIndexMeta> filedMap = {{vec, fieldIndexMeta}};
    IndexMetaPtr metaPtr =
        std::make_shared<CollectionIndexMeta>(100000, std::move(filedMap));
    auto segment_growing = CreateGrowingSegment(schema, metaPtr, 1, config);
    auto segment = dynamic_cast<SegmentGrowingImpl*>(segment_growing.get());

    int64_t per_batch = 5000;
    int64_t n_batch = 20;
    int64_t dim = 128;
    for (int64_t i = 0; i < n_batch; i++) {
        auto dataset = DataGen(schema, per_batch);
        auto fakevec = dataset.get_col<float16>(vec);
        auto offset = segment->PreInsert(per_batch);
        segment->Insert(offset,
                        per_batch,
                        dataset.row_ids_.data(),
                        dataset.timestamps_.data(),
                        dataset.raw_);
        auto num_inserted = (i + 1) * per_batch;
        auto ids_ds = GenRandomIds(num_inserted);
        auto result =
            segment->bulk_subscript(vec, ids_ds->GetIds(), num_inserted);

        auto vector = result.get()->mutable_vectors()->float16_vector();
        EXPECT_TRUE(vector.size() == num_inserted * dim * sizeof(float16));
        for (size_t i = 0; i < num_inserted; ++i) {
            auto id = ids_ds->GetIds()[i];
            for (size_t j = 0; j < 128; ++j) {
                EXPECT_TRUE(
                    reinterpret_cast<float16*>(vector.data())[i * dim + j] ==
                    fakevec[(id % per_batch) * dim + j]);
            }
        }
    }
}

TEST(Float16, RetrieveEmpty) {
    auto schema = std::make_shared<Schema>();
    auto fid_64 = schema->AddDebugField("i64", DataType::INT64);
    auto DIM = 16;
    auto fid_vec = schema->AddDebugField(
        "vector_64", DataType::VECTOR_FLOAT16, DIM, knowhere::metric::L2);
    schema->set_primary_field_id(fid_64);

    int64_t N = 100;
    int64_t req_size = 10;
    auto choose = [=](int i) { return i * 3 % N; };

    auto segment = CreateSealedSegment(schema);

    auto plan = std::make_unique<query::RetrievePlan>(*schema);
    std::vector<proto::plan::GenericValue> values;
    {
        for (int i = 0; i < req_size; ++i) {
            proto::plan::GenericValue val;
            val.set_int64_val(choose(i));
            values.push_back(val);
        }
    }
    auto term_expr = std::make_shared<milvus::expr::TermFilterExpr>(
        milvus::expr::ColumnInfo(
            fid_64, DataType::INT64, std::vector<std::string>()),
        values);
    plan->plan_node_ = std::make_unique<query::RetrievePlanNode>();
    plan->plan_node_->plannodes_ =
        milvus::test::CreateRetrievePlanByExpr(term_expr);
    std::vector<FieldId> target_offsets{fid_64, fid_vec};
    plan->field_ids_ = target_offsets;

    auto retrieve_results = segment->Retrieve(
        nullptr, plan.get(), 100, DEFAULT_MAX_OUTPUT_SIZE, false);

    Assert(retrieve_results->fields_data_size() == target_offsets.size());
    auto field0 = retrieve_results->fields_data(0);
    auto field1 = retrieve_results->fields_data(1);
    Assert(field0.has_scalars());
    auto field0_data = field0.scalars().long_data();
    Assert(field0_data.data_size() == 0);
    Assert(field1.vectors().float16_vector().size() == 0);
}

TEST(Float16, ExecWithPredicate) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT16, 16, knowhere::metric::L2);
    schema->AddDebugField("age", DataType::FLOAT);
    auto i64_fid = schema->AddDebugField("counter", DataType::INT64);
    schema->set_primary_field_id(i64_fid);
    const char* raw_plan = R"(vector_anns: <
                                    field_id: 100
                                    predicates: <
                                      binary_range_expr: <
                                        column_info: <
                                          field_id: 101
                                          data_type: Float
                                        >
                                        lower_inclusive: true,
                                        upper_inclusive: false,
                                        lower_value: <
                                          float_val: -1
                                        >
                                        upper_value: <
                                          float_val: 1
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
    int64_t N = ROW_COUNT;
    auto dataset = DataGen(schema, N);
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
    auto plan =
        CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
    auto num_queries = 5;
    auto ph_group_raw =
        CreatePlaceholderGroup<milvus::Float16Vector>(num_queries, 16, 1024);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    auto sr = segment->Search(plan.get(), ph_group.get(), 1L << 63);
    int topk = 5;

    query::Json json = SearchResultToJson(*sr);
    std::cout << json.dump(2);
}

// TEST(BFloat16, Insert) {
//     int64_t N = ROW_COUNT;
//     constexpr int64_t size_per_chunk = 32 * 1024;
//     auto schema = std::make_shared<Schema>();
//     auto bfloat16_vec_fid = schema->AddDebugField(
//         "bfloat16vec", DataType::VECTOR_BFLOAT16, 32, knowhere::metric::L2);
//     auto i64_fid = schema->AddDebugField("counter", DataType::INT64);
//     schema->set_primary_field_id(i64_fid);

//     auto dataset = DataGen(schema, N);
//     // auto seg_conf = SegcoreConfig::default_config();
//     auto segment = CreateGrowingSegment(schema, empty_index_meta);
//     segment->PreInsert(N);
//     segment->Insert(0,
//                     N,
//                     dataset.row_ids_.data(),
//                     dataset.timestamps_.data(),
//                     dataset.raw_);
//     auto bfloat16_ptr = dataset.get_col<bfloat16>(bfloat16_vec_fid);
//     SegmentInternalInterface& interface = *segment;
//     auto num_chunk = interface.num_chunk();
//     ASSERT_EQ(num_chunk, upper_div(N, size_per_chunk));
//     auto row_count = interface.get_row_count();
//     ASSERT_EQ(N, row_count);
//     for (auto chunk_id = 0; chunk_id < num_chunk; ++chunk_id) {
//         auto bfloat16_span = interface.chunk_data<milvus::BFloat16Vector>(
//             bfloat16_vec_fid, chunk_id);
//         auto begin = chunk_id * size_per_chunk;
//         auto end = std::min((chunk_id + 1) * size_per_chunk, N);
//         auto size_of_chunk = end - begin;
//         for (int i = 0; i < size_of_chunk; ++i) {
//             // std::cout << float16_span.data()[i] << " " << float16_ptr[i + begin * 32] << std::endl;
//             ASSERT_EQ(bfloat16_span.data()[i], bfloat16_ptr[i + begin * 32]);
//         }
//     }
// }

TEST(BFloat16, ExecWithoutPredicateFlat) {
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_BFLOAT16, 32, knowhere::metric::L2);
    schema->AddDebugField("age", DataType::FLOAT);
    auto i64_fid = schema->AddDebugField("counter", DataType::INT64);
    schema->set_primary_field_id(i64_fid);
    const char* raw_plan = R"(vector_anns: <
                                    field_id: 100
                                    query_info: <
                                      topk: 5
                                      round_decimal: 3
                                      metric_type: "L2"
                                      search_params: "{\"nprobe\": 10}"
                                    >
                                    placeholder_tag: "$0"
        >)";
    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
    auto plan =
        CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
    int64_t N = ROW_COUNT;
    auto dataset = DataGen(schema, N);
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);
    auto vec_ptr = dataset.get_col<bfloat16>(vec_fid);

    auto num_queries = 5;
    auto ph_group_raw =
        CreatePlaceholderGroup<milvus::BFloat16Vector>(num_queries, 32, 1024);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
    Timestamp timestamp = 1000000;
    auto sr = segment->Search(plan.get(), ph_group.get(), timestamp);

    std::vector<std::vector<std::string>> results;
    auto json = SearchResultToJson(*sr);
    std::cout << json.dump(2);
}

TEST(BFloat16, GetVector) {
    auto metricType = knowhere::metric::L2;
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    auto random = schema->AddDebugField("random", DataType::DOUBLE);
    auto vec = schema->AddDebugField(
        "embeddings", DataType::VECTOR_BFLOAT16, 128, metricType);
    schema->set_primary_field_id(pk);
    std::map<std::string, std::string> index_params = {
        {"index_type", "IVF_FLAT"},
        {"metric_type", metricType},
        {"nlist", "128"}};
    std::map<std::string, std::string> type_params = {{"dim", "128"}};
    FieldIndexMeta fieldIndexMeta(
        vec, std::move(index_params), std::move(type_params));
    auto config = SegcoreConfig::default_config();
    config.set_chunk_rows(1024);
    config.set_enable_interim_segment_index(true);
    std::map<FieldId, FieldIndexMeta> filedMap = {{vec, fieldIndexMeta}};
    IndexMetaPtr metaPtr =
        std::make_shared<CollectionIndexMeta>(100000, std::move(filedMap));
    auto segment_growing = CreateGrowingSegment(schema, metaPtr, 1, config);
    auto segment = dynamic_cast<SegmentGrowingImpl*>(segment_growing.get());

    int64_t per_batch = 5000;
    int64_t n_batch = 20;
    int64_t dim = 128;
    for (int64_t i = 0; i < n_batch; i++) {
        auto dataset = DataGen(schema, per_batch);
        auto fakevec = dataset.get_col<bfloat16>(vec);
        auto offset = segment->PreInsert(per_batch);
        segment->Insert(offset,
                        per_batch,
                        dataset.row_ids_.data(),
                        dataset.timestamps_.data(),
                        dataset.raw_);
        auto num_inserted = (i + 1) * per_batch;
        auto ids_ds = GenRandomIds(num_inserted);
        auto result =
            segment->bulk_subscript(vec, ids_ds->GetIds(), num_inserted);

        auto vector = result.get()->mutable_vectors()->bfloat16_vector();
        EXPECT_TRUE(vector.size() == num_inserted * dim * sizeof(bfloat16));
        for (size_t i = 0; i < num_inserted; ++i) {
            auto id = ids_ds->GetIds()[i];
            for (size_t j = 0; j < 128; ++j) {
                EXPECT_TRUE(
                    reinterpret_cast<bfloat16*>(vector.data())[i * dim + j] ==
                    fakevec[(id % per_batch) * dim + j]);
            }
        }
    }
}

TEST(BFloat16, RetrieveEmpty) {
    auto schema = std::make_shared<Schema>();
    auto fid_64 = schema->AddDebugField("i64", DataType::INT64);
    auto DIM = 16;
    auto fid_vec = schema->AddDebugField(
        "vector_64", DataType::VECTOR_BFLOAT16, DIM, knowhere::metric::L2);
    schema->set_primary_field_id(fid_64);

    int64_t N = 100;
    int64_t req_size = 10;
    auto choose = [=](int i) { return i * 3 % N; };

    auto segment = CreateSealedSegment(schema);

    auto plan = std::make_unique<query::RetrievePlan>(*schema);
    std::vector<int64_t> values;
    std::vector<proto::plan::GenericValue> retrieve_ints;
    for (int i = 0; i < req_size; ++i) {
        values.emplace_back(choose(i));
        proto::plan::GenericValue val;
        val.set_int64_val(i);
        retrieve_ints.push_back(val);
    }
    auto term_expr = std::make_shared<expr::TermFilterExpr>(
        expr::ColumnInfo(fid_64, DataType::INT64), retrieve_ints);
    auto expr_plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, term_expr);
    plan->plan_node_ = std::make_unique<query::RetrievePlanNode>();
    plan->plan_node_->plannodes_ = std::move(expr_plan);
    std::vector<FieldId> target_offsets{fid_64, fid_vec};
    plan->field_ids_ = target_offsets;

    auto retrieve_results = segment->Retrieve(
        nullptr, plan.get(), 100, DEFAULT_MAX_OUTPUT_SIZE, false);

    Assert(retrieve_results->fields_data_size() == target_offsets.size());
    auto field0 = retrieve_results->fields_data(0);
    auto field1 = retrieve_results->fields_data(1);
    Assert(field0.has_scalars());
    auto field0_data = field0.scalars().long_data();
    Assert(field0_data.data_size() == 0);
    Assert(field1.vectors().bfloat16_vector().size() == 0);
}

TEST(BFloat16, ExecWithPredicate) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_BFLOAT16, 16, knowhere::metric::L2);
    schema->AddDebugField("age", DataType::FLOAT);
    auto i64_fid = schema->AddDebugField("counter", DataType::INT64);
    schema->set_primary_field_id(i64_fid);
    const char* raw_plan = R"(vector_anns: <
                                    field_id: 100
                                    predicates: <
                                      binary_range_expr: <
                                        column_info: <
                                          field_id: 101
                                          data_type: Float
                                        >
                                        lower_inclusive: true,
                                        upper_inclusive: false,
                                        lower_value: <
                                          float_val: -1
                                        >
                                        upper_value: <
                                          float_val: 1
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
    int64_t N = ROW_COUNT;
    auto dataset = DataGen(schema, N);
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
    auto plan =
        CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
    auto num_queries = 5;
    auto ph_group_raw =
        CreatePlaceholderGroup<milvus::BFloat16Vector>(num_queries, 16, 1024);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
    Timestamp timestamp = 1000000;
    auto sr = segment->Search(plan.get(), ph_group.get(), timestamp);
    int topk = 5;

    query::Json json = SearchResultToJson(*sr);
    std::cout << json.dump(2);
}
