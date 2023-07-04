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
#include <boost/format.hpp>

#include "common/Types.h"
#include "segcore/SegmentSealedImpl.h"
#include "test_utils/DataGen.h"
#include "index/IndexFactory.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;
using milvus::segcore::LoadIndexInfo;

const int64_t ROW_COUNT = 10 * 1000;
const int64_t BIAS = 4200;

TEST(Sealed, without_predicate) {
    using namespace milvus::query;
    using namespace milvus::segcore;
    auto schema = std::make_shared<Schema>();
    auto dim = 16;
    auto topK = 5;
    auto metric_type = knowhere::metric::L2;
    auto fake_id = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, dim, metric_type);
    auto float_fid = schema->AddDebugField("age", DataType::FLOAT);
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

    auto N = ROW_COUNT;

    auto dataset = DataGen(schema, N);
    auto vec_col = dataset.get_col<float>(fake_id);
    for (int64_t i = 0; i < 1000 * dim; ++i) {
        vec_col.push_back(0);
    }
    auto query_ptr = vec_col.data() + BIAS * dim;
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
        CreatePlaceholderGroupFromBlob(num_queries, 16, query_ptr);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    Timestamp time = 1000000;
    std::vector<const PlaceholderGroup*> ph_group_arr = {ph_group.get()};

    auto sr = segment->Search(plan.get(), ph_group.get(), time);
    auto pre_result = SearchResultToJson(*sr);
    milvus::index::CreateIndexInfo create_index_info;
    create_index_info.field_type = DataType::VECTOR_FLOAT;
    create_index_info.metric_type = knowhere::metric::L2;
    create_index_info.index_type = knowhere::IndexEnum::INDEX_FAISS_IVFFLAT;

    auto indexing = milvus::index::IndexFactory::GetInstance().CreateIndex(
        create_index_info, nullptr);

    auto build_conf =
        knowhere::Json{{knowhere::meta::METRIC_TYPE, knowhere::metric::L2},
                       {knowhere::meta::DIM, std::to_string(dim)},
                       {knowhere::indexparam::NLIST, "100"}};

    auto search_conf = knowhere::Json{{knowhere::indexparam::NPROBE, 10}};

    auto database = knowhere::GenDataSet(N, dim, vec_col.data() + 1000 * dim);
    indexing->BuildWithDataset(database, build_conf);

    auto vec_index = dynamic_cast<milvus::index::VectorIndex*>(indexing.get());
    EXPECT_EQ(vec_index->Count(), N);
    EXPECT_EQ(vec_index->GetDim(), dim);
    auto query_dataset = knowhere::GenDataSet(num_queries, dim, query_ptr);

    milvus::SearchInfo searchInfo;
    searchInfo.topk_ = topK;
    searchInfo.metric_type_ = knowhere::metric::L2;
    searchInfo.search_params_ = search_conf;
    auto result = vec_index->Query(query_dataset, searchInfo, nullptr);
    auto ref_result = SearchResultToJson(*result);

    LoadIndexInfo load_info;
    load_info.field_id = fake_id.get();
    load_info.index = std::move(indexing);
    load_info.index_params["metric_type"] = "L2";

    // load index for vec field, load raw data for scalar field
    auto sealed_segment = SealedCreator(schema, dataset);
    sealed_segment->DropFieldData(fake_id);
    sealed_segment->LoadIndex(load_info);

    sr = sealed_segment->Search(plan.get(), ph_group.get(), time);

    auto post_result = SearchResultToJson(*sr);
    std::cout << "ref_result" << std::endl;
    std::cout << ref_result.dump(1) << std::endl;
    std::cout << "post_result" << std::endl;
    std::cout << post_result.dump(1);
    // ASSERT_EQ(ref_result.dump(1), post_result.dump(1));

    sr = sealed_segment->Search(plan.get(), ph_group.get(), 0);
    EXPECT_EQ(sr->get_total_result_count(), 0);
}

TEST(Sealed, with_predicate) {
    using namespace milvus::query;
    using namespace milvus::segcore;
    auto schema = std::make_shared<Schema>();
    auto dim = 16;
    auto topK = 5;
    auto metric_type = knowhere::metric::L2;
    auto fake_id = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, dim, metric_type);
    auto i64_fid = schema->AddDebugField("counter", DataType::INT64);
    schema->set_primary_field_id(i64_fid);
    // std::string dsl = R"({
    //     "bool": {
    //         "must": [
    //         {
    //             "range": {
    //                 "counter": {
    //                     "GE": 4200,
    //                     "LT": 4205
    //                 }
    //             }
    //         },
    //         {
    //             "vector": {
    //                 "fakevec": {
    //                     "metric_type": "L2",
    //                     "params": {
    //                         "nprobe": 10
    //                     },
    //                     "query": "$0",
    //                     "topk": 5,
    //                     "round_decimal": 6
    //                 }
    //             }
    //         }
    //         ]
    //     }
    // })";
    const char* raw_plan = R"(vector_anns: <
                                field_id: 100
                                predicates: <
                                  binary_range_expr: <
                                    column_info: <
                                      field_id: 101
                                      data_type: Int64
                                    >
                                    lower_inclusive: true,
                                    upper_inclusive: false,
                                    lower_value: <
                                      int64_val: 4200
                                    >
                                    upper_value: <
                                      int64_val: 4205
                                    >
                                  >
                                >
                                query_info: <
                                  topk: 5
                                  round_decimal: 6
                                  metric_type: "L2"
                                  search_params: "{\"nprobe\": 10}"
                                >
                                placeholder_tag: "$0"
     >)";

    auto N = ROW_COUNT;

    auto dataset = DataGen(schema, N);
    auto vec_col = dataset.get_col<float>(fake_id);
    auto query_ptr = vec_col.data() + BIAS * dim;
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
        CreatePlaceholderGroupFromBlob(num_queries, 16, query_ptr);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    Timestamp time = 10000000;
    std::vector<const PlaceholderGroup*> ph_group_arr = {ph_group.get()};

    auto sr = segment->Search(plan.get(), ph_group.get(), time);
    milvus::index::CreateIndexInfo create_index_info;
    create_index_info.field_type = DataType::VECTOR_FLOAT;
    create_index_info.metric_type = knowhere::metric::L2;
    create_index_info.index_type = knowhere::IndexEnum::INDEX_FAISS_IVFFLAT;
    auto indexing = milvus::index::IndexFactory::GetInstance().CreateIndex(
        create_index_info, nullptr);

    auto build_conf =
        knowhere::Json{{knowhere::meta::METRIC_TYPE, knowhere::metric::L2},
                       {knowhere::meta::DIM, std::to_string(dim)},
                       {knowhere::indexparam::NLIST, "100"}};

    auto database = knowhere::GenDataSet(N, dim, vec_col.data());
    indexing->BuildWithDataset(database, build_conf);

    auto vec_index = dynamic_cast<index::VectorIndex*>(indexing.get());
    EXPECT_EQ(vec_index->Count(), N);
    EXPECT_EQ(vec_index->GetDim(), dim);

    auto query_dataset = knowhere::GenDataSet(num_queries, dim, query_ptr);

    auto search_conf =
        knowhere::Json{{knowhere::meta::METRIC_TYPE, knowhere::metric::L2},
                       {knowhere::indexparam::NPROBE, 10}};
    milvus::SearchInfo searchInfo;
    searchInfo.topk_ = topK;
    searchInfo.metric_type_ = knowhere::metric::L2;
    searchInfo.search_params_ = search_conf;
    auto result = vec_index->Query(query_dataset, searchInfo, nullptr);

    LoadIndexInfo load_info;
    load_info.field_id = fake_id.get();
    load_info.index = std::move(indexing);
    load_info.index_params["metric_type"] = "L2";

    // load index for vec field, load raw data for scalar field
    auto sealed_segment = SealedCreator(schema, dataset);
    sealed_segment->DropFieldData(fake_id);
    sealed_segment->LoadIndex(load_info);

    sr = sealed_segment->Search(plan.get(), ph_group.get(), time);

    for (int i = 0; i < num_queries; ++i) {
        auto offset = i * topK;
        ASSERT_EQ(sr->seg_offsets_[offset], BIAS + i);
        ASSERT_EQ(sr->distances_[offset], 0.0);
    }
}

TEST(Sealed, with_predicate_filter_all) {
    using namespace milvus::query;
    using namespace milvus::segcore;
    auto schema = std::make_shared<Schema>();
    auto dim = 16;
    auto topK = 5;
    // auto metric_type = MetricType::METRIC_L2;
    auto metric_type = knowhere::metric::L2;
    auto fake_id = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, dim, metric_type);
    auto i64_fid = schema->AddDebugField("counter", DataType::INT64);
    schema->set_primary_field_id(i64_fid);
    // std::string dsl = R"({
    //     "bool": {
    //         "must": [
    //         {
    //             "range": {
    //                 "counter": {
    //                     "GE": 4200,
    //                     "LT": 4199
    //                 }
    //             }
    //         },
    //         {
    //             "vector": {
    //                 "fakevec": {
    //                     "metric_type": "L2",
    //                     "params": {
    //                         "nprobe": 10
    //                     },
    //                     "query": "$0",
    //                     "topk": 5,
    //                    "round_decimal": 6
    //                 }
    //             }
    //         }
    //         ]
    //     }
    // })";
    const char* raw_plan = R"(vector_anns: <
                                field_id: 100
                                predicates: <
                                  binary_range_expr: <
                                    column_info: <
                                      field_id: 101
                                      data_type: Int64
                                    >
                                    lower_inclusive: true,
                                    upper_inclusive: false,
                                    lower_value: <
                                      int64_val: 4200
                                    >
                                    upper_value: <
                                      int64_val: 4199
                                    >
                                  >
                                >
                                query_info: <
                                  topk: 5
                                  round_decimal: 6
                                  metric_type: "L2"
                                  search_params: "{\"nprobe\": 10}"
                                >
                                placeholder_tag: "$0"
     >)";

    auto N = ROW_COUNT;

    auto dataset = DataGen(schema, N);
    auto vec_col = dataset.get_col<float>(fake_id);
    auto query_ptr = vec_col.data() + BIAS * dim;
    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
    auto plan =
        CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
    auto num_queries = 5;
    auto ph_group_raw =
        CreatePlaceholderGroupFromBlob(num_queries, 16, query_ptr);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    Timestamp time = 10000000;
    std::vector<const PlaceholderGroup*> ph_group_arr = {ph_group.get()};

    milvus::index::CreateIndexInfo create_index_info;
    create_index_info.field_type = DataType::VECTOR_FLOAT;
    create_index_info.metric_type = knowhere::metric::L2;
    create_index_info.index_type = knowhere::IndexEnum::INDEX_FAISS_IVFFLAT;
    auto ivf_indexing = milvus::index::IndexFactory::GetInstance().CreateIndex(
        create_index_info, nullptr);

    auto ivf_build_conf =
        knowhere::Json{{knowhere::meta::DIM, std::to_string(dim)},
                       {knowhere::indexparam::NLIST, "100"},
                       {knowhere::meta::METRIC_TYPE, knowhere::metric::L2}};

    auto database = knowhere::GenDataSet(N, dim, vec_col.data());
    ivf_indexing->BuildWithDataset(database, ivf_build_conf);

    auto ivf_vec_index = dynamic_cast<index::VectorIndex*>(ivf_indexing.get());
    EXPECT_EQ(ivf_vec_index->Count(), N);
    EXPECT_EQ(ivf_vec_index->GetDim(), dim);

    LoadIndexInfo load_info;
    load_info.field_id = fake_id.get();
    load_info.index = std::move(ivf_indexing);
    load_info.index_params["metric_type"] = "L2";

    // load index for vec field, load raw data for scalar field
    auto ivf_sealed_segment = SealedCreator(schema, dataset);
    ivf_sealed_segment->DropFieldData(fake_id);
    ivf_sealed_segment->LoadIndex(load_info);

    auto sr = ivf_sealed_segment->Search(plan.get(), ph_group.get(), time);
    EXPECT_EQ(sr->get_total_result_count(), 0);

    auto hnsw_conf =
        knowhere::Json{{knowhere::meta::DIM, std::to_string(dim)},
                       {knowhere::indexparam::HNSW_M, "16"},
                       {knowhere::indexparam::EFCONSTRUCTION, "200"},
                       {knowhere::indexparam::EF, "200"},
                       {knowhere::meta::METRIC_TYPE, knowhere::metric::L2}};

    create_index_info.field_type = DataType::VECTOR_FLOAT;
    create_index_info.metric_type = knowhere::metric::L2;
    create_index_info.index_type = knowhere::IndexEnum::INDEX_HNSW;
    auto hnsw_indexing = milvus::index::IndexFactory::GetInstance().CreateIndex(
        create_index_info, nullptr);
    hnsw_indexing->BuildWithDataset(database, hnsw_conf);

    auto hnsw_vec_index =
        dynamic_cast<index::VectorIndex*>(hnsw_indexing.get());
    EXPECT_EQ(hnsw_vec_index->Count(), N);
    EXPECT_EQ(hnsw_vec_index->GetDim(), dim);

    LoadIndexInfo hnsw_load_info;
    hnsw_load_info.field_id = fake_id.get();
    hnsw_load_info.index = std::move(hnsw_indexing);
    hnsw_load_info.index_params["metric_type"] = "L2";

    // load index for vec field, load raw data for scalar field
    auto hnsw_sealed_segment = SealedCreator(schema, dataset);
    hnsw_sealed_segment->DropFieldData(fake_id);
    hnsw_sealed_segment->LoadIndex(hnsw_load_info);

    auto sr2 = hnsw_sealed_segment->Search(plan.get(), ph_group.get(), time);
    EXPECT_EQ(sr2->get_total_result_count(), 0);
}

TEST(Sealed, LoadFieldData) {
    auto dim = 16;
    auto topK = 5;
    auto N = ROW_COUNT;
    auto metric_type = knowhere::metric::L2;
    auto schema = std::make_shared<Schema>();
    auto fakevec_id = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, dim, metric_type);
    auto counter_id = schema->AddDebugField("counter", DataType::INT64);
    auto double_id = schema->AddDebugField("double", DataType::DOUBLE);
    auto nothing_id = schema->AddDebugField("nothing", DataType::INT32);
    auto str_id = schema->AddDebugField("str", DataType::VARCHAR);
    schema->AddDebugField("int8", DataType::INT8);
    schema->AddDebugField("int16", DataType::INT16);
    schema->AddDebugField("float", DataType::FLOAT);
    schema->AddDebugField("json", DataType::JSON);
    schema->set_primary_field_id(counter_id);

    auto dataset = DataGen(schema, N);

    auto fakevec = dataset.get_col<float>(fakevec_id);

    auto indexing = GenVecIndexing(N, dim, fakevec.data());

    auto segment = CreateSealedSegment(schema);
    // std::string dsl = R"({
    //     "bool": {
    //         "must": [
    //         {
    //             "range": {
    //                 "double": {
    //                     "GE": -1,
    //                     "LT": 1
    //                 }
    //             }
    //         },
    //         {
    //             "vector": {
    //                 "fakevec": {
    //                     "metric_type": "L2",
    //                     "params": {
    //                         "nprobe": 10
    //                     },
    //                     "query": "$0",
    //                     "topk": 5,
    //                     "round_decimal": 3
    //                 }
    //             }
    //         }
    //         ]
    //     }
    // })";
    const char* raw_plan = R"(vector_anns: <
                                    field_id: 100
                                    predicates: <
                                      binary_range_expr: <
                                        column_info: <
                                          field_id: 102
                                          data_type: Double
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

    Timestamp time = 1000000;
    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
    auto plan =
        CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
    auto num_queries = 5;
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, 16, 1024);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    ASSERT_ANY_THROW(segment->Search(plan.get(), ph_group.get(), time));

    SealedLoadFieldData(dataset, *segment);
    segment->Search(plan.get(), ph_group.get(), time);

    segment->DropFieldData(fakevec_id);
    ASSERT_ANY_THROW(segment->Search(plan.get(), ph_group.get(), time));

    LoadIndexInfo vec_info;
    vec_info.field_id = fakevec_id.get();
    vec_info.index = std::move(indexing);
    vec_info.index_params["metric_type"] = knowhere::metric::L2;
    segment->LoadIndex(vec_info);

    ASSERT_EQ(segment->num_chunk(), 1);
    ASSERT_EQ(segment->num_chunk_index(double_id), 0);
    ASSERT_EQ(segment->num_chunk_index(str_id), 0);
    auto chunk_span1 = segment->chunk_data<int64_t>(counter_id, 0);
    auto chunk_span2 = segment->chunk_data<double>(double_id, 0);
    auto chunk_span3 = segment->chunk_data<std::string_view>(str_id, 0);
    auto ref1 = dataset.get_col<int64_t>(counter_id);
    auto ref2 = dataset.get_col<double>(double_id);
    auto ref3 = dataset.get_col(str_id)->scalars().string_data().data();
    for (int i = 0; i < N; ++i) {
        ASSERT_EQ(chunk_span1[i], ref1[i]);
        ASSERT_EQ(chunk_span2[i], ref2[i]);
        ASSERT_EQ(chunk_span3[i], ref3[i]);
    }

    auto sr = segment->Search(plan.get(), ph_group.get(), time);
    auto json = SearchResultToJson(*sr);
    std::cout << json.dump(1);

    segment->DropIndex(fakevec_id);
    ASSERT_ANY_THROW(segment->Search(plan.get(), ph_group.get(), time));
}

TEST(Sealed, LoadFieldDataMmap) {
    auto dim = 16;
    auto topK = 5;
    auto N = ROW_COUNT;
    auto metric_type = knowhere::metric::L2;
    auto schema = std::make_shared<Schema>();
    auto fakevec_id = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, dim, metric_type);
    auto counter_id = schema->AddDebugField("counter", DataType::INT64);
    auto double_id = schema->AddDebugField("double", DataType::DOUBLE);
    auto nothing_id = schema->AddDebugField("nothing", DataType::INT32);
    auto str_id = schema->AddDebugField("str", DataType::VARCHAR);
    schema->AddDebugField("int8", DataType::INT8);
    schema->AddDebugField("int16", DataType::INT16);
    schema->AddDebugField("float", DataType::FLOAT);
    schema->AddDebugField("json", DataType::JSON);
    schema->set_primary_field_id(counter_id);

    auto dataset = DataGen(schema, N);

    auto fakevec = dataset.get_col<float>(fakevec_id);

    auto indexing = GenVecIndexing(N, dim, fakevec.data());

    auto segment = CreateSealedSegment(schema);
    // std::string dsl = R"({
    //     "bool": {
    //         "must": [
    //         {
    //             "range": {
    //                 "double": {
    //                     "GE": -1,
    //                     "LT": 1
    //                 }
    //             }
    //         },
    //         {
    //             "vector": {
    //                 "fakevec": {
    //                     "metric_type": "L2",
    //                     "params": {
    //                         "nprobe": 10
    //                     },
    //                     "query": "$0",
    //                     "topk": 5,
    //                     "round_decimal": 3
    //                 }
    //             }
    //         }
    //         ]
    //     }
    // })";
    const char* raw_plan = R"(vector_anns: <
                                    field_id: 100
                                    predicates: <
                                      binary_range_expr: <
                                        column_info: <
                                          field_id: 102
                                          data_type: Double
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

    Timestamp time = 1000000;
    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
    auto plan =
        CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
    auto num_queries = 5;
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, 16, 1024);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    ASSERT_ANY_THROW(segment->Search(plan.get(), ph_group.get(), time));

    SealedLoadFieldData(dataset, *segment, {}, true);
    segment->Search(plan.get(), ph_group.get(), time);

    segment->DropFieldData(fakevec_id);
    ASSERT_ANY_THROW(segment->Search(plan.get(), ph_group.get(), time));

    LoadIndexInfo vec_info;
    vec_info.field_id = fakevec_id.get();
    vec_info.index = std::move(indexing);
    vec_info.index_params["metric_type"] = knowhere::metric::L2;
    segment->LoadIndex(vec_info);

    ASSERT_EQ(segment->num_chunk(), 1);
    ASSERT_EQ(segment->num_chunk_index(double_id), 0);
    ASSERT_EQ(segment->num_chunk_index(str_id), 0);
    auto chunk_span1 = segment->chunk_data<int64_t>(counter_id, 0);
    auto chunk_span2 = segment->chunk_data<double>(double_id, 0);
    auto chunk_span3 = segment->chunk_data<std::string_view>(str_id, 0);
    auto ref1 = dataset.get_col<int64_t>(counter_id);
    auto ref2 = dataset.get_col<double>(double_id);
    auto ref3 = dataset.get_col(str_id)->scalars().string_data().data();
    for (int i = 0; i < N; ++i) {
        ASSERT_EQ(chunk_span1[i], ref1[i]);
        ASSERT_EQ(chunk_span2[i], ref2[i]);
        ASSERT_EQ(chunk_span3[i], ref3[i]);
    }

    auto sr = segment->Search(plan.get(), ph_group.get(), time);
    auto json = SearchResultToJson(*sr);
    std::cout << json.dump(1);

    segment->DropIndex(fakevec_id);
    ASSERT_ANY_THROW(segment->Search(plan.get(), ph_group.get(), time));
}

TEST(Sealed, LoadScalarIndex) {
    auto dim = 16;
    auto N = ROW_COUNT;
    auto metric_type = knowhere::metric::L2;
    auto schema = std::make_shared<Schema>();
    auto fakevec_id = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, dim, metric_type);
    auto counter_id = schema->AddDebugField("counter", DataType::INT64);
    auto double_id = schema->AddDebugField("double", DataType::DOUBLE);
    auto nothing_id = schema->AddDebugField("nothing", DataType::INT32);
    schema->set_primary_field_id(counter_id);

    auto dataset = DataGen(schema, N);

    auto fakevec = dataset.get_col<float>(fakevec_id);

    auto indexing = GenVecIndexing(N, dim, fakevec.data());

    auto segment = CreateSealedSegment(schema);
    // std::string dsl = R"({
    //     "bool": {
    //         "must": [
    //         {
    //             "range": {
    //                 "double": {
    //                     "GE": -1,
    //                     "LT": 1
    //                 }
    //             }
    //         },
    //         {
    //             "vector": {
    //                 "fakevec": {
    //                     "metric_type": "L2",
    //                     "params": {
    //                         "nprobe": 10
    //                     },
    //                     "query": "$0",
    //                     "topk": 5,
    //                     "round_decimal": 3
    //                 }
    //             }
    //         }
    //         ]
    //     }
    // })";
    const char* raw_plan = R"(vector_anns: <
                                    field_id: 100
                                    predicates: <
                                      binary_range_expr: <
                                        column_info: <
                                          field_id: 102
                                          data_type: Double
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

    Timestamp time = 1000000;
    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
    auto plan =
        CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
    auto num_queries = 5;
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, 16, 1024);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    LoadFieldDataInfo row_id_info;
    FieldMeta row_id_field_meta(
        FieldName("RowID"), RowFieldID, DataType::INT64);
    auto field_data =
        std::make_shared<milvus::storage::FieldData<int64_t>>(DataType::INT64);
    field_data->FillFieldData(dataset.row_ids_.data(), N);
    auto field_data_info = FieldDataInfo{
        RowFieldID.get(), N, std::vector<storage::FieldDataPtr>{field_data}};
    segment->LoadFieldData(RowFieldID, field_data_info);

    LoadFieldDataInfo ts_info;
    FieldMeta ts_field_meta(
        FieldName("Timestamp"), TimestampFieldID, DataType::INT64);
    field_data =
        std::make_shared<milvus::storage::FieldData<int64_t>>(DataType::INT64);
    field_data->FillFieldData(dataset.timestamps_.data(), N);
    field_data_info =
        FieldDataInfo{TimestampFieldID.get(),
                      N,
                      std::vector<storage::FieldDataPtr>{field_data}};
    segment->LoadFieldData(TimestampFieldID, field_data_info);

    LoadIndexInfo vec_info;
    vec_info.field_id = fakevec_id.get();
    vec_info.field_type = DataType::VECTOR_FLOAT;
    vec_info.index = std::move(indexing);
    vec_info.index_params["metric_type"] = knowhere::metric::L2;
    segment->LoadIndex(vec_info);

    LoadIndexInfo counter_index;
    counter_index.field_id = counter_id.get();
    counter_index.field_type = DataType::INT64;
    counter_index.index_params["index_type"] = "sort";
    auto counter_data = dataset.get_col<int64_t>(counter_id);
    counter_index.index =
        std::move(GenScalarIndexing<int64_t>(N, counter_data.data()));
    segment->LoadIndex(counter_index);

    LoadIndexInfo double_index;
    double_index.field_id = double_id.get();
    double_index.field_type = DataType::DOUBLE;
    double_index.index_params["index_type"] = "sort";
    auto double_data = dataset.get_col<double>(double_id);
    double_index.index =
        std::move(GenScalarIndexing<double>(N, double_data.data()));
    segment->LoadIndex(double_index);

    LoadIndexInfo nothing_index;
    nothing_index.field_id = nothing_id.get();
    nothing_index.field_type = DataType::INT32;
    nothing_index.index_params["index_type"] = "sort";
    auto nothing_data = dataset.get_col<int32_t>(nothing_id);
    nothing_index.index =
        std::move(GenScalarIndexing<int32_t>(N, nothing_data.data()));
    segment->LoadIndex(nothing_index);

    auto sr = segment->Search(plan.get(), ph_group.get(), time);
    auto json = SearchResultToJson(*sr);
    std::cout << json.dump(1);
}

TEST(Sealed, Delete) {
    auto dim = 16;
    auto topK = 5;
    auto N = 10;
    auto metric_type = knowhere::metric::L2;
    auto schema = std::make_shared<Schema>();
    auto fakevec_id = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, dim, metric_type);
    auto counter_id = schema->AddDebugField("counter", DataType::INT64);
    auto double_id = schema->AddDebugField("double", DataType::DOUBLE);
    auto nothing_id = schema->AddDebugField("nothing", DataType::INT32);
    schema->set_primary_field_id(counter_id);

    auto dataset = DataGen(schema, N);

    auto fakevec = dataset.get_col<float>(fakevec_id);

    auto segment = CreateSealedSegment(schema);
    // std::string dsl = R"({
    //     "bool": {
    //         "must": [
    //         {
    //             "range": {
    //                 "double": {
    //                     "GE": -1,
    //                     "LT": 1
    //                 }
    //             }
    //         },
    //         {
    //             "vector": {
    //                 "fakevec": {
    //                     "metric_type": "L2",
    //                     "params": {
    //                         "nprobe": 10
    //                     },
    //                     "query": "$0",
    //                     "topk": 5,
    //                     "round_decimal": 3
    //                 }
    //             }
    //         }
    //         ]
    //     }
    // })";
    const char* raw_plan = R"(vector_anns: <
                                    field_id: 100
                                    predicates: <
                                      binary_range_expr: <
                                        column_info: <
                                          field_id: 102
                                          data_type: Double
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

    Timestamp time = 1000000;
    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
    auto plan =
        CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
    auto num_queries = 5;
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, 16, 1024);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    ASSERT_ANY_THROW(segment->Search(plan.get(), ph_group.get(), time));

    SealedLoadFieldData(dataset, *segment);

    int64_t row_count = 5;
    std::vector<idx_t> pks{1, 2, 3, 4, 5};
    auto ids = std::make_unique<IdArray>();
    ids->mutable_int_id()->mutable_data()->Add(pks.begin(), pks.end());
    std::vector<Timestamp> timestamps{10, 10, 10, 10, 10};

    LoadDeletedRecordInfo info = {timestamps.data(), ids.get(), row_count};
    segment->LoadDeletedRecord(info);

    BitsetType bitset(N, false);
    segment->mask_with_delete(bitset, 10, 11);
    ASSERT_EQ(bitset.count(), pks.size());

    int64_t new_count = 3;
    std::vector<idx_t> new_pks{6, 7, 8};
    auto new_ids = std::make_unique<IdArray>();
    new_ids->mutable_int_id()->mutable_data()->Add(new_pks.begin(),
                                                   new_pks.end());
    std::vector<idx_t> new_timestamps{10, 10, 10};
    auto reserved_offset = segment->get_deleted_count();
    ASSERT_EQ(reserved_offset, row_count);
    segment->Delete(reserved_offset,
                    new_count,
                    new_ids.get(),
                    reinterpret_cast<const Timestamp*>(new_timestamps.data()));
}

TEST(Sealed, OverlapDelete) {
    auto dim = 16;
    auto topK = 5;
    auto N = 10;
    auto metric_type = knowhere::metric::L2;
    auto schema = std::make_shared<Schema>();
    auto fakevec_id = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, dim, metric_type);
    auto counter_id = schema->AddDebugField("counter", DataType::INT64);
    auto double_id = schema->AddDebugField("double", DataType::DOUBLE);
    auto nothing_id = schema->AddDebugField("nothing", DataType::INT32);
    schema->set_primary_field_id(counter_id);

    auto dataset = DataGen(schema, N);

    auto fakevec = dataset.get_col<float>(fakevec_id);

    auto segment = CreateSealedSegment(schema);
    // std::string dsl = R"({
    //     "bool": {
    //         "must": [
    //         {
    //             "range": {
    //                 "double": {
    //                     "GE": -1,
    //                     "LT": 1
    //                 }
    //             }
    //         },
    //         {
    //             "vector": {
    //                 "fakevec": {
    //                     "metric_type": "L2",
    //                     "params": {
    //                         "nprobe": 10
    //                     },
    //                     "query": "$0",
    //                     "topk": 5,
    //                     "round_decimal": 3
    //                 }
    //             }
    //         }
    //         ]
    //     }
    // })";
    const char* raw_plan = R"(vector_anns: <
                                    field_id: 100
                                    predicates: <
                                      binary_range_expr: <
                                        column_info: <
                                          field_id: 102
                                          data_type: Double
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

    Timestamp time = 1000000;
    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
    auto plan =
        CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
    auto num_queries = 5;
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, 16, 1024);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    ASSERT_ANY_THROW(segment->Search(plan.get(), ph_group.get(), time));

    SealedLoadFieldData(dataset, *segment);

    int64_t row_count = 5;
    std::vector<idx_t> pks{1, 2, 3, 4, 5};
    auto ids = std::make_unique<IdArray>();
    ids->mutable_int_id()->mutable_data()->Add(pks.begin(), pks.end());
    std::vector<Timestamp> timestamps{10, 10, 10, 10, 10};

    LoadDeletedRecordInfo info = {timestamps.data(), ids.get(), row_count};
    segment->LoadDeletedRecord(info);
    ASSERT_EQ(segment->get_deleted_count(), pks.size())
        << "deleted_count=" << segment->get_deleted_count()
        << " pks_count=" << pks.size() << std::endl;

    // Load overlapping delete records
    row_count += 3;
    pks.insert(pks.end(), {6, 7, 8});
    auto new_ids = std::make_unique<IdArray>();
    new_ids->mutable_int_id()->mutable_data()->Add(pks.begin(), pks.end());
    timestamps.insert(timestamps.end(), {11, 11, 11});
    LoadDeletedRecordInfo overlap_info = {
        timestamps.data(), new_ids.get(), row_count};
    segment->LoadDeletedRecord(overlap_info);

    BitsetType bitset(N, false);
    // NOTE: need to change delete timestamp, so not to hit the cache
    ASSERT_EQ(segment->get_deleted_count(), pks.size())
        << "deleted_count=" << segment->get_deleted_count()
        << " pks_count=" << pks.size() << std::endl;
    segment->mask_with_delete(bitset, 10, 12);
    ASSERT_EQ(bitset.count(), pks.size())
        << "bitset_count=" << bitset.count() << " pks_count=" << pks.size()
        << std::endl;
}

auto
GenMaxFloatVecs(int N, int dim) {
    std::vector<float> vecs;
    for (int i = 0; i < N; i++) {
        for (int j = 0; j < dim; j++) {
            vecs.push_back(std::numeric_limits<float>::max());
        }
    }
    return vecs;
}

auto
GenRandomFloatVecs(int N, int dim) {
    std::vector<float> vecs;
    srand(time(NULL));
    for (int i = 0; i < N; i++) {
        for (int j = 0; j < dim; j++) {
            vecs.push_back(static_cast<float>(rand()) /
                           static_cast<float>(RAND_MAX));
        }
    }
    return vecs;
}

auto
GenQueryVecs(int N, int dim) {
    std::vector<float> vecs;
    for (int i = 0; i < N; i++) {
        for (int j = 0; j < dim; j++) {
            vecs.push_back(1);
        }
    }
    return vecs;
}

TEST(Sealed, BF) {
    auto schema = std::make_shared<Schema>();
    auto dim = 128;
    auto metric_type = "L2";
    auto fake_id = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, dim, metric_type);
    auto i64_fid = schema->AddDebugField("counter", DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    int64_t N = 100000;

    auto dataset = DataGen(schema, N);
    auto segment = CreateSealedSegment(schema);
    std::cout << fake_id.get() << std::endl;
    SealedLoadFieldData(dataset, *segment, {fake_id.get()});

    auto vec_data = GenRandomFloatVecs(N, dim);
    auto field_data = storage::CreateFieldData(DataType::VECTOR_FLOAT, dim);
    field_data->FillFieldData(vec_data.data(), N);
    auto field_data_info = FieldDataInfo{
        fake_id.get(), N, std::vector<storage::FieldDataPtr>{field_data}};
    segment->LoadFieldData(fake_id, field_data_info);

    auto topK = 1;
    auto fmt = boost::format(R"(vector_anns: <
                                            field_id: 100
                                            query_info: <
                                                topk: %1%
                                                metric_type: "L2"
                                                search_params: "{\"nprobe\": 10}"
                                            >
                                            placeholder_tag: "$0">
                                            output_field_ids: 101)") %
               topK;
    auto serialized_expr_plan = fmt.str();
    auto binary_plan =
        translate_text_plan_to_binary_plan(serialized_expr_plan.data());
    auto plan =
        CreateSearchPlanByExpr(*schema, binary_plan.data(), binary_plan.size());

    auto num_queries = 10;
    auto query = GenQueryVecs(num_queries, dim);
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, query);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    auto result = segment->Search(plan.get(), ph_group.get(), MAX_TIMESTAMP);
    auto ves = SearchResultToVector(*result);
    // first: offset, second: distance
    EXPECT_GT(ves[0].first, 0);
    EXPECT_LE(ves[0].first, N);
    EXPECT_LE(ves[0].second, dim);

    auto result2 = segment->Search(plan.get(), ph_group.get(), 0);
    EXPECT_EQ(result2->get_total_result_count(), 0);
}

TEST(Sealed, BF_Overflow) {
    auto schema = std::make_shared<Schema>();
    auto dim = 128;
    auto metric_type = "L2";
    auto fake_id = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, dim, metric_type);
    auto i64_fid = schema->AddDebugField("counter", DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    int64_t N = 10;

    auto dataset = DataGen(schema, N);
    auto segment = CreateSealedSegment(schema);
    std::cout << fake_id.get() << std::endl;
    SealedLoadFieldData(dataset, *segment, {fake_id.get()});

    auto vec_data = GenMaxFloatVecs(N, dim);
    auto field_data = storage::CreateFieldData(DataType::VECTOR_FLOAT, dim);
    field_data->FillFieldData(vec_data.data(), N);
    auto field_data_info = FieldDataInfo{
        fake_id.get(), N, std::vector<storage::FieldDataPtr>{field_data}};
    segment->LoadFieldData(fake_id, field_data_info);

    auto topK = 1;
    auto fmt = boost::format(R"(vector_anns: <
                                            field_id: 100
                                            query_info: <
                                                topk: %1%
                                                metric_type: "L2"
                                                search_params: "{\"nprobe\": 10}"
                                            >
                                            placeholder_tag: "$0">
                                            output_field_ids: 101)") %
               topK;
    auto serialized_expr_plan = fmt.str();
    auto binary_plan =
        translate_text_plan_to_binary_plan(serialized_expr_plan.data());
    auto plan =
        CreateSearchPlanByExpr(*schema, binary_plan.data(), binary_plan.size());

    auto num_queries = 10;
    auto query = GenQueryVecs(num_queries, dim);
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, query);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    auto result = segment->Search(plan.get(), ph_group.get(), MAX_TIMESTAMP);
    auto ves = SearchResultToVector(*result);
    for (int i = 0; i < num_queries; ++i) {
        EXPECT_EQ(ves[0].first, -1);
    }
}

TEST(Sealed, DeleteCount) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk);
    auto segment = CreateSealedSegment(schema);

    int64_t c = 10;
    auto offset = segment->get_deleted_count();
    ASSERT_EQ(offset, 0);

    Timestamp begin_ts = 100;
    auto tss = GenTss(c, begin_ts);
    auto pks = GenPKs(c, 0);
    auto status = segment->Delete(offset, c, pks.get(), tss.data());
    ASSERT_TRUE(status.ok());

    auto cnt = segment->get_deleted_count();
    ASSERT_EQ(cnt, c);
}

TEST(Sealed, RealCount) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk);
    auto segment = CreateSealedSegment(schema);

    ASSERT_EQ(0, segment->get_real_count());

    int64_t c = 10;
    auto dataset = DataGen(schema, c);
    auto pks = dataset.get_col<int64_t>(pk);
    SealedLoadFieldData(dataset, *segment);

    // no delete.
    ASSERT_EQ(c, segment->get_real_count());

    // delete half.
    auto half = c / 2;
    auto del_offset1 = segment->get_deleted_count();
    ASSERT_EQ(del_offset1, 0);
    auto del_ids1 = GenPKs(pks.begin(), pks.begin() + half);
    auto del_tss1 = GenTss(half, c);
    auto status =
        segment->Delete(del_offset1, half, del_ids1.get(), del_tss1.data());
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(c - half, segment->get_real_count());

    // delete duplicate.
    auto del_offset2 = segment->get_deleted_count();
    ASSERT_EQ(del_offset2, half);
    auto del_tss2 = GenTss(half, c + half);
    status =
        segment->Delete(del_offset2, half, del_ids1.get(), del_tss2.data());
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(c - half, segment->get_real_count());

    // delete all.
    auto del_offset3 = segment->get_deleted_count();
    ASSERT_EQ(del_offset3, half * 2);
    auto del_ids3 = GenPKs(pks.begin(), pks.end());
    auto del_tss3 = GenTss(c, c + half * 2);
    status = segment->Delete(del_offset3, c, del_ids3.get(), del_tss3.data());
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(0, segment->get_real_count());
}

TEST(Sealed, GetVector) {
    auto dim = 16;
    auto topK = 5;
    auto N = ROW_COUNT;
    auto metric_type = knowhere::metric::L2;
    auto schema = std::make_shared<Schema>();
    auto fakevec_id = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, dim, metric_type);
    auto counter_id = schema->AddDebugField("counter", DataType::INT64);
    auto double_id = schema->AddDebugField("double", DataType::DOUBLE);
    auto nothing_id = schema->AddDebugField("nothing", DataType::INT32);
    auto str_id = schema->AddDebugField("str", DataType::VARCHAR);
    schema->AddDebugField("int8", DataType::INT8);
    schema->AddDebugField("int16", DataType::INT16);
    schema->AddDebugField("float", DataType::FLOAT);
    schema->set_primary_field_id(counter_id);

    auto dataset = DataGen(schema, N);

    auto fakevec = dataset.get_col<float>(fakevec_id);

    auto indexing = GenVecIndexing(N, dim, fakevec.data());

    auto segment_sealed = CreateSealedSegment(schema);

    LoadIndexInfo vec_info;
    vec_info.field_id = fakevec_id.get();
    vec_info.index = std::move(indexing);
    vec_info.index_params["metric_type"] = knowhere::metric::L2;
    segment_sealed->LoadIndex(vec_info);

    auto segment = dynamic_cast<SegmentSealedImpl*>(segment_sealed.get());

    auto has = segment->HasRawData(vec_info.field_id);
    EXPECT_TRUE(has);

    auto ids_ds = GenRandomIds(N);
    auto result = segment->get_vector(fakevec_id, ids_ds->GetIds(), N);

    auto vector = result.get()->mutable_vectors()->float_vector().data();
    EXPECT_TRUE(vector.size() == fakevec.size());
    for (size_t i = 0; i < N; ++i) {
        auto id = ids_ds->GetIds()[i];
        for (size_t j = 0; j < dim; ++j) {
            EXPECT_TRUE(vector[i * dim + j] == fakevec[id * dim + j]);
        }
    }
}
