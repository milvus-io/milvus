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

#include <boost/format.hpp>
#include <gtest/gtest.h>

#include "common/Types.h"
#include "common/Tracer.h"
#include "index/IndexFactory.h"
#include "knowhere/version.h"
#include "segcore/SegmentSealedImpl.h"
#include "storage/MmapManager.h"
#include "storage/MinioChunkManager.h"
#include "storage/RemoteChunkManagerSingleton.h"
#include "storage/Util.h"
#include "test_utils/DataGen.h"
#include "test_utils/indexbuilder_test_utils.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;

using milvus::segcore::LoadIndexInfo;

const int64_t ROW_COUNT = 10 * 1000;
const int64_t BIAS = 4200;

using Param = std::string;
class SealedTest : public ::testing::TestWithParam<Param> {
 public:
    void
    SetUp() override {
    }
};

TEST(Sealed, without_predicate) {
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
    Timestamp timestamp = 1000000;

    std::vector<const PlaceholderGroup*> ph_group_arr = {ph_group.get()};

    auto sr = segment->Search(plan.get(), ph_group.get(), timestamp);
    auto pre_result = SearchResultToJson(*sr);
    milvus::index::CreateIndexInfo create_index_info;
    create_index_info.field_type = DataType::VECTOR_FLOAT;
    create_index_info.metric_type = knowhere::metric::L2;
    create_index_info.index_type = knowhere::IndexEnum::INDEX_FAISS_IVFFLAT;
    create_index_info.index_engine_version =
        knowhere::Version::GetCurrentVersion().VersionNumber();

    auto indexing = milvus::index::IndexFactory::GetInstance().CreateIndex(
        create_index_info, milvus::storage::FileManagerContext());

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
    SearchResult result;
    vec_index->Query(query_dataset, searchInfo, nullptr, result);
    auto ref_result = SearchResultToJson(result);

    LoadIndexInfo load_info;
    load_info.field_id = fake_id.get();
    load_info.index = std::move(indexing);
    load_info.index_params["metric_type"] = "L2";

    // load index for vec field, load raw data for scalar field
    auto sealed_segment = SealedCreator(schema, dataset);
    sealed_segment->DropFieldData(fake_id);
    sealed_segment->LoadIndex(load_info);

    sr = sealed_segment->Search(plan.get(), ph_group.get(), timestamp);

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
    auto schema = std::make_shared<Schema>();
    auto dim = 16;
    auto topK = 5;
    auto metric_type = knowhere::metric::L2;
    auto fake_id = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, dim, metric_type);
    auto i64_fid = schema->AddDebugField("counter", DataType::INT64);
    schema->set_primary_field_id(i64_fid);
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
    Timestamp timestamp = 1000000;

    std::vector<const PlaceholderGroup*> ph_group_arr = {ph_group.get()};

    auto sr = segment->Search(plan.get(), ph_group.get(), timestamp);
    milvus::index::CreateIndexInfo create_index_info;
    create_index_info.field_type = DataType::VECTOR_FLOAT;
    create_index_info.metric_type = knowhere::metric::L2;
    create_index_info.index_type = knowhere::IndexEnum::INDEX_FAISS_IVFFLAT;
    create_index_info.index_engine_version =
        knowhere::Version::GetCurrentVersion().VersionNumber();
    auto indexing = milvus::index::IndexFactory::GetInstance().CreateIndex(
        create_index_info, milvus::storage::FileManagerContext());

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
    SearchResult result;
    vec_index->Query(query_dataset, searchInfo, nullptr, result);

    LoadIndexInfo load_info;
    load_info.field_id = fake_id.get();
    load_info.index = std::move(indexing);
    load_info.index_params["metric_type"] = "L2";

    // load index for vec field, load raw data for scalar field
    auto sealed_segment = SealedCreator(schema, dataset);
    sealed_segment->DropFieldData(fake_id);
    sealed_segment->LoadIndex(load_info);

    sr = sealed_segment->Search(plan.get(), ph_group.get(), timestamp);

    for (int i = 0; i < num_queries; ++i) {
        auto offset = i * topK;
        ASSERT_EQ(sr->seg_offsets_[offset], BIAS + i);
        ASSERT_EQ(sr->distances_[offset], 0.0);
    }
}

TEST(Sealed, with_predicate_filter_all) {
    auto schema = std::make_shared<Schema>();
    auto dim = 16;
    auto topK = 5;
    // auto metric_type = MetricType::METRIC_L2;
    auto metric_type = knowhere::metric::L2;
    auto fake_id = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, dim, metric_type);
    auto i64_fid = schema->AddDebugField("counter", DataType::INT64);
    schema->set_primary_field_id(i64_fid);
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
    Timestamp timestamp = 1000000;

    std::vector<const PlaceholderGroup*> ph_group_arr = {ph_group.get()};

    milvus::index::CreateIndexInfo create_index_info;
    create_index_info.field_type = DataType::VECTOR_FLOAT;
    create_index_info.metric_type = knowhere::metric::L2;
    create_index_info.index_type = knowhere::IndexEnum::INDEX_FAISS_IVFFLAT;
    create_index_info.index_engine_version =
        knowhere::Version::GetCurrentVersion().VersionNumber();
    auto ivf_indexing = milvus::index::IndexFactory::GetInstance().CreateIndex(
        create_index_info, milvus::storage::FileManagerContext());

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

    auto sr = ivf_sealed_segment->Search(plan.get(), ph_group.get(), timestamp);
    EXPECT_EQ(sr->unity_topK_, 0);
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
    create_index_info.index_engine_version =
        knowhere::Version::GetCurrentVersion().VersionNumber();
    auto hnsw_indexing = milvus::index::IndexFactory::GetInstance().CreateIndex(
        create_index_info, milvus::storage::FileManagerContext());
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

    auto sr2 =
        hnsw_sealed_segment->Search(plan.get(), ph_group.get(), timestamp);
    EXPECT_EQ(sr2->unity_topK_, 0);
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
    schema->AddDebugField("array", DataType::ARRAY, DataType::INT64);
    schema->set_primary_field_id(counter_id);
    auto int8_nullable_id =
        schema->AddDebugField("int8_null", DataType::INT8, true);
    auto int16_nullable_id =
        schema->AddDebugField("int16_null", DataType::INT16, true);
    auto int32_nullable_id =
        schema->AddDebugField("int32_null", DataType::INT32, true);
    auto int64_nullable_id =
        schema->AddDebugField("int64_null", DataType::INT64, true);
    auto double_nullable_id =
        schema->AddDebugField("double_null", DataType::DOUBLE, true);
    auto str_nullable_id =
        schema->AddDebugField("str_null", DataType::VARCHAR, true);
    auto float_nullable_id =
        schema->AddDebugField("float_null", DataType::FLOAT, true);

    auto dataset = DataGen(schema, N);

    auto fakevec = dataset.get_col<float>(fakevec_id);

    auto indexing = GenVecIndexing(
        N, dim, fakevec.data(), knowhere::IndexEnum::INDEX_FAISS_IVFFLAT);

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
    Timestamp timestamp = 1000000;
    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
    auto plan =
        CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
    auto num_queries = 5;
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, 16, 1024);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    ASSERT_ANY_THROW(segment->Search(plan.get(), ph_group.get(), timestamp));

    SealedLoadFieldData(dataset, *segment);
    segment->Search(plan.get(), ph_group.get(), timestamp);

    segment->DropFieldData(fakevec_id);
    ASSERT_ANY_THROW(segment->Search(plan.get(), ph_group.get(), timestamp));

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
    auto chunk_span3 =
        segment->get_batch_views<std::string_view>(str_id, 0, 0, N);
    auto chunk_span4 = segment->chunk_data<int8_t>(int8_nullable_id, 0);
    auto chunk_span5 = segment->chunk_data<int16_t>(int16_nullable_id, 0);
    auto chunk_span6 = segment->chunk_data<int32_t>(int32_nullable_id, 0);
    auto chunk_span7 = segment->chunk_data<int64_t>(int64_nullable_id, 0);
    auto chunk_span8 = segment->chunk_data<double>(double_nullable_id, 0);
    auto chunk_span9 =
        segment->get_batch_views<std::string_view>(str_nullable_id, 0, 0, N);

    auto ref1 = dataset.get_col<int64_t>(counter_id);
    auto ref2 = dataset.get_col<double>(double_id);
    auto ref3 = dataset.get_col(str_id)->scalars().string_data().data();
    auto ref4 = dataset.get_col<int8_t>(int8_nullable_id);
    auto ref5 = dataset.get_col<int16_t>(int16_nullable_id);
    auto ref6 = dataset.get_col<int32_t>(int32_nullable_id);
    auto ref7 = dataset.get_col<int64_t>(int64_nullable_id);
    auto ref8 = dataset.get_col<double>(double_nullable_id);
    auto ref9 =
        dataset.get_col(str_nullable_id)->scalars().string_data().data();
    auto valid4 = dataset.get_col_valid(int8_nullable_id);
    auto valid5 = dataset.get_col_valid(int16_nullable_id);
    auto valid6 = dataset.get_col_valid(int32_nullable_id);
    auto valid7 = dataset.get_col_valid(int64_nullable_id);
    auto valid8 = dataset.get_col_valid(double_nullable_id);
    auto valid9 = dataset.get_col_valid(str_nullable_id);
    ASSERT_EQ(chunk_span1.valid_data(), nullptr);
    ASSERT_EQ(chunk_span2.valid_data(), nullptr);
    ASSERT_EQ(chunk_span3.second.size(), 0);
    for (int i = 0; i < N; ++i) {
        ASSERT_EQ(chunk_span1.data()[i], ref1[i]);
        ASSERT_EQ(chunk_span2.data()[i], ref2[i]);
        ASSERT_EQ(chunk_span3.first[i], ref3[i]);
        ASSERT_EQ(chunk_span4.data()[i], ref4[i]);
        ASSERT_EQ(chunk_span5.data()[i], ref5[i]);
        ASSERT_EQ(chunk_span6.data()[i], ref6[i]);
        ASSERT_EQ(chunk_span7.data()[i], ref7[i]);
        ASSERT_EQ(chunk_span8.data()[i], ref8[i]);
        ASSERT_EQ(chunk_span9.first[i], ref9[i]);
        ASSERT_EQ(chunk_span4.valid_data()[i], valid4[i]);
        ASSERT_EQ(chunk_span5.valid_data()[i], valid5[i]);
        ASSERT_EQ(chunk_span6.valid_data()[i], valid6[i]);
        ASSERT_EQ(chunk_span7.valid_data()[i], valid7[i]);
        ASSERT_EQ(chunk_span8.valid_data()[i], valid8[i]);
        ASSERT_EQ(chunk_span9.second[i], valid9[i]);
    }

    auto sr = segment->Search(plan.get(), ph_group.get(), timestamp);
    auto json = SearchResultToJson(*sr);
    std::cout << json.dump(1);

    segment->DropIndex(fakevec_id);
    ASSERT_ANY_THROW(segment->Search(plan.get(), ph_group.get(), timestamp));
}

TEST(Sealed, ClearData) {
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
    schema->AddDebugField("array", DataType::ARRAY, DataType::INT64);
    schema->set_primary_field_id(counter_id);

    auto dataset = DataGen(schema, N);

    auto fakevec = dataset.get_col<float>(fakevec_id);

    auto indexing = GenVecIndexing(
        N, dim, fakevec.data(), knowhere::IndexEnum::INDEX_FAISS_IVFFLAT);

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
    Timestamp timestamp = 1000000;
    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
    auto plan =
        CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
    auto num_queries = 5;
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, 16, 1024);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    ASSERT_ANY_THROW(segment->Search(plan.get(), ph_group.get(), timestamp));

    SealedLoadFieldData(dataset, *segment);
    segment->Search(plan.get(), ph_group.get(), timestamp);

    segment->DropFieldData(fakevec_id);
    ASSERT_ANY_THROW(segment->Search(plan.get(), ph_group.get(), timestamp));

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
    auto chunk_span3 =
        segment->get_batch_views<std::string_view>(str_id, 0, 0, N);
    auto ref1 = dataset.get_col<int64_t>(counter_id);
    auto ref2 = dataset.get_col<double>(double_id);
    auto ref3 = dataset.get_col(str_id)->scalars().string_data().data();
    ASSERT_EQ(chunk_span3.second.size(), 0);
    for (int i = 0; i < N; ++i) {
        ASSERT_EQ(chunk_span1[i], ref1[i]);
        ASSERT_EQ(chunk_span2[i], ref2[i]);
        ASSERT_EQ(chunk_span3.first[i], ref3[i]);
    }

    auto sr = segment->Search(plan.get(), ph_group.get(), timestamp);
    auto json = SearchResultToJson(*sr);
    std::cout << json.dump(1);

    auto sealed_segment = (SegmentSealedImpl*)segment.get();
    sealed_segment->ClearData();
    ASSERT_EQ(sealed_segment->get_row_count(), 0);
    ASSERT_EQ(sealed_segment->get_real_count(), 0);
    ASSERT_ANY_THROW(segment->Search(plan.get(), ph_group.get(), timestamp));
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
    schema->AddDebugField("array", DataType::ARRAY, DataType::INT64);
    schema->set_primary_field_id(counter_id);

    auto dataset = DataGen(schema, N);

    auto fakevec = dataset.get_col<float>(fakevec_id);

    auto indexing = GenVecIndexing(
        N, dim, fakevec.data(), knowhere::IndexEnum::INDEX_FAISS_IVFFLAT);

    auto segment = CreateSealedSegment(schema);
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
    Timestamp timestamp = 1000000;
    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
    auto plan =
        CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
    auto num_queries = 5;
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, 16, 1024);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    ASSERT_ANY_THROW(segment->Search(plan.get(), ph_group.get(), timestamp));

    SealedLoadFieldData(dataset, *segment, {}, true);
    segment->Search(plan.get(), ph_group.get(), timestamp);

    segment->DropFieldData(fakevec_id);
    ASSERT_ANY_THROW(segment->Search(plan.get(), ph_group.get(), timestamp));

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
    auto chunk_span3 =
        segment->get_batch_views<std::string_view>(str_id, 0, 0, N);
    auto ref1 = dataset.get_col<int64_t>(counter_id);
    auto ref2 = dataset.get_col<double>(double_id);
    auto ref3 = dataset.get_col(str_id)->scalars().string_data().data();
    ASSERT_EQ(chunk_span3.second.size(), 0);
    for (int i = 0; i < N; ++i) {
        ASSERT_EQ(chunk_span1[i], ref1[i]);
        ASSERT_EQ(chunk_span2[i], ref2[i]);
        ASSERT_EQ(chunk_span3.first[i], ref3[i]);
    }

    auto sr = segment->Search(plan.get(), ph_group.get(), timestamp);
    auto json = SearchResultToJson(*sr);
    std::cout << json.dump(1);

    segment->DropIndex(fakevec_id);
    ASSERT_ANY_THROW(segment->Search(plan.get(), ph_group.get(), timestamp));
}

TEST(Sealed, LoadPkScalarIndex) {
    size_t N = ROW_COUNT;
    auto schema = std::make_shared<Schema>();
    auto pk_id = schema->AddDebugField("counter", DataType::INT64);
    auto nothing_id = schema->AddDebugField("nothing", DataType::INT32);
    schema->set_primary_field_id(pk_id);

    auto dataset = DataGen(schema, N);
    auto segment = CreateSealedSegment(schema);
    auto fields = schema->get_fields();
    for (auto field_data : dataset.raw_->fields_data()) {
        int64_t field_id = field_data.field_id();

        auto info = FieldDataInfo(field_data.field_id(), N);
        auto field_meta = fields.at(FieldId(field_id));
        info.channel->push(
            CreateFieldDataFromDataArray(N, &field_data, field_meta));
        info.channel->close();

        segment->LoadFieldData(FieldId(field_id), info);
    }

    LoadIndexInfo pk_index;
    pk_index.field_id = pk_id.get();
    pk_index.field_type = DataType::INT64;
    pk_index.index_params["index_type"] = "sort";
    auto pk_data = dataset.get_col<int64_t>(pk_id);
    pk_index.index = GenScalarIndexing<int64_t>(N, pk_data.data());
    segment->LoadIndex(pk_index);
}

TEST(Sealed, LoadScalarIndex) {
    auto dim = 16;
    size_t N = ROW_COUNT;
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

    auto indexing = GenVecIndexing(
        N, dim, fakevec.data(), knowhere::IndexEnum::INDEX_FAISS_IVFFLAT);

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
    Timestamp timestamp = 1000000;
    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
    auto plan =
        CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
    auto num_queries = 5;
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, 16, 1024);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    LoadFieldDataInfo row_id_info;
    FieldMeta row_id_field_meta(
        FieldName("RowID"), RowFieldID, DataType::INT64, false);
    auto field_data =
        std::make_shared<milvus::FieldData<int64_t>>(DataType::INT64, false);
    field_data->FillFieldData(dataset.row_ids_.data(), N);
    auto field_data_info = FieldDataInfo{
        RowFieldID.get(), N, std::vector<FieldDataPtr>{field_data}};
    segment->LoadFieldData(RowFieldID, field_data_info);

    LoadFieldDataInfo ts_info;
    FieldMeta ts_field_meta(
        FieldName("Timestamp"), TimestampFieldID, DataType::INT64, false);
    field_data =
        std::make_shared<milvus::FieldData<int64_t>>(DataType::INT64, false);
    field_data->FillFieldData(dataset.timestamps_.data(), N);
    field_data_info = FieldDataInfo{
        TimestampFieldID.get(), N, std::vector<FieldDataPtr>{field_data}};
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
    counter_index.index = GenScalarIndexing<int64_t>(N, counter_data.data());
    segment->LoadIndex(counter_index);

    LoadIndexInfo double_index;
    double_index.field_id = double_id.get();
    double_index.field_type = DataType::DOUBLE;
    double_index.index_params["index_type"] = "sort";
    auto double_data = dataset.get_col<double>(double_id);
    double_index.index = GenScalarIndexing<double>(N, double_data.data());
    segment->LoadIndex(double_index);

    LoadIndexInfo nothing_index;
    nothing_index.field_id = nothing_id.get();
    nothing_index.field_type = DataType::INT32;
    nothing_index.index_params["index_type"] = "sort";
    auto nothing_data = dataset.get_col<int32_t>(nothing_id);
    nothing_index.index = GenScalarIndexing<int32_t>(N, nothing_data.data());
    segment->LoadIndex(nothing_index);

    auto sr = segment->Search(plan.get(), ph_group.get(), timestamp);
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
    Timestamp timestamp = 1000000;
    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
    auto plan =
        CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
    auto num_queries = 5;
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, 16, 1024);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    ASSERT_ANY_THROW(segment->Search(plan.get(), ph_group.get(), timestamp));

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
    Timestamp timestamp = 1000000;
    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
    auto plan =
        CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
    auto num_queries = 5;
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, 16, 1024);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    ASSERT_ANY_THROW(segment->Search(plan.get(), ph_group.get(), timestamp));

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

    size_t N = 100000;

    auto dataset = DataGen(schema, N);
    auto segment = CreateSealedSegment(schema);
    std::cout << fake_id.get() << std::endl;
    SealedLoadFieldData(dataset, *segment, {fake_id.get()});

    auto vec_data = GenRandomFloatVecs(N, dim);
    auto field_data =
        storage::CreateFieldData(DataType::VECTOR_FLOAT, false, dim);
    field_data->FillFieldData(vec_data.data(), N);
    auto field_data_info =
        FieldDataInfo{fake_id.get(), N, std::vector<FieldDataPtr>{field_data}};
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
    EXPECT_GE(ves[0].first, 0);
    EXPECT_LE(ves[0].first, N);
    EXPECT_LE(ves[0].second, dim);
}

TEST(Sealed, BF_Overflow) {
    auto schema = std::make_shared<Schema>();
    auto dim = 128;
    auto metric_type = "L2";
    auto fake_id = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, dim, metric_type);
    auto i64_fid = schema->AddDebugField("counter", DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    size_t N = 10;

    auto dataset = DataGen(schema, N);
    auto segment = CreateSealedSegment(schema);
    std::cout << fake_id.get() << std::endl;
    SealedLoadFieldData(dataset, *segment, {fake_id.get()});

    auto vec_data = GenMaxFloatVecs(N, dim);
    auto field_data =
        storage::CreateFieldData(DataType::VECTOR_FLOAT, false, dim);
    field_data->FillFieldData(vec_data.data(), N);
    auto field_data_info =
        FieldDataInfo{fake_id.get(), N, std::vector<FieldDataPtr>{field_data}};
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
    {
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

        // shouldn't be filtered for empty segment.
        auto cnt = segment->get_deleted_count();
        ASSERT_EQ(cnt, 10);
    }
    {
        auto schema = std::make_shared<Schema>();
        auto pk = schema->AddDebugField("pk", DataType::INT64);
        schema->set_primary_field_id(pk);
        auto segment = CreateSealedSegment(schema);

        int64_t c = 10;
        auto dataset = DataGen(schema, c);
        auto pks = dataset.get_col<int64_t>(pk);
        SealedLoadFieldData(dataset, *segment);

        auto offset = segment->get_deleted_count();
        ASSERT_EQ(offset, 0);

        auto iter = std::max_element(pks.begin(), pks.end());
        auto delete_pks = GenPKs(c, *iter);
        Timestamp begin_ts = 100;
        auto tss = GenTss(c, begin_ts);
        auto status = segment->Delete(offset, c, delete_pks.get(), tss.data());
        ASSERT_TRUE(status.ok());

        // 9 of element should be filtered.
        auto cnt = segment->get_deleted_count();
        ASSERT_EQ(cnt, 1);
    }
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
    auto N = ROW_COUNT;
    auto metric_type = knowhere::metric::L2;
    auto schema = std::make_shared<Schema>();
    auto fakevec_id = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, dim, metric_type);
    auto counter_id = schema->AddDebugField("counter", DataType::INT64);
    schema->AddDebugField("int8", DataType::INT8);
    schema->AddDebugField("int16", DataType::INT16);
    schema->AddDebugField("float", DataType::FLOAT);
    schema->set_primary_field_id(counter_id);

    auto dataset = DataGen(schema, N);

    auto fakevec = dataset.get_col<float>(fakevec_id);

    auto indexing = GenVecIndexing(
        N, dim, fakevec.data(), knowhere::IndexEnum::INDEX_FAISS_IVFFLAT);

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

TEST(Sealed, GetVectorFromChunkCache) {
    // skip test due to mem leak from AWS::InitSDK
    return;

    auto dim = 16;
    auto topK = 5;
    auto N = ROW_COUNT;
    auto metric_type = knowhere::metric::L2;
    auto index_type = knowhere::IndexEnum::INDEX_FAISS_IVFPQ;

    auto file_name = std::string(
        "sealed_test_get_vector_from_chunk_cache/insert_log/1/101/1000000");

    auto sc = milvus::storage::StorageConfig{};
    milvus::storage::RemoteChunkManagerSingleton::GetInstance().Init(sc);
    auto mcm = std::make_unique<milvus::storage::MinioChunkManager>(sc);
    // mcm->CreateBucket(sc.bucket_name);

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
    auto field_data_meta =
        milvus::storage::FieldDataMeta{1, 2, 3, fakevec_id.get()};
    auto field_meta = milvus::FieldMeta(milvus::FieldName("facevec"),
                                        fakevec_id,
                                        milvus::DataType::VECTOR_FLOAT,
                                        dim,
                                        metric_type);

    auto rcm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                   .GetRemoteChunkManager();
    auto data = dataset.get_col<float>(fakevec_id);
    auto data_slices = std::vector<void*>{data.data()};
    auto slice_sizes = std::vector<int64_t>{static_cast<int64_t>(N)};
    auto slice_names = std::vector<std::string>{file_name};
    PutFieldData(rcm.get(),
                 data_slices,
                 slice_sizes,
                 slice_names,
                 field_data_meta,
                 field_meta);

    auto conf = generate_build_conf(index_type, metric_type);
    auto ds = knowhere::GenDataSet(N, dim, data.data());
    auto indexing = std::make_unique<index::VectorMemIndex<float>>(
        index_type,
        metric_type,
        knowhere::Version::GetCurrentVersion().VersionNumber());
    indexing->BuildWithDataset(ds, conf);
    auto segment_sealed = CreateSealedSegment(schema);

    LoadIndexInfo vec_info;
    vec_info.field_id = fakevec_id.get();
    vec_info.index = std::move(indexing);
    vec_info.index_params["metric_type"] = knowhere::metric::L2;
    segment_sealed->LoadIndex(vec_info);

    auto field_binlog_info =
        FieldBinlogInfo{fakevec_id.get(),
                        N,
                        std::vector<int64_t>{N},
                        false,
                        std::vector<std::string>{file_name}};
    segment_sealed->AddFieldDataInfoForSealed(
        LoadFieldDataInfo{std::map<int64_t, FieldBinlogInfo>{
            {fakevec_id.get(), field_binlog_info}}});

    auto segment = dynamic_cast<SegmentSealedImpl*>(segment_sealed.get());
    auto has = segment->HasRawData(vec_info.field_id);
    EXPECT_FALSE(has);

    auto ids_ds = GenRandomIds(N);
    auto result =
        segment->get_vector(fakevec_id, ids_ds->GetIds(), ids_ds->GetRows());

    auto vector = result.get()->mutable_vectors()->float_vector().data();
    EXPECT_TRUE(vector.size() == data.size());
    for (size_t i = 0; i < N; ++i) {
        auto id = ids_ds->GetIds()[i];
        for (size_t j = 0; j < dim; ++j) {
            auto expect = data[id * dim + j];
            auto actual = vector[i * dim + j];
            AssertInfo(expect == actual,
                       fmt::format("expect {}, actual {}", expect, actual));
        }
    }

    rcm->Remove(file_name);
    auto exist = rcm->Exist(file_name);
    Assert(!exist);
}

TEST(Sealed, GetSparseVectorFromChunkCache) {
    // skip test due to mem leak from AWS::InitSDK
    return;

    auto dim = 16;
    auto topK = 5;
    auto N = ROW_COUNT;
    auto metric_type = knowhere::metric::IP;
    // TODO: remove SegmentSealedImpl::TEST_skip_index_for_retrieve_ after
    // we have a type of sparse index that doesn't include raw data.
    auto index_type = knowhere::IndexEnum::INDEX_SPARSE_INVERTED_INDEX;

    auto file_name = std::string(
        "sealed_test_get_vector_from_chunk_cache/insert_log/1/101/1000000");

    auto sc = milvus::storage::StorageConfig{};
    milvus::storage::RemoteChunkManagerSingleton::GetInstance().Init(sc);
    auto mcm = std::make_unique<milvus::storage::MinioChunkManager>(sc);

    auto schema = std::make_shared<Schema>();
    auto fakevec_id = schema->AddDebugField(
        "fakevec", DataType::VECTOR_SPARSE_FLOAT, dim, metric_type);
    auto counter_id = schema->AddDebugField("counter", DataType::INT64);
    auto double_id = schema->AddDebugField("double", DataType::DOUBLE);
    auto nothing_id = schema->AddDebugField("nothing", DataType::INT32);
    auto str_id = schema->AddDebugField("str", DataType::VARCHAR);
    schema->AddDebugField("int8", DataType::INT8);
    schema->AddDebugField("int16", DataType::INT16);
    schema->AddDebugField("float", DataType::FLOAT);
    schema->set_primary_field_id(counter_id);

    auto dataset = DataGen(schema, N);
    auto field_data_meta =
        milvus::storage::FieldDataMeta{1, 2, 3, fakevec_id.get()};
    auto field_meta = milvus::FieldMeta(milvus::FieldName("facevec"),
                                        fakevec_id,
                                        milvus::DataType::VECTOR_SPARSE_FLOAT,
                                        dim,
                                        metric_type);

    auto rcm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                   .GetRemoteChunkManager();
    auto data = dataset.get_col<knowhere::sparse::SparseRow<float>>(fakevec_id);
    auto data_slices = std::vector<void*>{data.data()};
    auto slice_sizes = std::vector<int64_t>{static_cast<int64_t>(N)};
    auto slice_names = std::vector<std::string>{file_name};
    PutFieldData(rcm.get(),
                 data_slices,
                 slice_sizes,
                 slice_names,
                 field_data_meta,
                 field_meta);

    auto conf = generate_build_conf(index_type, metric_type);
    auto ds = knowhere::GenDataSet(N, dim, data.data());
    auto indexing = std::make_unique<index::VectorMemIndex<float>>(
        index_type,
        metric_type,
        knowhere::Version::GetCurrentVersion().VersionNumber());
    indexing->BuildWithDataset(ds, conf);
    auto segment_sealed = CreateSealedSegment(
        schema, nullptr, -1, SegcoreConfig::default_config(), true);

    LoadIndexInfo vec_info;
    vec_info.field_id = fakevec_id.get();
    vec_info.index = std::move(indexing);
    vec_info.index_params["metric_type"] = metric_type;
    segment_sealed->LoadIndex(vec_info);

    auto field_binlog_info =
        FieldBinlogInfo{fakevec_id.get(),
                        N,
                        std::vector<int64_t>{N},
                        false,
                        std::vector<std::string>{file_name}};
    segment_sealed->AddFieldDataInfoForSealed(
        LoadFieldDataInfo{std::map<int64_t, FieldBinlogInfo>{
            {fakevec_id.get(), field_binlog_info}}});

    auto segment = dynamic_cast<SegmentSealedImpl*>(segment_sealed.get());

    auto ids_ds = GenRandomIds(N);
    auto result =
        segment->get_vector(fakevec_id, ids_ds->GetIds(), ids_ds->GetRows());

    auto vector =
        result.get()->mutable_vectors()->sparse_float_vector().contents();
    // number of rows
    EXPECT_TRUE(vector.size() == data.size());
    auto sparse_rows = SparseBytesToRows(vector, true);
    for (size_t i = 0; i < N; ++i) {
        auto expect = data[ids_ds->GetIds()[i]];
        auto& actual = sparse_rows[i];
        AssertInfo(
            expect.size() == actual.size(),
            fmt::format("expect {}, actual {}", expect.size(), actual.size()));
        AssertInfo(
            memcmp(expect.data(), actual.data(), expect.data_byte_size()) == 0,
            "sparse float vector doesn't match");
    }

    rcm->Remove(file_name);
    auto exist = rcm->Exist(file_name);
    Assert(!exist);
}

TEST(Sealed, WarmupChunkCache) {
    // skip test due to mem leak from AWS::InitSDK
    return;

    auto dim = 16;
    auto topK = 5;
    auto N = ROW_COUNT;
    auto metric_type = knowhere::metric::L2;
    auto index_type = knowhere::IndexEnum::INDEX_FAISS_IVFPQ;

    auto mmap_dir = "/tmp/mmap";
    auto file_name = std::string(
        "sealed_test_get_vector_from_chunk_cache/insert_log/1/101/1000000");

    auto sc = milvus::storage::StorageConfig{};
    milvus::storage::RemoteChunkManagerSingleton::GetInstance().Init(sc);
    auto mcm = std::make_unique<milvus::storage::MinioChunkManager>(sc);

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
    auto field_data_meta =
        milvus::storage::FieldDataMeta{1, 2, 3, fakevec_id.get()};
    auto field_meta = milvus::FieldMeta(milvus::FieldName("facevec"),
                                        fakevec_id,
                                        milvus::DataType::VECTOR_FLOAT,
                                        dim,
                                        metric_type);

    auto rcm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                   .GetRemoteChunkManager();
    auto data = dataset.get_col<float>(fakevec_id);
    auto data_slices = std::vector<void*>{data.data()};
    auto slice_sizes = std::vector<int64_t>{static_cast<int64_t>(N)};
    auto slice_names = std::vector<std::string>{file_name};
    PutFieldData(rcm.get(),
                 data_slices,
                 slice_sizes,
                 slice_names,
                 field_data_meta,
                 field_meta);

    auto conf = generate_build_conf(index_type, metric_type);
    auto ds = knowhere::GenDataSet(N, dim, data.data());
    auto indexing = std::make_unique<index::VectorMemIndex<float>>(
        index_type,
        metric_type,
        knowhere::Version::GetCurrentVersion().VersionNumber());
    indexing->BuildWithDataset(ds, conf);
    auto segment_sealed = CreateSealedSegment(schema);

    LoadIndexInfo vec_info;
    vec_info.field_id = fakevec_id.get();
    vec_info.index = std::move(indexing);
    vec_info.index_params["metric_type"] = knowhere::metric::L2;
    segment_sealed->LoadIndex(vec_info);

    auto field_binlog_info =
        FieldBinlogInfo{fakevec_id.get(),
                        N,
                        std::vector<int64_t>{N},
                        false,
                        std::vector<std::string>{file_name}};
    segment_sealed->AddFieldDataInfoForSealed(
        LoadFieldDataInfo{std::map<int64_t, FieldBinlogInfo>{
            {fakevec_id.get(), field_binlog_info}}});

    auto segment = dynamic_cast<SegmentSealedImpl*>(segment_sealed.get());
    auto has = segment->HasRawData(vec_info.field_id);
    EXPECT_FALSE(has);

    segment_sealed->WarmupChunkCache(FieldId(vec_info.field_id), true);

    auto ids_ds = GenRandomIds(N);
    auto result =
        segment->get_vector(fakevec_id, ids_ds->GetIds(), ids_ds->GetRows());

    auto vector = result.get()->mutable_vectors()->float_vector().data();
    EXPECT_TRUE(vector.size() == data.size());
    for (size_t i = 0; i < N; ++i) {
        auto id = ids_ds->GetIds()[i];
        for (size_t j = 0; j < dim; ++j) {
            auto expect = data[id * dim + j];
            auto actual = vector[i * dim + j];
            AssertInfo(expect == actual,
                       fmt::format("expect {}, actual {}", expect, actual));
        }
    }

    rcm->Remove(file_name);
    std::filesystem::remove_all(mmap_dir);
    auto exist = rcm->Exist(file_name);
    Assert(!exist);
    exist = std::filesystem::exists(mmap_dir);
    Assert(!exist);
}

TEST(Sealed, LoadArrayFieldData) {
    auto dim = 16;
    auto topK = 5;
    auto N = 10;
    auto metric_type = knowhere::metric::L2;
    auto schema = std::make_shared<Schema>();
    auto fakevec_id = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, dim, metric_type);
    auto counter_id = schema->AddDebugField("counter", DataType::INT64);
    auto array_id =
        schema->AddDebugField("array", DataType::ARRAY, DataType::INT64);
    schema->set_primary_field_id(counter_id);

    auto dataset = DataGen(schema, N);
    auto fakevec = dataset.get_col<float>(fakevec_id);
    auto segment = CreateSealedSegment(schema);

    const char* raw_plan = R"(vector_anns:<
            field_id:100
            predicates:<
                json_contains_expr:<
                    column_info:<
                        field_id:102
                        data_type:Array
                        element_type:Int64
                    >
                    elements:<int64_val:1 >
                    op:Contains
                    elements_same_type:true
                >
            >
            query_info:<
                topk: 5
                round_decimal: 3
                metric_type: "L2"
                search_params: "{\"nprobe\": 10}"
            > placeholder_tag:"$0"
        >)";

    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
    auto plan =
        CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
    auto num_queries = 5;
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, 16, 1024);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    SealedLoadFieldData(dataset, *segment);
    segment->Search(plan.get(), ph_group.get(), 1L << 63);

    auto ids_ds = GenRandomIds(N);
    auto s = dynamic_cast<SegmentSealedImpl*>(segment.get());
    auto int64_result = s->bulk_subscript(array_id, ids_ds->GetIds(), N);
    auto result_count = int64_result->scalars().array_data().data().size();
    ASSERT_EQ(result_count, N);
}

TEST(Sealed, LoadArrayFieldDataWithMMap) {
    auto dim = 16;
    auto topK = 5;
    auto N = ROW_COUNT;
    auto metric_type = knowhere::metric::L2;
    auto schema = std::make_shared<Schema>();
    auto fakevec_id = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, dim, metric_type);
    auto counter_id = schema->AddDebugField("counter", DataType::INT64);
    auto array_id =
        schema->AddDebugField("array", DataType::ARRAY, DataType::INT64);
    schema->set_primary_field_id(counter_id);

    auto dataset = DataGen(schema, N);
    auto fakevec = dataset.get_col<float>(fakevec_id);
    auto segment = CreateSealedSegment(schema);

    const char* raw_plan = R"(vector_anns:<
            field_id:100
            predicates:<
                json_contains_expr:<
                    column_info:<
                        field_id:102
                        data_type:Array
                        element_type:Int64
                    >
                    elements:<int64_val:1 >
                    op:Contains
                    elements_same_type:true
                >
            >
            query_info:<
                topk: 5
                round_decimal: 3
                metric_type: "L2"
                search_params: "{\"nprobe\": 10}"
            > placeholder_tag:"$0"
        >)";

    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
    auto plan =
        CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
    auto num_queries = 5;
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, 16, 1024);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    SealedLoadFieldData(dataset, *segment, {}, true);
    segment->Search(plan.get(), ph_group.get(), 1L << 63);
}

TEST(Sealed, SkipIndexSkipUnaryRange) {
    auto schema = std::make_shared<Schema>();
    auto dim = 128;
    auto metrics_type = "L2";
    auto fake_vec_fid = schema->AddDebugField(
        "fakeVec", DataType::VECTOR_FLOAT, dim, metrics_type);
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto i32_fid = schema->AddDebugField("int32_field", DataType::INT32);
    auto i16_fid = schema->AddDebugField("int16_field", DataType::INT16);
    auto i8_fid = schema->AddDebugField("int8_field", DataType::INT8);
    auto float_fid = schema->AddDebugField("float_field", DataType::FLOAT);
    auto double_fid = schema->AddDebugField("double_field", DataType::DOUBLE);
    size_t N = 10;
    auto dataset = DataGen(schema, N);
    auto segment = CreateSealedSegment(schema);
    std::cout << "pk_fid:" << pk_fid.get() << std::endl;

    //test for int64
    std::vector<int64_t> pks = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    auto pk_field_data =
        storage::CreateFieldData(DataType::INT64, false, 1, 10);
    pk_field_data->FillFieldData(pks.data(), N);
    segment->LoadPrimitiveSkipIndex(
        pk_fid, 0, DataType::INT64, pk_field_data->Data(), nullptr, N);
    auto& skip_index = segment->GetSkipIndex();
    bool equal_5_skip =
        skip_index.CanSkipUnaryRange<int64_t>(pk_fid, 0, OpType::Equal, 5);
    bool equal_12_skip =
        skip_index.CanSkipUnaryRange<int64_t>(pk_fid, 0, OpType::Equal, 12);
    bool equal_10_skip =
        skip_index.CanSkipUnaryRange<int64_t>(pk_fid, 0, OpType::Equal, 10);
    ASSERT_FALSE(equal_5_skip);
    ASSERT_TRUE(equal_12_skip);
    ASSERT_FALSE(equal_10_skip);
    bool less_than_1_skip =
        skip_index.CanSkipUnaryRange<int64_t>(pk_fid, 0, OpType::LessThan, 1);
    bool less_than_5_skip =
        skip_index.CanSkipUnaryRange<int64_t>(pk_fid, 0, OpType::LessThan, 5);
    ASSERT_TRUE(less_than_1_skip);
    ASSERT_FALSE(less_than_5_skip);
    bool less_equal_than_1_skip =
        skip_index.CanSkipUnaryRange<int64_t>(pk_fid, 0, OpType::LessEqual, 1);
    bool less_equal_than_15_skip =
        skip_index.CanSkipUnaryRange<int64_t>(pk_fid, 0, OpType::LessThan, 15);
    ASSERT_FALSE(less_equal_than_1_skip);
    ASSERT_FALSE(less_equal_than_15_skip);
    bool greater_than_10_skip = skip_index.CanSkipUnaryRange<int64_t>(
        pk_fid, 0, OpType::GreaterThan, 10);
    bool greater_than_5_skip = skip_index.CanSkipUnaryRange<int64_t>(
        pk_fid, 0, OpType::GreaterThan, 5);
    ASSERT_TRUE(greater_than_10_skip);
    ASSERT_FALSE(greater_than_5_skip);
    bool greater_equal_than_10_skip = skip_index.CanSkipUnaryRange<int64_t>(
        pk_fid, 0, OpType::GreaterEqual, 10);
    bool greater_equal_than_5_skip = skip_index.CanSkipUnaryRange<int64_t>(
        pk_fid, 0, OpType::GreaterEqual, 5);
    ASSERT_FALSE(greater_equal_than_10_skip);
    ASSERT_FALSE(greater_equal_than_5_skip);

    //test for int32
    std::vector<int32_t> int32s = {2, 2, 3, 4, 5, 6, 7, 8, 9, 12};
    auto int32_field_data =
        storage::CreateFieldData(DataType::INT32, false, 1, 10);
    int32_field_data->FillFieldData(int32s.data(), N);
    segment->LoadPrimitiveSkipIndex(
        i32_fid, 0, DataType::INT32, int32_field_data->Data(), nullptr, N);
    less_than_1_skip =
        skip_index.CanSkipUnaryRange<int32_t>(i32_fid, 0, OpType::LessThan, 1);
    ASSERT_TRUE(less_than_1_skip);

    //test for int16
    std::vector<int16_t> int16s = {2, 2, 3, 4, 5, 6, 7, 8, 9, 12};
    auto int16_field_data =
        storage::CreateFieldData(DataType::INT16, false, 1, 10);
    int16_field_data->FillFieldData(int16s.data(), N);
    segment->LoadPrimitiveSkipIndex(
        i16_fid, 0, DataType::INT16, int16_field_data->Data(), nullptr, N);
    bool less_than_12_skip =
        skip_index.CanSkipUnaryRange<int16_t>(i16_fid, 0, OpType::LessThan, 12);
    ASSERT_FALSE(less_than_12_skip);

    //test for int8
    std::vector<int8_t> int8s = {2, 2, 3, 4, 5, 6, 7, 8, 9, 12};
    auto int8_field_data =
        storage::CreateFieldData(DataType::INT8, false, 1, 10);
    int8_field_data->FillFieldData(int8s.data(), N);
    segment->LoadPrimitiveSkipIndex(
        i8_fid, 0, DataType::INT8, int8_field_data->Data(), nullptr, N);
    bool greater_than_12_skip = skip_index.CanSkipUnaryRange<int8_t>(
        i8_fid, 0, OpType::GreaterThan, 12);
    ASSERT_TRUE(greater_than_12_skip);

    // test for float
    std::vector<float> floats = {
        1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0};
    auto float_field_data =
        storage::CreateFieldData(DataType::FLOAT, false, 1, 10);
    float_field_data->FillFieldData(floats.data(), N);
    segment->LoadPrimitiveSkipIndex(
        float_fid, 0, DataType::FLOAT, float_field_data->Data(), nullptr, N);
    greater_than_10_skip = skip_index.CanSkipUnaryRange<float>(
        float_fid, 0, OpType::GreaterThan, 10.0);
    ASSERT_TRUE(greater_than_10_skip);

    // test for double
    std::vector<double> doubles = {
        1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0};
    auto double_field_data =
        storage::CreateFieldData(DataType::DOUBLE, false, 1, 10);
    double_field_data->FillFieldData(doubles.data(), N);
    segment->LoadPrimitiveSkipIndex(
        double_fid, 0, DataType::DOUBLE, double_field_data->Data(), nullptr, N);
    greater_than_10_skip = skip_index.CanSkipUnaryRange<double>(
        double_fid, 0, OpType::GreaterThan, 10.0);
    ASSERT_TRUE(greater_than_10_skip);
}

TEST(Sealed, SkipIndexSkipBinaryRange) {
    auto schema = std::make_shared<Schema>();
    auto dim = 128;
    auto metrics_type = "L2";
    auto fake_vec_fid = schema->AddDebugField(
        "fakeVec", DataType::VECTOR_FLOAT, dim, metrics_type);
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    size_t N = 10;
    auto dataset = DataGen(schema, N);
    auto segment = CreateSealedSegment(schema);
    std::cout << "pk_fid:" << pk_fid.get() << std::endl;

    //test for int64
    std::vector<int64_t> pks = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    auto pk_field_data =
        storage::CreateFieldData(DataType::INT64, false, 1, 10);
    pk_field_data->FillFieldData(pks.data(), N);
    segment->LoadPrimitiveSkipIndex(
        pk_fid, 0, DataType::INT64, pk_field_data->Data(), nullptr, N);
    auto& skip_index = segment->GetSkipIndex();
    ASSERT_FALSE(
        skip_index.CanSkipBinaryRange<int64_t>(pk_fid, 0, -3, 1, true, true));
    ASSERT_TRUE(
        skip_index.CanSkipBinaryRange<int64_t>(pk_fid, 0, -3, 1, true, false));

    ASSERT_FALSE(
        skip_index.CanSkipBinaryRange<int64_t>(pk_fid, 0, 7, 9, true, true));
    ASSERT_FALSE(
        skip_index.CanSkipBinaryRange<int64_t>(pk_fid, 0, 8, 12, true, false));

    ASSERT_TRUE(
        skip_index.CanSkipBinaryRange<int64_t>(pk_fid, 0, 10, 12, false, true));
    ASSERT_FALSE(
        skip_index.CanSkipBinaryRange<int64_t>(pk_fid, 0, 10, 12, true, true));
}

TEST(Sealed, SkipIndexSkipUnaryRangeNullable) {
    auto schema = std::make_shared<Schema>();
    auto dim = 128;
    auto metrics_type = "L2";
    auto fake_vec_fid = schema->AddDebugField(
        "fakeVec", DataType::VECTOR_FLOAT, dim, metrics_type);
    auto i64_fid = schema->AddDebugField("int64_field", DataType::INT64, true);

    auto dataset = DataGen(schema, 5);
    auto segment = CreateSealedSegment(schema);

    //test for int64
    std::vector<int64_t> int64s = {1, 2, 3, 4, 5};
    std::array<uint8_t, 1> valid_data = {0x03};
    FixedVector<bool> valid_data_ = {true, true, false, false, false};
    auto int64s_field_data =
        storage::CreateFieldData(DataType::INT64, true, 1, 5);

    int64s_field_data->FillFieldData(int64s.data(), valid_data.data(), 5);
    segment->LoadPrimitiveSkipIndex(i64_fid,
                                    0,
                                    DataType::INT64,
                                    int64s_field_data->Data(),
                                    valid_data_.data(),
                                    5);
    auto& skip_index = segment->GetSkipIndex();
    bool equal_5_skip =
        skip_index.CanSkipUnaryRange<int64_t>(i64_fid, 0, OpType::Equal, 5);
    bool equal_4_skip =
        skip_index.CanSkipUnaryRange<int64_t>(i64_fid, 0, OpType::Equal, 4);
    bool equal_2_skip =
        skip_index.CanSkipUnaryRange<int64_t>(i64_fid, 0, OpType::Equal, 2);
    bool equal_1_skip =
        skip_index.CanSkipUnaryRange<int64_t>(i64_fid, 0, OpType::Equal, 1);
    ASSERT_TRUE(equal_5_skip);
    ASSERT_TRUE(equal_4_skip);
    ASSERT_FALSE(equal_2_skip);
    ASSERT_FALSE(equal_1_skip);
    bool less_than_1_skip =
        skip_index.CanSkipUnaryRange<int64_t>(i64_fid, 0, OpType::LessThan, 1);
    bool less_than_5_skip =
        skip_index.CanSkipUnaryRange<int64_t>(i64_fid, 0, OpType::LessThan, 5);
    ASSERT_TRUE(less_than_1_skip);
    ASSERT_FALSE(less_than_5_skip);
    bool less_equal_than_1_skip =
        skip_index.CanSkipUnaryRange<int64_t>(i64_fid, 0, OpType::LessEqual, 1);
    bool less_equal_than_15_skip =
        skip_index.CanSkipUnaryRange<int64_t>(i64_fid, 0, OpType::LessThan, 15);
    ASSERT_FALSE(less_equal_than_1_skip);
    ASSERT_FALSE(less_equal_than_15_skip);
    bool greater_than_10_skip = skip_index.CanSkipUnaryRange<int64_t>(
        i64_fid, 0, OpType::GreaterThan, 10);
    bool greater_than_5_skip = skip_index.CanSkipUnaryRange<int64_t>(
        i64_fid, 0, OpType::GreaterThan, 5);
    bool greater_than_2_skip = skip_index.CanSkipUnaryRange<int64_t>(
        i64_fid, 0, OpType::GreaterThan, 2);
    bool greater_than_1_skip = skip_index.CanSkipUnaryRange<int64_t>(
        i64_fid, 0, OpType::GreaterThan, 1);
    ASSERT_TRUE(greater_than_10_skip);
    ASSERT_TRUE(greater_than_5_skip);
    ASSERT_TRUE(greater_than_2_skip);
    ASSERT_FALSE(greater_than_1_skip);
    bool greater_equal_than_3_skip = skip_index.CanSkipUnaryRange<int64_t>(
        i64_fid, 0, OpType::GreaterEqual, 3);
    bool greater_equal_than_2_skip = skip_index.CanSkipUnaryRange<int64_t>(
        i64_fid, 0, OpType::GreaterEqual, 2);
    ASSERT_TRUE(greater_equal_than_3_skip);
    ASSERT_FALSE(greater_equal_than_2_skip);
}

TEST(Sealed, SkipIndexSkipBinaryRangeNullable) {
    auto schema = std::make_shared<Schema>();
    auto dim = 128;
    auto metrics_type = "L2";
    auto fake_vec_fid = schema->AddDebugField(
        "fakeVec", DataType::VECTOR_FLOAT, dim, metrics_type);
    auto i64_fid = schema->AddDebugField("int64_field", DataType::INT64, true);
    auto dataset = DataGen(schema, 5);
    auto segment = CreateSealedSegment(schema);

    //test for int64
    std::vector<int64_t> int64s = {1, 2, 3, 4, 5};
    std::array<uint8_t, 1> valid_data = {0x03};
    FixedVector<bool> valid_data_ = {true, true, false, false, false};
    auto int64s_field_data =
        storage::CreateFieldData(DataType::INT64, true, 1, 5);

    int64s_field_data->FillFieldData(int64s.data(), valid_data.data(), 5);
    segment->LoadPrimitiveSkipIndex(i64_fid,
                                    0,
                                    DataType::INT64,
                                    int64s_field_data->Data(),
                                    valid_data_.data(),
                                    5);
    auto& skip_index = segment->GetSkipIndex();
    ASSERT_FALSE(
        skip_index.CanSkipBinaryRange<int64_t>(i64_fid, 0, -3, 1, true, true));
    ASSERT_TRUE(
        skip_index.CanSkipBinaryRange<int64_t>(i64_fid, 0, -3, 1, true, false));

    ASSERT_FALSE(
        skip_index.CanSkipBinaryRange<int64_t>(i64_fid, 0, 1, 3, true, true));
    ASSERT_FALSE(
        skip_index.CanSkipBinaryRange<int64_t>(i64_fid, 0, 1, 2, true, false));

    ASSERT_TRUE(
        skip_index.CanSkipBinaryRange<int64_t>(i64_fid, 0, 2, 3, false, true));
    ASSERT_FALSE(
        skip_index.CanSkipBinaryRange<int64_t>(i64_fid, 0, 2, 3, true, true));
}

TEST(Sealed, SkipIndexSkipStringRange) {
    auto schema = std::make_shared<Schema>();
    auto dim = 128;
    auto metrics_type = "L2";
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto string_fid = schema->AddDebugField("string_field", DataType::VARCHAR);
    auto fake_vec_fid = schema->AddDebugField(
        "fakeVec", DataType::VECTOR_FLOAT, dim, metrics_type);
    size_t N = 5;
    auto dataset = DataGen(schema, N);
    auto segment = CreateSealedSegment(schema);

    //test for string
    std::vector<std::string> strings = {"e", "f", "g", "g", "j"};
    auto string_field_data =
        storage::CreateFieldData(DataType::VARCHAR, false, 1, N);
    string_field_data->FillFieldData(strings.data(), N);
    auto string_field_data_info = FieldDataInfo{
        string_fid.get(), N, std::vector<FieldDataPtr>{string_field_data}};
    segment->LoadFieldData(string_fid, string_field_data_info);
    auto& skip_index = segment->GetSkipIndex();
    ASSERT_TRUE(skip_index.CanSkipUnaryRange<std::string>(
        string_fid, 0, OpType::Equal, "w"));
    ASSERT_FALSE(skip_index.CanSkipUnaryRange<std::string>(
        string_fid, 0, OpType::Equal, "e"));
    ASSERT_FALSE(skip_index.CanSkipUnaryRange<std::string>(
        string_fid, 0, OpType::Equal, "j"));

    ASSERT_TRUE(skip_index.CanSkipUnaryRange<std::string>(
        string_fid, 0, OpType::LessThan, "e"));
    ASSERT_FALSE(skip_index.CanSkipUnaryRange<std::string>(
        string_fid, 0, OpType::LessEqual, "e"));

    ASSERT_TRUE(skip_index.CanSkipUnaryRange<std::string>(
        string_fid, 0, OpType::GreaterThan, "j"));
    ASSERT_FALSE(skip_index.CanSkipUnaryRange<std::string>(
        string_fid, 0, OpType::GreaterEqual, "j"));
    ASSERT_FALSE(skip_index.CanSkipUnaryRange<int64_t>(
        string_fid, 0, OpType::GreaterEqual, 1));

    ASSERT_TRUE(skip_index.CanSkipBinaryRange<std::string>(
        string_fid, 0, "a", "c", true, true));
    ASSERT_TRUE(skip_index.CanSkipBinaryRange<std::string>(
        string_fid, 0, "c", "e", true, false));
    ASSERT_FALSE(skip_index.CanSkipBinaryRange<std::string>(
        string_fid, 0, "c", "e", true, true));
    ASSERT_FALSE(skip_index.CanSkipBinaryRange<std::string>(
        string_fid, 0, "e", "k", false, true));
    ASSERT_FALSE(skip_index.CanSkipBinaryRange<std::string>(
        string_fid, 0, "j", "k", true, true));
    ASSERT_TRUE(skip_index.CanSkipBinaryRange<std::string>(
        string_fid, 0, "j", "k", false, true));
    ASSERT_FALSE(skip_index.CanSkipBinaryRange<int64_t>(
        string_fid, 0, 1, 2, false, true));
}

TEST(Sealed, QueryAllFields) {
    auto schema = std::make_shared<Schema>();
    auto metric_type = knowhere::metric::L2;
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
    auto vec = schema->AddDebugField(
        "embeddings", DataType::VECTOR_FLOAT, 128, metric_type);
    auto float16_vec = schema->AddDebugField(
        "float16_vec", DataType::VECTOR_FLOAT16, 128, metric_type);
    auto bfloat16_vec = schema->AddDebugField(
        "bfloat16_vec", DataType::VECTOR_BFLOAT16, 128, metric_type);
    schema->set_primary_field_id(int64_field);

    std::map<std::string, std::string> index_params = {
        {"index_type", "IVF_FLAT"},
        {"metric_type", metric_type},
        {"nlist", "128"}};
    std::map<std::string, std::string> type_params = {{"dim", "128"}};
    FieldIndexMeta fieldIndexMeta(
        vec, std::move(index_params), std::move(type_params));
    std::map<FieldId, FieldIndexMeta> filedMap = {{vec, fieldIndexMeta}};
    IndexMetaPtr metaPtr =
        std::make_shared<CollectionIndexMeta>(100000, std::move(filedMap));
    auto segment_sealed = CreateSealedSegment(schema, metaPtr);
    auto segment = dynamic_cast<SegmentSealedImpl*>(segment_sealed.get());

    int64_t dataset_size = 1000;
    int64_t dim = 128;
    auto dataset = DataGen(schema, dataset_size);
    SealedLoadFieldData(dataset, *segment);

    auto bool_values = dataset.get_col<bool>(bool_field);
    auto int8_values = dataset.get_col<int8_t>(int8_field);
    auto int16_values = dataset.get_col<int16_t>(int16_field);
    auto int32_values = dataset.get_col<int32_t>(int32_field);
    auto int64_values = dataset.get_col<int64_t>(int64_field);
    auto float_values = dataset.get_col<float>(float_field);
    auto double_values = dataset.get_col<double>(double_field);
    auto varchar_values = dataset.get_col<std::string>(varchar_field);
    auto json_values = dataset.get_col<std::string>(json_field);
    auto int_array_values = dataset.get_col<ScalarArray>(int_array_field);
    auto long_array_values = dataset.get_col<ScalarArray>(long_array_field);
    auto bool_array_values = dataset.get_col<ScalarArray>(bool_array_field);
    auto string_array_values = dataset.get_col<ScalarArray>(string_array_field);
    auto double_array_values = dataset.get_col<ScalarArray>(double_array_field);
    auto float_array_values = dataset.get_col<ScalarArray>(float_array_field);
    auto vector_values = dataset.get_col<float>(vec);
    auto float16_vector_values = dataset.get_col<uint8_t>(float16_vec);
    auto bfloat16_vector_values = dataset.get_col<uint8_t>(bfloat16_vec);

    auto ids_ds = GenRandomIds(dataset_size);
    auto bool_result =
        segment->bulk_subscript(bool_field, ids_ds->GetIds(), dataset_size);
    auto int8_result =
        segment->bulk_subscript(int8_field, ids_ds->GetIds(), dataset_size);
    auto int16_result =
        segment->bulk_subscript(int16_field, ids_ds->GetIds(), dataset_size);
    auto int32_result =
        segment->bulk_subscript(int32_field, ids_ds->GetIds(), dataset_size);
    auto int64_result =
        segment->bulk_subscript(int64_field, ids_ds->GetIds(), dataset_size);
    auto float_result =
        segment->bulk_subscript(float_field, ids_ds->GetIds(), dataset_size);
    auto double_result =
        segment->bulk_subscript(double_field, ids_ds->GetIds(), dataset_size);
    auto varchar_result =
        segment->bulk_subscript(varchar_field, ids_ds->GetIds(), dataset_size);
    auto json_result =
        segment->bulk_subscript(json_field, ids_ds->GetIds(), dataset_size);
    auto int_array_result = segment->bulk_subscript(
        int_array_field, ids_ds->GetIds(), dataset_size);
    auto long_array_result = segment->bulk_subscript(
        long_array_field, ids_ds->GetIds(), dataset_size);
    auto bool_array_result = segment->bulk_subscript(
        bool_array_field, ids_ds->GetIds(), dataset_size);
    auto string_array_result = segment->bulk_subscript(
        string_array_field, ids_ds->GetIds(), dataset_size);
    auto double_array_result = segment->bulk_subscript(
        double_array_field, ids_ds->GetIds(), dataset_size);
    auto float_array_result = segment->bulk_subscript(
        float_array_field, ids_ds->GetIds(), dataset_size);
    auto vec_result =
        segment->bulk_subscript(vec, ids_ds->GetIds(), dataset_size);
    auto float16_vec_result =
        segment->bulk_subscript(float16_vec, ids_ds->GetIds(), dataset_size);
    auto bfloat16_vec_result =
        segment->bulk_subscript(bfloat16_vec, ids_ds->GetIds(), dataset_size);

    EXPECT_EQ(bool_result->scalars().bool_data().data_size(), dataset_size);
    EXPECT_EQ(int8_result->scalars().int_data().data_size(), dataset_size);
    EXPECT_EQ(int16_result->scalars().int_data().data_size(), dataset_size);
    EXPECT_EQ(int32_result->scalars().int_data().data_size(), dataset_size);
    EXPECT_EQ(int64_result->scalars().long_data().data_size(), dataset_size);
    EXPECT_EQ(float_result->scalars().float_data().data_size(), dataset_size);
    EXPECT_EQ(double_result->scalars().double_data().data_size(), dataset_size);
    EXPECT_EQ(varchar_result->scalars().string_data().data_size(),
              dataset_size);
    EXPECT_EQ(json_result->scalars().json_data().data_size(), dataset_size);
    EXPECT_EQ(vec_result->vectors().float_vector().data_size(),
              dataset_size * dim);
    EXPECT_EQ(float16_vec_result->vectors().float16_vector().size(),
              dataset_size * dim * 2);
    EXPECT_EQ(bfloat16_vec_result->vectors().bfloat16_vector().size(),
              dataset_size * dim * 2);
    EXPECT_EQ(int_array_result->scalars().array_data().data_size(),
              dataset_size);
    EXPECT_EQ(long_array_result->scalars().array_data().data_size(),
              dataset_size);
    EXPECT_EQ(bool_array_result->scalars().array_data().data_size(),
              dataset_size);
    EXPECT_EQ(string_array_result->scalars().array_data().data_size(),
              dataset_size);
    EXPECT_EQ(double_array_result->scalars().array_data().data_size(),
              dataset_size);
    EXPECT_EQ(float_array_result->scalars().array_data().data_size(),
              dataset_size);

    EXPECT_EQ(bool_result->valid_data_size(), 0);
    EXPECT_EQ(int8_result->valid_data_size(), 0);
    EXPECT_EQ(int16_result->valid_data_size(), 0);
    EXPECT_EQ(int32_result->valid_data_size(), 0);
    EXPECT_EQ(int64_result->valid_data_size(), 0);
    EXPECT_EQ(float_result->valid_data_size(), 0);
    EXPECT_EQ(double_result->valid_data_size(), 0);
    EXPECT_EQ(varchar_result->valid_data_size(), 0);
    EXPECT_EQ(json_result->valid_data_size(), 0);
    EXPECT_EQ(int_array_result->valid_data_size(), 0);
    EXPECT_EQ(long_array_result->valid_data_size(), 0);
    EXPECT_EQ(bool_array_result->valid_data_size(), 0);
    EXPECT_EQ(string_array_result->valid_data_size(), 0);
    EXPECT_EQ(double_array_result->valid_data_size(), 0);
    EXPECT_EQ(float_array_result->valid_data_size(), 0);
}

TEST(Sealed, QueryAllNullableFields) {
    auto schema = std::make_shared<Schema>();
    auto metric_type = knowhere::metric::L2;
    auto bool_field = schema->AddDebugField("bool", DataType::BOOL, true);
    auto int8_field = schema->AddDebugField("int8", DataType::INT8, true);
    auto int16_field = schema->AddDebugField("int16", DataType::INT16, true);
    auto int32_field = schema->AddDebugField("int32", DataType::INT32, true);
    auto int64_field = schema->AddDebugField("int64", DataType::INT64, false);
    auto float_field = schema->AddDebugField("float", DataType::FLOAT, true);
    auto double_field = schema->AddDebugField("double", DataType::DOUBLE, true);
    auto varchar_field =
        schema->AddDebugField("varchar", DataType::VARCHAR, true);
    auto json_field = schema->AddDebugField("json", DataType::JSON, true);
    auto int_array_field = schema->AddDebugField(
        "int_array", DataType::ARRAY, DataType::INT8, true);
    auto long_array_field = schema->AddDebugField(
        "long_array", DataType::ARRAY, DataType::INT64, true);
    auto bool_array_field = schema->AddDebugField(
        "bool_array", DataType::ARRAY, DataType::BOOL, true);
    auto string_array_field = schema->AddDebugField(
        "string_array", DataType::ARRAY, DataType::VARCHAR, true);
    auto double_array_field = schema->AddDebugField(
        "double_array", DataType::ARRAY, DataType::DOUBLE, true);
    auto float_array_field = schema->AddDebugField(
        "float_array", DataType::ARRAY, DataType::FLOAT, true);
    auto vec = schema->AddDebugField(
        "embeddings", DataType::VECTOR_FLOAT, 128, metric_type);
    schema->set_primary_field_id(int64_field);

    std::map<std::string, std::string> index_params = {
        {"index_type", "IVF_FLAT"},
        {"metric_type", metric_type},
        {"nlist", "128"}};
    std::map<std::string, std::string> type_params = {{"dim", "128"}};
    FieldIndexMeta fieldIndexMeta(
        vec, std::move(index_params), std::move(type_params));
    std::map<FieldId, FieldIndexMeta> filedMap = {{vec, fieldIndexMeta}};
    IndexMetaPtr metaPtr =
        std::make_shared<CollectionIndexMeta>(100000, std::move(filedMap));
    auto segment_sealed = CreateSealedSegment(schema, metaPtr);
    auto segment = dynamic_cast<SegmentSealedImpl*>(segment_sealed.get());

    int64_t dataset_size = 1000;
    int64_t dim = 128;
    auto dataset = DataGen(schema, dataset_size);
    SealedLoadFieldData(dataset, *segment);

    auto bool_values = dataset.get_col<bool>(bool_field);
    auto int8_values = dataset.get_col<int8_t>(int8_field);
    auto int16_values = dataset.get_col<int16_t>(int16_field);
    auto int32_values = dataset.get_col<int32_t>(int32_field);
    auto int64_values = dataset.get_col<int64_t>(int64_field);
    auto float_values = dataset.get_col<float>(float_field);
    auto double_values = dataset.get_col<double>(double_field);
    auto varchar_values = dataset.get_col<std::string>(varchar_field);
    auto json_values = dataset.get_col<std::string>(json_field);
    auto int_array_values = dataset.get_col<ScalarArray>(int_array_field);
    auto long_array_values = dataset.get_col<ScalarArray>(long_array_field);
    auto bool_array_values = dataset.get_col<ScalarArray>(bool_array_field);
    auto string_array_values = dataset.get_col<ScalarArray>(string_array_field);
    auto double_array_values = dataset.get_col<ScalarArray>(double_array_field);
    auto float_array_values = dataset.get_col<ScalarArray>(float_array_field);
    auto vector_values = dataset.get_col<float>(vec);

    auto bool_valid_values = dataset.get_col_valid(bool_field);
    auto int8_valid_values = dataset.get_col_valid(int8_field);
    auto int16_valid_values = dataset.get_col_valid(int16_field);
    auto int32_valid_values = dataset.get_col_valid(int32_field);
    auto float_valid_values = dataset.get_col_valid(float_field);
    auto double_valid_values = dataset.get_col_valid(double_field);
    auto varchar_valid_values = dataset.get_col_valid(varchar_field);
    auto json_valid_values = dataset.get_col_valid(json_field);
    auto int_array_valid_values = dataset.get_col_valid(int_array_field);
    auto long_array_valid_values = dataset.get_col_valid(long_array_field);
    auto bool_array_valid_values = dataset.get_col_valid(bool_array_field);
    auto string_array_valid_values = dataset.get_col_valid(string_array_field);
    auto double_array_valid_values = dataset.get_col_valid(double_array_field);
    auto float_array_valid_values = dataset.get_col_valid(float_array_field);

    auto ids_ds = GenRandomIds(dataset_size);
    auto bool_result =
        segment->bulk_subscript(bool_field, ids_ds->GetIds(), dataset_size);
    auto int8_result =
        segment->bulk_subscript(int8_field, ids_ds->GetIds(), dataset_size);
    auto int16_result =
        segment->bulk_subscript(int16_field, ids_ds->GetIds(), dataset_size);
    auto int32_result =
        segment->bulk_subscript(int32_field, ids_ds->GetIds(), dataset_size);
    auto int64_result =
        segment->bulk_subscript(int64_field, ids_ds->GetIds(), dataset_size);
    auto float_result =
        segment->bulk_subscript(float_field, ids_ds->GetIds(), dataset_size);
    auto double_result =
        segment->bulk_subscript(double_field, ids_ds->GetIds(), dataset_size);
    auto varchar_result =
        segment->bulk_subscript(varchar_field, ids_ds->GetIds(), dataset_size);
    auto json_result =
        segment->bulk_subscript(json_field, ids_ds->GetIds(), dataset_size);
    auto int_array_result = segment->bulk_subscript(
        int_array_field, ids_ds->GetIds(), dataset_size);
    auto long_array_result = segment->bulk_subscript(
        long_array_field, ids_ds->GetIds(), dataset_size);
    auto bool_array_result = segment->bulk_subscript(
        bool_array_field, ids_ds->GetIds(), dataset_size);
    auto string_array_result = segment->bulk_subscript(
        string_array_field, ids_ds->GetIds(), dataset_size);
    auto double_array_result = segment->bulk_subscript(
        double_array_field, ids_ds->GetIds(), dataset_size);
    auto float_array_result = segment->bulk_subscript(
        float_array_field, ids_ds->GetIds(), dataset_size);
    auto vec_result =
        segment->bulk_subscript(vec, ids_ds->GetIds(), dataset_size);

    EXPECT_EQ(bool_result->scalars().bool_data().data_size(), dataset_size);
    EXPECT_EQ(int8_result->scalars().int_data().data_size(), dataset_size);
    EXPECT_EQ(int16_result->scalars().int_data().data_size(), dataset_size);
    EXPECT_EQ(int32_result->scalars().int_data().data_size(), dataset_size);
    EXPECT_EQ(int64_result->scalars().long_data().data_size(), dataset_size);
    EXPECT_EQ(float_result->scalars().float_data().data_size(), dataset_size);
    EXPECT_EQ(double_result->scalars().double_data().data_size(), dataset_size);
    EXPECT_EQ(varchar_result->scalars().string_data().data_size(),
              dataset_size);
    EXPECT_EQ(json_result->scalars().json_data().data_size(), dataset_size);
    EXPECT_EQ(vec_result->vectors().float_vector().data_size(),
              dataset_size * dim);
    EXPECT_EQ(int_array_result->scalars().array_data().data_size(),
              dataset_size);
    EXPECT_EQ(long_array_result->scalars().array_data().data_size(),
              dataset_size);
    EXPECT_EQ(bool_array_result->scalars().array_data().data_size(),
              dataset_size);
    EXPECT_EQ(string_array_result->scalars().array_data().data_size(),
              dataset_size);
    EXPECT_EQ(double_array_result->scalars().array_data().data_size(),
              dataset_size);
    EXPECT_EQ(float_array_result->scalars().array_data().data_size(),
              dataset_size);

    EXPECT_EQ(bool_result->valid_data_size(), dataset_size);
    EXPECT_EQ(int8_result->valid_data_size(), dataset_size);
    EXPECT_EQ(int16_result->valid_data_size(), dataset_size);
    EXPECT_EQ(int32_result->valid_data_size(), dataset_size);
    EXPECT_EQ(float_result->valid_data_size(), dataset_size);
    EXPECT_EQ(double_result->valid_data_size(), dataset_size);
    EXPECT_EQ(varchar_result->valid_data_size(), dataset_size);
    EXPECT_EQ(json_result->valid_data_size(), dataset_size);
    EXPECT_EQ(int_array_result->valid_data_size(), dataset_size);
    EXPECT_EQ(long_array_result->valid_data_size(), dataset_size);
    EXPECT_EQ(bool_array_result->valid_data_size(), dataset_size);
    EXPECT_EQ(string_array_result->valid_data_size(), dataset_size);
    EXPECT_EQ(double_array_result->valid_data_size(), dataset_size);
    EXPECT_EQ(float_array_result->valid_data_size(), dataset_size);
}
