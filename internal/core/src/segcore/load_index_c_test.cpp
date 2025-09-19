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

#include "segcore/collection_c.h"
#include "segcore/segment_c.h"
#include "segcore/reduce_c.h"

#include "test_utils/c_api_test_utils.h"
#include "test_utils/storage_test_utils.h"
#include "test_utils/cachinglayer_test_utils.h"
#include "test_utils/GenExprProto.h"

using namespace milvus;
using namespace milvus::segcore;
using namespace milvus::index;
using namespace milvus::test;
using namespace knowhere;

const int64_t ROW_COUNT = 10 * 1000;
const int64_t BIAS = 4200;

auto
generate_data(int N) {
    std::vector<char> raw_data;
    std::vector<uint64_t> timestamps;
    std::vector<int64_t> uids;
    std::default_random_engine e(42);
    std::normal_distribution<> dis(0.0, 1.0);
    for (int i = 0; i < N; ++i) {
        uids.push_back(10 * N + i);
        timestamps.push_back(0);
        float vec[DIM];
        for (auto& x : vec) {
            x = dis(e);
        }
        raw_data.insert(raw_data.end(),
                        (const char*)std::begin(vec),
                        (const char*)std::end(vec));
        int age = e() % 100;
        raw_data.insert(raw_data.end(),
                        (const char*)&age,
                        ((const char*)&age) + sizeof(age));
    }
    return std::make_tuple(raw_data, timestamps, uids);
}

TEST(CApiTest, LoadIndexInfo) {
    // generator index
    constexpr auto TOPK = 10;

    auto N = 1024 * 10;
    auto [raw_data, timestamps, uids] = generate_data(N);
    auto get_index_obj = knowhere::IndexFactory::Instance().Create<float>(
        knowhere::IndexEnum::INDEX_FAISS_IVFSQ8,
        knowhere::Version::GetCurrentVersion().VersionNumber());
    auto indexing = get_index_obj.value();
    auto conf =
        knowhere::Json{{knowhere::meta::METRIC_TYPE, knowhere::metric::L2},
                       {knowhere::meta::DIM, DIM},
                       {knowhere::meta::TOPK, TOPK},
                       {knowhere::indexparam::NLIST, 100},
                       {knowhere::indexparam::NPROBE, 4}};

    auto database = knowhere::GenDataSet(N, DIM, raw_data.data());
    indexing.Train(database, conf);
    indexing.Add(database, conf);
    EXPECT_EQ(indexing.Count(), N);
    EXPECT_EQ(indexing.Dim(), DIM);
    knowhere::BinarySet binary_set;
    indexing.Serialize(binary_set);
    CBinarySet c_binary_set = (CBinarySet)&binary_set;

    void* c_load_index_info = nullptr;
    auto status = NewLoadIndexInfo(&c_load_index_info);
    ASSERT_EQ(status.error_code, Success);
    std::string index_param_key1 = "index_type";
    std::string index_param_value1 = knowhere::IndexEnum::INDEX_FAISS_IVFSQ8;
    status = AppendIndexParam(
        c_load_index_info, index_param_key1.data(), index_param_value1.data());
    std::string index_param_key2 = knowhere::meta::METRIC_TYPE;
    std::string index_param_value2 = knowhere::metric::L2;
    status = AppendIndexParam(
        c_load_index_info, index_param_key2.data(), index_param_value2.data());
    ASSERT_EQ(status.error_code, Success);
    std::string field_name = "field0";
    status = AppendFieldInfoForTest(
        c_load_index_info, 0, 0, 0, 0, CDataType::FloatVector, false, "");
    ASSERT_EQ(status.error_code, Success);
    AppendIndexEngineVersionToLoadInfo(
        c_load_index_info,
        knowhere::Version::GetCurrentVersion().VersionNumber());
    status = AppendIndex(c_load_index_info, c_binary_set);
    ASSERT_EQ(status.error_code, Success);
    DeleteLoadIndexInfo(c_load_index_info);
}

TEST(CApiTest, LoadIndexSearch) {
    // generator index
    constexpr auto TOPK = 10;

    auto N = 1024 * 10;
    auto num_query = 100;
    auto [raw_data, timestamps, uids] = generate_data(N);
    auto get_index_obj = knowhere::IndexFactory::Instance().Create<float>(
        knowhere::IndexEnum::INDEX_FAISS_IVFSQ8,
        knowhere::Version::GetCurrentVersion().VersionNumber());
    auto indexing = get_index_obj.value();
    auto conf =
        knowhere::Json{{knowhere::meta::METRIC_TYPE, knowhere::metric::L2},
                       {knowhere::meta::DIM, DIM},
                       {knowhere::meta::TOPK, TOPK},
                       {knowhere::indexparam::NLIST, 100},
                       {knowhere::indexparam::NPROBE, 4}};

    auto database = knowhere::GenDataSet(N, DIM, raw_data.data());
    indexing.Train(database, conf);
    indexing.Add(database, conf);

    EXPECT_EQ(indexing.Count(), N);
    EXPECT_EQ(indexing.Dim(), DIM);

    // serializ index to binarySet
    knowhere::BinarySet binary_set;
    indexing.Serialize(binary_set);

    // fill loadIndexInfo
    milvus::segcore::LoadIndexInfo load_index_info;
    auto& index_params = load_index_info.index_params;
    index_params["index_type"] = knowhere::IndexEnum::INDEX_FAISS_IVFSQ8;
    auto index = std::make_unique<VectorMemIndex<float>>(
        DataType::NONE,
        index_params["index_type"],
        knowhere::metric::L2,
        knowhere::Version::GetCurrentVersion().VersionNumber());
    index->Load(binary_set);
    index_params = GenIndexParams(index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(index));

    // search
    auto query_dataset =
        knowhere::GenDataSet(num_query, DIM, raw_data.data() + BIAS * DIM);

    auto result = indexing.Search(query_dataset, conf, nullptr);
}

template <class TraitType>
void
Test_Indexing_Without_Predicate() {
    GET_ELEM_TYPE_FOR_VECTOR_TRAIT

    // insert data to segment
    constexpr auto TOPK = 5;

    std::string schema_string =
        generate_collection_schema<TraitType>(knowhere::metric::L2, DIM);
    auto collection = NewCollection(schema_string.c_str());
    auto schema = ((segcore::Collection*)collection)->get_schema();
    CSegmentInterface segment;
    auto status = NewSegment(collection, Growing, -1, &segment, false);
    ASSERT_EQ(status.error_code, Success);

    auto N = ROW_COUNT;
    auto dataset = DataGen(schema, N);
    auto vec_col = dataset.get_col<elem_type>(FieldId(100));
    auto query_ptr = vec_col.data() + BIAS * DIM;

    int64_t offset;
    PreInsert(segment, N, &offset);

    auto insert_data = serialize(dataset.raw_);
    auto ins_res = Insert(segment,
                          offset,
                          N,
                          dataset.row_ids_.data(),
                          dataset.timestamps_.data(),
                          insert_data.data(),
                          insert_data.size());
    ASSERT_EQ(ins_res.error_code, Success);

    milvus::proto::plan::PlanNode plan_node;
    auto vector_anns = plan_node.mutable_vector_anns();
    vector_anns->set_vector_type(TraitType::vector_type);
    vector_anns->set_placeholder_tag("$0");
    vector_anns->set_field_id(100);
    auto query_info = vector_anns->mutable_query_info();
    query_info->set_topk(5);
    query_info->set_round_decimal(-1);
    query_info->set_metric_type("L2");
    query_info->set_search_params(R"({"nprobe": 10})");
    auto plan_str = plan_node.SerializeAsString();

    // create place_holder_group
    int num_queries = 5;
    auto raw_group =
        CreatePlaceholderGroupFromBlob<TraitType>(num_queries, DIM, query_ptr);
    auto blob = raw_group.SerializeAsString();

    // search on segment's small index
    void* plan = nullptr;
    status = CreateSearchPlanByExpr(
        collection, plan_str.data(), plan_str.size(), &plan);
    ASSERT_EQ(status.error_code, Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(
        plan, blob.data(), blob.length(), &placeholderGroup);
    ASSERT_EQ(status.error_code, Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);

    Timestamp timestmap = 10000000;

    CSearchResult c_search_result_on_smallIndex;
    auto res_before_load_index = CSearch(segment,
                                         plan,
                                         placeholderGroup,
                                         timestmap,
                                         &c_search_result_on_smallIndex);
    ASSERT_EQ(res_before_load_index.error_code, Success);

    // load index to segment
    auto indexing = generate_index(vec_col.data(),
                                   TraitType::data_type,
                                   knowhere::metric::L2,
                                   IndexEnum::INDEX_FAISS_IVFSQ8,
                                   DIM,
                                   N);

    // gen query dataset
    auto query_dataset = knowhere::GenDataSet(num_queries, DIM, query_ptr);
    auto vec_index = dynamic_cast<VectorIndex*>(indexing.get());
    auto search_plan = reinterpret_cast<milvus::query::Plan*>(plan);
    SearchInfo search_info = search_plan->plan_node_->search_info_;
    SearchResult result_on_index;
    vec_index->Query(
        query_dataset, search_info, nullptr, nullptr, result_on_index);
    auto ids = result_on_index.seg_offsets_.data();
    auto dis = result_on_index.distances_.data();
    std::vector<int64_t> vec_ids(ids, ids + TOPK * num_queries);
    std::vector<float> vec_dis;
    for (int j = 0; j < TOPK * num_queries; ++j) {
        vec_dis.push_back(dis[j] * -1);
    }

    auto search_result_on_raw_index =
        (SearchResult*)c_search_result_on_smallIndex;
    search_result_on_raw_index->seg_offsets_ = vec_ids;
    search_result_on_raw_index->distances_ = vec_dis;

    auto binary_set = indexing->Serialize(milvus::Config{});
    void* c_load_index_info = nullptr;
    status = NewLoadIndexInfo(&c_load_index_info);
    ASSERT_EQ(status.error_code, Success);
    std::string index_type_key = "index_type";
    std::string index_type_value = IndexEnum::INDEX_FAISS_IVFSQ8;
    std::string metric_type_key = "metric_type";
    std::string metric_type_value = knowhere::metric::L2;

    AppendIndexParam(
        c_load_index_info, index_type_key.c_str(), index_type_value.c_str());
    AppendIndexParam(
        c_load_index_info, metric_type_key.c_str(), metric_type_value.c_str());
    AppendFieldInfoForTest(
        c_load_index_info, 0, 0, 0, 100, TraitType::c_data_type, false, "");
    AppendIndexEngineVersionToLoadInfo(
        c_load_index_info,
        knowhere::Version::GetCurrentVersion().VersionNumber());
    AppendIndex(c_load_index_info, (CBinarySet)&binary_set);
    // load index for vec field, load raw data for scalar field
    auto sealed_segment = CreateSealedWithFieldDataLoaded(schema, dataset);
    sealed_segment->DropFieldData(FieldId(100));
    sealed_segment->LoadIndex(*(LoadIndexInfo*)c_load_index_info);

    CSearchResult c_search_result_on_bigIndex;
    auto res_after_load_index = CSearch(sealed_segment.get(),
                                        plan,
                                        placeholderGroup,
                                        timestmap,
                                        &c_search_result_on_bigIndex);
    ASSERT_EQ(res_after_load_index.error_code, Success);

    auto search_result_on_raw_index_json =
        SearchResultToJson(*search_result_on_raw_index);
    auto search_result_on_bigIndex_json =
        SearchResultToJson((*(SearchResult*)c_search_result_on_bigIndex));

    ASSERT_EQ(search_result_on_raw_index_json.dump(1),
              search_result_on_bigIndex_json.dump(1));

    DeleteLoadIndexInfo(c_load_index_info);
    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchResult(c_search_result_on_smallIndex);
    DeleteSearchResult(c_search_result_on_bigIndex);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, Indexing_Without_Predicate) {
    Test_Indexing_Without_Predicate<milvus::FloatVector>();
    Test_Indexing_Without_Predicate<milvus::Float16Vector>();
    Test_Indexing_Without_Predicate<milvus::BFloat16Vector>();
    Test_Indexing_Without_Predicate<milvus::Int8Vector>();
}

TEST(CApiTest, Indexing_Expr_Without_Predicate) {
    // insert data to segment
    constexpr auto TOPK = 5;

    std::string schema_string = generate_collection_schema<milvus::FloatVector>(
        knowhere::metric::L2, DIM);
    auto collection = NewCollection(schema_string.c_str());
    auto schema = ((segcore::Collection*)collection)->get_schema();
    CSegmentInterface segment;
    auto status = NewSegment(collection, Growing, -1, &segment, false);
    ASSERT_EQ(status.error_code, Success);

    auto N = ROW_COUNT;
    auto dataset = DataGen(schema, N);
    auto vec_col = dataset.get_col<float>(FieldId(100));
    auto query_ptr = vec_col.data() + BIAS * DIM;

    int64_t offset;
    PreInsert(segment, N, &offset);

    auto insert_data = serialize(dataset.raw_);
    auto ins_res = Insert(segment,
                          offset,
                          N,
                          dataset.row_ids_.data(),
                          dataset.timestamps_.data(),
                          insert_data.data(),
                          insert_data.size());
    ASSERT_EQ(ins_res.error_code, Success);

    const char* serialized_expr_plan = R"(vector_anns: <
                                             field_id: 100
                                             query_info: <
                                                 topk: 5
                                                 round_decimal: -1
                                                 metric_type: "L2"
                                                 search_params: "{\"nprobe\": 10}"
                                             >
                                             placeholder_tag: "$0"
                                          >)";

    // create place_holder_group
    int num_queries = 5;
    auto raw_group =
        CreatePlaceholderGroupFromBlob(num_queries, DIM, query_ptr);
    auto blob = raw_group.SerializeAsString();

    // search on segment's small index
    void* plan = nullptr;
    auto binary_plan = translate_text_plan_to_binary_plan(serialized_expr_plan);
    status = CreateSearchPlanByExpr(
        collection, binary_plan.data(), binary_plan.size(), &plan);
    ASSERT_EQ(status.error_code, Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(
        plan, blob.data(), blob.length(), &placeholderGroup);
    ASSERT_EQ(status.error_code, Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);

    Timestamp timestamp = 10000000;

    CSearchResult c_search_result_on_smallIndex;
    auto res_before_load_index = CSearch(segment,
                                         plan,
                                         placeholderGroup,
                                         timestamp,
                                         &c_search_result_on_smallIndex);
    ASSERT_EQ(res_before_load_index.error_code, Success);

    // load index to segment
    auto indexing = generate_index(vec_col.data(),
                                   DataType::VECTOR_FLOAT,
                                   knowhere::metric::L2,
                                   IndexEnum::INDEX_FAISS_IVFSQ8,
                                   DIM,
                                   N);

    // gen query dataset
    auto query_dataset = knowhere::GenDataSet(num_queries, DIM, query_ptr);
    auto vec_index = dynamic_cast<VectorIndex*>(indexing.get());
    auto search_plan = reinterpret_cast<milvus::query::Plan*>(plan);
    SearchInfo search_info = search_plan->plan_node_->search_info_;
    SearchResult result_on_index;
    vec_index->Query(
        query_dataset, search_info, nullptr, nullptr, result_on_index);
    auto ids = result_on_index.seg_offsets_.data();
    auto dis = result_on_index.distances_.data();
    std::vector<int64_t> vec_ids(ids, ids + TOPK * num_queries);
    std::vector<float> vec_dis;
    for (int j = 0; j < TOPK * num_queries; ++j) {
        vec_dis.push_back(dis[j] * -1);
    }

    auto search_result_on_raw_index =
        (SearchResult*)c_search_result_on_smallIndex;
    search_result_on_raw_index->seg_offsets_ = vec_ids;
    search_result_on_raw_index->distances_ = vec_dis;

    auto binary_set = indexing->Serialize(milvus::Config{});
    void* c_load_index_info = nullptr;
    status = NewLoadIndexInfo(&c_load_index_info);
    ASSERT_EQ(status.error_code, Success);
    std::string index_type_key = "index_type";
    std::string index_type_value = IndexEnum::INDEX_FAISS_IVFSQ8;
    std::string metric_type_key = "metric_type";
    std::string metric_type_value = knowhere::metric::L2;

    AppendIndexParam(
        c_load_index_info, index_type_key.c_str(), index_type_value.c_str());
    AppendIndexParam(
        c_load_index_info, metric_type_key.c_str(), metric_type_value.c_str());
    AppendFieldInfoForTest(
        c_load_index_info, 0, 0, 0, 100, CDataType::FloatVector, false, "");
    AppendIndexEngineVersionToLoadInfo(
        c_load_index_info,
        knowhere::Version::GetCurrentVersion().VersionNumber());
    AppendIndex(c_load_index_info, (CBinarySet)&binary_set);

    // load index for vec field, load raw data for scalar field
    auto sealed_segment = CreateSealedWithFieldDataLoaded(schema, dataset);
    sealed_segment->DropFieldData(FieldId(100));
    sealed_segment->LoadIndex(*(LoadIndexInfo*)c_load_index_info);

    CSearchResult c_search_result_on_bigIndex;
    auto res_after_load_index = CSearch(sealed_segment.get(),
                                        plan,
                                        placeholderGroup,
                                        timestamp,
                                        &c_search_result_on_bigIndex);
    ASSERT_EQ(res_after_load_index.error_code, Success);

    auto search_result_on_raw_index_json =
        SearchResultToJson(*search_result_on_raw_index);
    auto search_result_on_bigIndex_json =
        SearchResultToJson((*(SearchResult*)c_search_result_on_bigIndex));

    ASSERT_EQ(search_result_on_raw_index_json.dump(1),
              search_result_on_bigIndex_json.dump(1));

    DeleteLoadIndexInfo(c_load_index_info);
    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchResult(c_search_result_on_smallIndex);
    DeleteSearchResult(c_search_result_on_bigIndex);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, Indexing_With_float_Predicate_Range) {
    // insert data to segment
    constexpr auto TOPK = 5;

    std::string schema_string = generate_collection_schema<milvus::FloatVector>(
        knowhere::metric::L2, DIM);
    auto collection = NewCollection(schema_string.c_str());
    auto schema = ((segcore::Collection*)collection)->get_schema();
    CSegmentInterface segment;
    auto status = NewSegment(collection, Growing, -1, &segment, false);
    ASSERT_EQ(status.error_code, Success);

    auto N = ROW_COUNT;
    auto dataset = DataGen(schema, N);
    auto vec_col = dataset.get_col<float>(FieldId(100));
    auto query_ptr = vec_col.data() + BIAS * DIM;

    int64_t offset;
    PreInsert(segment, N, &offset);

    auto insert_data = serialize(dataset.raw_);
    auto ins_res = Insert(segment,
                          offset,
                          N,
                          dataset.row_ids_.data(),
                          dataset.timestamps_.data(),
                          insert_data.data(),
                          insert_data.size());
    ASSERT_EQ(ins_res.error_code, Success);

    const char* raw_plan = R"(vector_anns: <
                                field_id: 100
                                predicates: <
                                  binary_expr: <
                                    op: LogicalAnd
                                    left: <
                                      unary_range_expr: <
                                        column_info: <
                                          field_id: 101
                                          data_type: Int64
                                        >
                                        op: GreaterEqual
                                        value: <
                                          int64_val: 4200
                                        >
                                      >
                                    >
                                    right: <
                                      unary_range_expr: <
                                        column_info: <
                                          field_id: 101
                                          data_type: Int64
                                        >
                                        op: LessThan
                                        value: <
                                          int64_val: 4210
                                        >
                                      >
                                    >
                                  >
                                >
                                query_info: <
                                  topk: 5
                                  round_decimal: -1
                                  metric_type: "L2"
                                  search_params: "{\"nprobe\": 10}"
                                >
                                placeholder_tag: "$0"
     >)";
    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);

    // create place_holder_group
    int num_queries = 10;
    auto raw_group =
        CreatePlaceholderGroupFromBlob(num_queries, DIM, query_ptr);
    auto blob = raw_group.SerializeAsString();

    // search on segment's small index
    void* plan = nullptr;
    status = CreateSearchPlanByExpr(
        collection, plan_str.data(), plan_str.size(), &plan);
    ASSERT_EQ(status.error_code, Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(
        plan, blob.data(), blob.length(), &placeholderGroup);
    ASSERT_EQ(status.error_code, Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    Timestamp timestamp = 10000000;

    CSearchResult c_search_result_on_smallIndex;
    auto res_before_load_index = CSearch(segment,
                                         plan,
                                         placeholderGroup,
                                         timestamp,
                                         &c_search_result_on_smallIndex);
    ASSERT_EQ(res_before_load_index.error_code, Success);

    // load index to segment
    auto indexing = generate_index(vec_col.data(),
                                   DataType::VECTOR_FLOAT,
                                   knowhere::metric::L2,
                                   IndexEnum::INDEX_FAISS_IVFSQ8,
                                   DIM,
                                   N);

    // gen query dataset
    auto query_dataset = knowhere::GenDataSet(num_queries, DIM, query_ptr);
    auto vec_index = dynamic_cast<VectorIndex*>(indexing.get());
    auto search_plan = reinterpret_cast<milvus::query::Plan*>(plan);
    SearchInfo search_info = search_plan->plan_node_->search_info_;
    SearchResult result_on_index;
    vec_index->Query(
        query_dataset, search_info, nullptr, nullptr, result_on_index);
    auto ids = result_on_index.seg_offsets_.data();
    auto dis = result_on_index.distances_.data();
    std::vector<int64_t> vec_ids(ids, ids + TOPK * num_queries);
    std::vector<float> vec_dis;
    for (int j = 0; j < TOPK * num_queries; ++j) {
        vec_dis.push_back(dis[j] * -1);
    }

    auto search_result_on_raw_index =
        (SearchResult*)c_search_result_on_smallIndex;
    search_result_on_raw_index->seg_offsets_ = vec_ids;
    search_result_on_raw_index->distances_ = vec_dis;

    auto binary_set = indexing->Serialize(milvus::Config{});
    void* c_load_index_info = nullptr;
    status = NewLoadIndexInfo(&c_load_index_info);
    ASSERT_EQ(status.error_code, Success);
    std::string index_type_key = "index_type";
    std::string index_type_value = IndexEnum::INDEX_FAISS_IVFSQ8;
    std::string metric_type_key = "metric_type";
    std::string metric_type_value = knowhere::metric::L2;

    AppendIndexParam(
        c_load_index_info, index_type_key.c_str(), index_type_value.c_str());
    AppendIndexParam(
        c_load_index_info, metric_type_key.c_str(), metric_type_value.c_str());
    AppendFieldInfoForTest(
        c_load_index_info, 0, 0, 0, 100, CDataType::FloatVector, false, "");
    AppendIndexEngineVersionToLoadInfo(
        c_load_index_info,
        knowhere::Version::GetCurrentVersion().VersionNumber());
    AppendIndex(c_load_index_info, (CBinarySet)&binary_set);

    // load index for vec field, load raw data for scalar field
    auto sealed_segment = CreateSealedWithFieldDataLoaded(schema, dataset);
    sealed_segment->DropFieldData(FieldId(100));
    sealed_segment->LoadIndex(*(LoadIndexInfo*)c_load_index_info);

    CSearchResult c_search_result_on_bigIndex;
    auto res_after_load_index = CSearch(sealed_segment.get(),
                                        plan,
                                        placeholderGroup,
                                        timestamp,
                                        &c_search_result_on_bigIndex);
    ASSERT_EQ(res_after_load_index.error_code, Success);

    auto search_result_on_bigIndex = (SearchResult*)c_search_result_on_bigIndex;
    for (int i = 0; i < num_queries; ++i) {
        auto offset = i * TOPK;
        ASSERT_EQ(search_result_on_bigIndex->seg_offsets_[offset], BIAS + i);
        ASSERT_EQ(search_result_on_bigIndex->distances_[offset],
                  search_result_on_raw_index->distances_[offset]);
    }

    DeleteLoadIndexInfo(c_load_index_info);
    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchResult(c_search_result_on_smallIndex);
    DeleteSearchResult(c_search_result_on_bigIndex);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, Indexing_Expr_With_float_Predicate_Range) {
    // insert data to segment
    constexpr auto TOPK = 5;

    std::string schema_string = generate_collection_schema<milvus::FloatVector>(
        knowhere::metric::L2, DIM);
    auto collection = NewCollection(schema_string.c_str());
    auto schema = ((segcore::Collection*)collection)->get_schema();
    CSegmentInterface segment;
    auto status = NewSegment(collection, Growing, -1, &segment, false);
    ASSERT_EQ(status.error_code, Success);

    auto N = 1000 * 10;
    auto dataset = DataGen(schema, N);
    auto vec_col = dataset.get_col<float>(FieldId(100));
    auto query_ptr = vec_col.data() + BIAS * DIM;

    {
        int64_t offset;
        PreInsert(segment, N, &offset);

        auto insert_data = serialize(dataset.raw_);
        auto ins_res = Insert(segment,
                              offset,
                              N,
                              dataset.row_ids_.data(),
                              dataset.timestamps_.data(),
                              insert_data.data(),
                              insert_data.size());
        ASSERT_EQ(ins_res.error_code, Success);
    }

    const char* serialized_expr_plan = R"(vector_anns: <
                                             field_id: 100
                                             predicates: <
                                               binary_expr: <
                                                 op: LogicalAnd
                                                 left: <
                                                   unary_range_expr: <
                                                     column_info: <
                                                       field_id: 101
                                                       data_type: Int64
                                                     >
                                                     op: GreaterEqual
                                                     value: <
                                                       int64_val: 4200
                                                     >
                                                   >
                                                 >
                                                 right: <
                                                   unary_range_expr: <
                                                     column_info: <
                                                       field_id: 101
                                                       data_type: Int64
                                                     >
                                                     op: LessThan
                                                     value: <
                                                       int64_val: 4210
                                                     >
                                                   >
                                                 >
                                               >
                                             >
                                             query_info: <
                                               topk: 5
                                               round_decimal: -1
                                               metric_type: "L2"
                                               search_params: "{\"nprobe\": 10}"
                                             >
                                             placeholder_tag: "$0"
     >)";

    // create place_holder_group
    int num_queries = 10;
    auto raw_group =
        CreatePlaceholderGroupFromBlob(num_queries, DIM, query_ptr);
    auto blob = raw_group.SerializeAsString();

    // search on segment's small index
    void* plan = nullptr;
    auto binary_plan = translate_text_plan_to_binary_plan(serialized_expr_plan);
    status = CreateSearchPlanByExpr(
        collection, binary_plan.data(), binary_plan.size(), &plan);
    ASSERT_EQ(status.error_code, Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(
        plan, blob.data(), blob.length(), &placeholderGroup);
    ASSERT_EQ(status.error_code, Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    Timestamp timestamp = 10000000;

    CSearchResult c_search_result_on_smallIndex;
    auto res_before_load_index = CSearch(segment,
                                         plan,
                                         placeholderGroup,
                                         timestamp,
                                         &c_search_result_on_smallIndex);
    ASSERT_EQ(res_before_load_index.error_code, Success);

    // load index to segment
    auto indexing = generate_index(vec_col.data(),
                                   DataType::VECTOR_FLOAT,
                                   knowhere::metric::L2,
                                   IndexEnum::INDEX_FAISS_IVFSQ8,
                                   DIM,
                                   N);

    // gen query dataset
    auto query_dataset = knowhere::GenDataSet(num_queries, DIM, query_ptr);
    auto vec_index = dynamic_cast<VectorIndex*>(indexing.get());
    auto search_plan = reinterpret_cast<milvus::query::Plan*>(plan);
    SearchInfo search_info = search_plan->plan_node_->search_info_;
    SearchResult result_on_index;
    vec_index->Query(
        query_dataset, search_info, nullptr, nullptr, result_on_index);
    auto ids = result_on_index.seg_offsets_.data();
    auto dis = result_on_index.distances_.data();
    std::vector<int64_t> vec_ids(ids, ids + TOPK * num_queries);
    std::vector<float> vec_dis;
    for (int j = 0; j < TOPK * num_queries; ++j) {
        vec_dis.push_back(dis[j] * -1);
    }

    auto search_result_on_raw_index =
        (SearchResult*)c_search_result_on_smallIndex;
    search_result_on_raw_index->seg_offsets_ = vec_ids;
    search_result_on_raw_index->distances_ = vec_dis;

    auto binary_set = indexing->Serialize(milvus::Config{});
    void* c_load_index_info = nullptr;
    status = NewLoadIndexInfo(&c_load_index_info);
    ASSERT_EQ(status.error_code, Success);
    std::string index_type_key = "index_type";
    std::string index_type_value = IndexEnum::INDEX_FAISS_IVFSQ8;
    std::string metric_type_key = "metric_type";
    std::string metric_type_value = knowhere::metric::L2;

    AppendIndexParam(
        c_load_index_info, index_type_key.c_str(), index_type_value.c_str());
    AppendIndexParam(
        c_load_index_info, metric_type_key.c_str(), metric_type_value.c_str());
    AppendFieldInfoForTest(
        c_load_index_info, 0, 0, 0, 100, CDataType::FloatVector, false, "");
    AppendIndexEngineVersionToLoadInfo(
        c_load_index_info,
        knowhere::Version::GetCurrentVersion().VersionNumber());
    AppendIndex(c_load_index_info, (CBinarySet)&binary_set);

    // load index for vec field, load raw data for scalar field
    auto sealed_segment = CreateSealedWithFieldDataLoaded(schema, dataset);
    sealed_segment->DropFieldData(FieldId(100));
    sealed_segment->LoadIndex(*(LoadIndexInfo*)c_load_index_info);

    CSearchResult c_search_result_on_bigIndex;
    auto res_after_load_index = CSearch(sealed_segment.get(),
                                        plan,
                                        placeholderGroup,
                                        timestamp,
                                        &c_search_result_on_bigIndex);
    ASSERT_EQ(res_after_load_index.error_code, Success);

    auto search_result_on_bigIndex = (SearchResult*)c_search_result_on_bigIndex;
    for (int i = 0; i < num_queries; ++i) {
        auto offset = i * TOPK;
        ASSERT_EQ(search_result_on_bigIndex->seg_offsets_[offset], BIAS + i);
        ASSERT_EQ(search_result_on_bigIndex->distances_[offset],
                  search_result_on_raw_index->distances_[offset]);
    }

    DeleteLoadIndexInfo(c_load_index_info);
    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchResult(c_search_result_on_smallIndex);
    DeleteSearchResult(c_search_result_on_bigIndex);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, Indexing_With_float_Predicate_Term) {
    // insert data to segment
    constexpr auto TOPK = 5;

    std::string schema_string = generate_collection_schema<milvus::FloatVector>(
        knowhere::metric::L2, DIM);
    auto collection = NewCollection(schema_string.c_str());
    auto schema = ((segcore::Collection*)collection)->get_schema();
    CSegmentInterface segment;
    auto status = NewSegment(collection, Growing, -1, &segment, false);
    ASSERT_EQ(status.error_code, Success);

    auto N = ROW_COUNT;
    auto dataset = DataGen(schema, N);
    auto vec_col = dataset.get_col<float>(FieldId(100));
    auto query_ptr = vec_col.data() + BIAS * DIM;

    int64_t offset;
    PreInsert(segment, N, &offset);

    auto insert_data = serialize(dataset.raw_);
    auto ins_res = Insert(segment,
                          offset,
                          N,
                          dataset.row_ids_.data(),
                          dataset.timestamps_.data(),
                          insert_data.data(),
                          insert_data.size());
    ASSERT_EQ(ins_res.error_code, Success);

    const char* raw_plan = R"(vector_anns: <
                                             field_id: 100
                                             predicates: <
                                               term_expr: <
                                                 column_info: <
                                                   field_id: 101
                                                   data_type: Int64
                                                 >
                                                 values: <
                                                   int64_val: 4200
                                                 >
                                                 values: <
                                                   int64_val: 4201
                                                 >
                                                 values: <
                                                   int64_val: 4202
                                                 >
                                                 values: <
                                                   int64_val: 4203
                                                 >
                                                 values: <
                                                   int64_val: 4204
                                                 >
                                               >
                                             >
                                             query_info: <
                                               topk: 5
                                               round_decimal: -1
                                               metric_type: "L2"
                                               search_params: "{\"nprobe\": 10}"
                                             >
                                             placeholder_tag: "$0"
     >)";
    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);

    // create place_holder_group
    int num_queries = 5;
    auto raw_group =
        CreatePlaceholderGroupFromBlob(num_queries, DIM, query_ptr);
    auto blob = raw_group.SerializeAsString();

    // search on segment's small index
    void* plan = nullptr;
    status = CreateSearchPlanByExpr(
        collection, plan_str.data(), plan_str.size(), &plan);
    ASSERT_EQ(status.error_code, Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(
        plan, blob.data(), blob.length(), &placeholderGroup);
    ASSERT_EQ(status.error_code, Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    Timestamp timestamp = 10000000;

    CSearchResult c_search_result_on_smallIndex;
    auto res_before_load_index = CSearch(segment,
                                         plan,
                                         placeholderGroup,
                                         timestamp,
                                         &c_search_result_on_smallIndex);
    ASSERT_EQ(res_before_load_index.error_code, Success);

    // load index to segment
    auto indexing = generate_index(vec_col.data(),
                                   DataType::VECTOR_FLOAT,
                                   knowhere::metric::L2,
                                   IndexEnum::INDEX_FAISS_IVFSQ8,
                                   DIM,
                                   N);

    // gen query dataset
    auto query_dataset = knowhere::GenDataSet(num_queries, DIM, query_ptr);
    auto vec_index = dynamic_cast<VectorIndex*>(indexing.get());
    auto search_plan = reinterpret_cast<milvus::query::Plan*>(plan);
    SearchInfo search_info = search_plan->plan_node_->search_info_;
    SearchResult result_on_index;
    vec_index->Query(
        query_dataset, search_info, nullptr, nullptr, result_on_index);
    auto ids = result_on_index.seg_offsets_.data();
    auto dis = result_on_index.distances_.data();
    std::vector<int64_t> vec_ids(ids, ids + TOPK * num_queries);
    std::vector<float> vec_dis;
    for (int j = 0; j < TOPK * num_queries; ++j) {
        vec_dis.push_back(dis[j] * -1);
    }

    auto search_result_on_raw_index =
        (SearchResult*)c_search_result_on_smallIndex;
    search_result_on_raw_index->seg_offsets_ = vec_ids;
    search_result_on_raw_index->distances_ = vec_dis;

    auto binary_set = indexing->Serialize(milvus::Config{});
    void* c_load_index_info = nullptr;
    status = NewLoadIndexInfo(&c_load_index_info);
    ASSERT_EQ(status.error_code, Success);
    std::string index_type_key = "index_type";
    std::string index_type_value = IndexEnum::INDEX_FAISS_IVFSQ8;
    std::string metric_type_key = "metric_type";
    std::string metric_type_value = knowhere::metric::L2;

    AppendIndexParam(
        c_load_index_info, index_type_key.c_str(), index_type_value.c_str());
    AppendIndexParam(
        c_load_index_info, metric_type_key.c_str(), metric_type_value.c_str());
    AppendFieldInfoForTest(
        c_load_index_info, 0, 0, 0, 100, CDataType::FloatVector, false, "");
    AppendIndexEngineVersionToLoadInfo(
        c_load_index_info,
        knowhere::Version::GetCurrentVersion().VersionNumber());
    AppendIndex(c_load_index_info, (CBinarySet)&binary_set);

    // load index for vec field, load raw data for scalar field
    auto sealed_segment = CreateSealedWithFieldDataLoaded(schema, dataset);
    sealed_segment->DropFieldData(FieldId(100));
    sealed_segment->LoadIndex(*(LoadIndexInfo*)c_load_index_info);

    CSearchResult c_search_result_on_bigIndex;
    auto res_after_load_index = CSearch(sealed_segment.get(),
                                        plan,
                                        placeholderGroup,
                                        timestamp,
                                        &c_search_result_on_bigIndex);
    ASSERT_EQ(res_after_load_index.error_code, Success);

    auto search_result_on_bigIndex = (SearchResult*)c_search_result_on_bigIndex;
    for (int i = 0; i < num_queries; ++i) {
        auto offset = i * TOPK;
        ASSERT_EQ(search_result_on_bigIndex->seg_offsets_[offset], BIAS + i);
        ASSERT_EQ(search_result_on_bigIndex->distances_[offset],
                  search_result_on_raw_index->distances_[offset]);
    }

    DeleteLoadIndexInfo(c_load_index_info);
    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchResult(c_search_result_on_smallIndex);
    DeleteSearchResult(c_search_result_on_bigIndex);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, Indexing_Expr_With_float_Predicate_Term) {
    // insert data to segment
    constexpr auto TOPK = 5;

    std::string schema_string = generate_collection_schema<milvus::FloatVector>(
        knowhere::metric::L2, DIM);
    auto collection = NewCollection(schema_string.c_str());
    auto schema = ((segcore::Collection*)collection)->get_schema();
    CSegmentInterface segment;
    auto status = NewSegment(collection, Growing, -1, &segment, false);
    ASSERT_EQ(status.error_code, Success);

    auto N = 1000 * 10;
    auto dataset = DataGen(schema, N);
    auto vec_col = dataset.get_col<float>(FieldId(100));
    auto query_ptr = vec_col.data() + BIAS * DIM;

    int64_t offset;
    PreInsert(segment, N, &offset);

    auto insert_data = serialize(dataset.raw_);
    auto ins_res = Insert(segment,
                          offset,
                          N,
                          dataset.row_ids_.data(),
                          dataset.timestamps_.data(),
                          insert_data.data(),
                          insert_data.size());
    ASSERT_EQ(ins_res.error_code, Success);

    const char* serialized_expr_plan = R"(
 vector_anns: <
   field_id: 100
   predicates: <
     term_expr: <
       column_info: <
         field_id: 101
         data_type: Int64
       >
       values: <
         int64_val: 4200
       >
       values: <
         int64_val: 4201
       >
       values: <
         int64_val: 4202
       >
       values: <
         int64_val: 4203
       >
       values: <
         int64_val: 4204
       >
     >
   >
   query_info: <
     topk: 5
     round_decimal: -1
     metric_type: "L2"
     search_params: "{\"nprobe\": 10}"
   >
   placeholder_tag: "$0"
 >)";

    // create place_holder_group
    int num_queries = 5;
    auto raw_group =
        CreatePlaceholderGroupFromBlob(num_queries, DIM, query_ptr);
    auto blob = raw_group.SerializeAsString();

    // search on segment's small index
    void* plan = nullptr;
    auto binary_plan = translate_text_plan_to_binary_plan(serialized_expr_plan);
    status = CreateSearchPlanByExpr(
        collection, binary_plan.data(), binary_plan.size(), &plan);
    ASSERT_EQ(status.error_code, Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(
        plan, blob.data(), blob.length(), &placeholderGroup);
    ASSERT_EQ(status.error_code, Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    Timestamp timestamp = 10000000;

    CSearchResult c_search_result_on_smallIndex;
    auto res_before_load_index = CSearch(segment,
                                         plan,
                                         placeholderGroup,
                                         timestamp,
                                         &c_search_result_on_smallIndex);
    ASSERT_EQ(res_before_load_index.error_code, Success);

    // load index to segment
    auto indexing = generate_index(vec_col.data(),
                                   DataType::VECTOR_FLOAT,
                                   knowhere::metric::L2,
                                   IndexEnum::INDEX_FAISS_IVFSQ8,
                                   DIM,
                                   N);

    // gen query dataset
    auto query_dataset = knowhere::GenDataSet(num_queries, DIM, query_ptr);
    auto vec_index = dynamic_cast<VectorIndex*>(indexing.get());
    auto search_plan = reinterpret_cast<milvus::query::Plan*>(plan);
    SearchInfo search_info = search_plan->plan_node_->search_info_;
    SearchResult result_on_index;
    vec_index->Query(
        query_dataset, search_info, nullptr, nullptr, result_on_index);
    auto ids = result_on_index.seg_offsets_.data();
    auto dis = result_on_index.distances_.data();
    std::vector<int64_t> vec_ids(ids, ids + TOPK * num_queries);
    std::vector<float> vec_dis;
    for (int j = 0; j < TOPK * num_queries; ++j) {
        vec_dis.push_back(dis[j] * -1);
    }

    auto search_result_on_raw_index =
        (SearchResult*)c_search_result_on_smallIndex;
    search_result_on_raw_index->seg_offsets_ = vec_ids;
    search_result_on_raw_index->distances_ = vec_dis;

    auto binary_set = indexing->Serialize(milvus::Config{});
    void* c_load_index_info = nullptr;
    status = NewLoadIndexInfo(&c_load_index_info);
    ASSERT_EQ(status.error_code, Success);
    std::string index_type_key = "index_type";
    std::string index_type_value = IndexEnum::INDEX_FAISS_IVFSQ8;
    std::string metric_type_key = "metric_type";
    std::string metric_type_value = knowhere::metric::L2;

    AppendIndexParam(
        c_load_index_info, index_type_key.c_str(), index_type_value.c_str());
    AppendIndexParam(
        c_load_index_info, metric_type_key.c_str(), metric_type_value.c_str());
    AppendFieldInfoForTest(
        c_load_index_info, 0, 0, 0, 100, CDataType::FloatVector, false, "");
    AppendIndexEngineVersionToLoadInfo(
        c_load_index_info,
        knowhere::Version::GetCurrentVersion().VersionNumber());
    AppendIndex(c_load_index_info, (CBinarySet)&binary_set);

    // load index for vec field, load raw data for scalar field
    auto sealed_segment = CreateSealedWithFieldDataLoaded(schema, dataset);
    sealed_segment->DropFieldData(FieldId(100));
    sealed_segment->LoadIndex(*(LoadIndexInfo*)c_load_index_info);

    CSearchResult c_search_result_on_bigIndex;
    auto res_after_load_index = CSearch(sealed_segment.get(),
                                        plan,
                                        placeholderGroup,
                                        timestamp,
                                        &c_search_result_on_bigIndex);
    ASSERT_EQ(res_after_load_index.error_code, Success);

    auto search_result_on_bigIndex = (SearchResult*)c_search_result_on_bigIndex;
    for (int i = 0; i < num_queries; ++i) {
        auto offset = i * TOPK;
        ASSERT_EQ(search_result_on_bigIndex->seg_offsets_[offset], BIAS + i);
        ASSERT_EQ(search_result_on_bigIndex->distances_[offset],
                  search_result_on_raw_index->distances_[offset]);
    }

    DeleteLoadIndexInfo(c_load_index_info);
    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchResult(c_search_result_on_smallIndex);
    DeleteSearchResult(c_search_result_on_bigIndex);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, Indexing_With_binary_Predicate_Range) {
    auto dim = 16;
    // insert data to segment
    constexpr auto TOPK = 5;

    std::string schema_string =
        generate_collection_schema<milvus::BinaryVector>(
            knowhere::metric::JACCARD, dim);
    auto collection =
        NewCollection(schema_string.c_str(), knowhere::metric::JACCARD);
    auto schema = ((segcore::Collection*)collection)->get_schema();
    CSegmentInterface segment;
    auto status = NewSegment(collection, Growing, -1, &segment, false);
    ASSERT_EQ(status.error_code, Success);

    auto N = ROW_COUNT;
    auto dataset = DataGen(schema, N);
    auto vec_col = dataset.get_col<uint8_t>(FieldId(100));
    auto query_ptr = vec_col.data() + BIAS * dim / 8;

    int64_t offset;
    PreInsert(segment, N, &offset);

    auto insert_data = serialize(dataset.raw_);
    auto ins_res = Insert(segment,
                          offset,
                          N,
                          dataset.row_ids_.data(),
                          dataset.timestamps_.data(),
                          insert_data.data(),
                          insert_data.size());
    ASSERT_EQ(ins_res.error_code, Success);

    const char* raw_plan = R"(vector_anns: <
                                             field_id: 100
                                             predicates: <
                                               binary_expr: <
                                                 op: LogicalAnd
                                                 left: <
                                                   unary_range_expr: <
                                                     column_info: <
                                                       field_id: 101
                                                       data_type: Int64
                                                     >
                                                     op: GreaterEqual
                                                     value: <
                                                       int64_val: 4200
                                                     >
                                                   >
                                                 >
                                                 right: <
                                                   unary_range_expr: <
                                                     column_info: <
                                                       field_id: 101
                                                       data_type: Int64
                                                     >
                                                     op: LessThan
                                                     value: <
                                                       int64_val: 4210
                                                     >
                                                   >
                                                 >
                                               >
                                             >
                                             query_info: <
                                               topk: 5
                                               round_decimal: -1
                                               metric_type: "JACCARD"
                                               search_params: "{\"nprobe\": 10}"
                                             >
                                             placeholder_tag: "$0"
     >)";
    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);

    // create place_holder_group
    int num_queries = 5;
    auto raw_group = CreatePlaceholderGroupFromBlob<milvus::BinaryVector>(
        num_queries, dim, query_ptr);
    auto blob = raw_group.SerializeAsString();

    // search on segment's small index
    void* plan = nullptr;
    status = CreateSearchPlanByExpr(
        collection, plan_str.data(), plan_str.size(), &plan);
    ASSERT_EQ(status.error_code, Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(
        plan, blob.data(), blob.length(), &placeholderGroup);
    ASSERT_EQ(status.error_code, Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    Timestamp timestamp = 10000000;

    CSearchResult c_search_result_on_smallIndex;
    auto res_before_load_index = CSearch(segment,
                                         plan,
                                         placeholderGroup,
                                         timestamp,
                                         &c_search_result_on_smallIndex);
    ASSERT_EQ(res_before_load_index.error_code, Success);

    // load index to segment

    auto indexing = generate_index(vec_col.data(),
                                   DataType::VECTOR_BINARY,
                                   knowhere::metric::JACCARD,
                                   IndexEnum::INDEX_FAISS_BIN_IVFFLAT,
                                   dim,
                                   N);

    // gen query dataset
    auto query_dataset = knowhere::GenDataSet(num_queries, dim, query_ptr);
    auto vec_index = dynamic_cast<VectorIndex*>(indexing.get());
    auto search_plan = reinterpret_cast<milvus::query::Plan*>(plan);
    SearchInfo search_info = search_plan->plan_node_->search_info_;
    SearchResult result_on_index;
    vec_index->Query(
        query_dataset, search_info, nullptr, nullptr, result_on_index);
    auto ids = result_on_index.seg_offsets_.data();
    auto dis = result_on_index.distances_.data();
    std::vector<int64_t> vec_ids(ids, ids + TOPK * num_queries);
    std::vector<float> vec_dis;
    for (int j = 0; j < TOPK * num_queries; ++j) {
        vec_dis.push_back(dis[j] * -1);
    }

    auto search_result_on_raw_index =
        (SearchResult*)c_search_result_on_smallIndex;
    search_result_on_raw_index->seg_offsets_ = vec_ids;
    search_result_on_raw_index->distances_ = vec_dis;

    auto binary_set = indexing->Serialize(milvus::Config{});
    void* c_load_index_info = nullptr;
    status = NewLoadIndexInfo(&c_load_index_info);
    ASSERT_EQ(status.error_code, Success);
    std::string index_type_key = "index_type";
    std::string index_type_value = IndexEnum::INDEX_FAISS_BIN_IVFFLAT;
    std::string metric_type_key = "metric_type";
    std::string metric_type_value = knowhere::metric::JACCARD;

    AppendIndexParam(
        c_load_index_info, index_type_key.c_str(), index_type_value.c_str());
    AppendIndexParam(
        c_load_index_info, metric_type_key.c_str(), metric_type_value.c_str());
    AppendFieldInfoForTest(
        c_load_index_info, 0, 0, 0, 100, CDataType::BinaryVector, false, "");
    AppendIndexEngineVersionToLoadInfo(
        c_load_index_info,
        knowhere::Version::GetCurrentVersion().VersionNumber());
    AppendIndex(c_load_index_info, (CBinarySet)&binary_set);

    // load index for vec field, load raw data for scalar field
    auto sealed_segment = CreateSealedWithFieldDataLoaded(schema, dataset);
    sealed_segment->DropFieldData(FieldId(100));
    sealed_segment->LoadIndex(*(LoadIndexInfo*)c_load_index_info);

    CSearchResult c_search_result_on_bigIndex;
    auto res_after_load_index = CSearch(sealed_segment.get(),
                                        plan,
                                        placeholderGroup,
                                        timestamp,
                                        &c_search_result_on_bigIndex);
    ASSERT_EQ(res_after_load_index.error_code, Success);

    auto search_result_on_bigIndex = (SearchResult*)c_search_result_on_bigIndex;
    for (int i = 0; i < num_queries; ++i) {
        auto offset = i * TOPK;
        ASSERT_EQ(search_result_on_bigIndex->seg_offsets_[offset], BIAS + i);
        ASSERT_EQ(search_result_on_bigIndex->distances_[offset],
                  search_result_on_raw_index->distances_[offset]);
    }

    DeleteLoadIndexInfo(c_load_index_info);
    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchResult(c_search_result_on_smallIndex);
    DeleteSearchResult(c_search_result_on_bigIndex);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, Indexing_Expr_With_binary_Predicate_Range) {
    // insert data to segment
    constexpr auto TOPK = 5;
    auto dim = 16;

    std::string schema_string =
        generate_collection_schema<milvus::BinaryVector>(
            knowhere::metric::JACCARD, dim);
    auto collection =
        NewCollection(schema_string.c_str(), knowhere::metric::JACCARD);
    auto schema = ((segcore::Collection*)collection)->get_schema();
    CSegmentInterface segment;
    auto status = NewSegment(collection, Growing, -1, &segment, false);
    ASSERT_EQ(status.error_code, Success);

    auto N = ROW_COUNT;
    auto dataset = DataGen(schema, N);
    auto vec_col = dataset.get_col<uint8_t>(FieldId(100));
    auto query_ptr = vec_col.data() + BIAS * dim / 8;

    int64_t offset;
    PreInsert(segment, N, &offset);

    auto insert_data = serialize(dataset.raw_);
    auto ins_res = Insert(segment,
                          offset,
                          N,
                          dataset.row_ids_.data(),
                          dataset.timestamps_.data(),
                          insert_data.data(),
                          insert_data.size());
    ASSERT_EQ(ins_res.error_code, Success);

    const char* serialized_expr_plan = R"(vector_anns: <
                                            field_id: 100
                                            predicates: <
                                              binary_expr: <
                                                op: LogicalAnd
                                                left: <
                                                  unary_range_expr: <
                                                    column_info: <
                                                      field_id: 101
                                                      data_type: Int64
                                                    >
                                                    op: GreaterEqual
                                                    value: <
                                                      int64_val: 4200
                                                    >
                                                  >
                                                >
                                                right: <
                                                  unary_range_expr: <
                                                    column_info: <
                                                      field_id: 101
                                                      data_type: Int64
                                                    >
                                                    op: LessThan
                                                    value: <
                                                      int64_val: 4210
                                                    >
                                                  >
                                                >
                                              >
                                            >
                                            query_info: <
                                              topk: 5
                                              round_decimal: -1
                                              metric_type: "JACCARD"
                                              search_params: "{\"nprobe\": 10}"
                                            >
                                            placeholder_tag: "$0"
                                        >)";

    // create place_holder_group
    int num_queries = 5;
    auto raw_group = CreatePlaceholderGroupFromBlob<milvus::BinaryVector>(
        num_queries, dim, query_ptr);
    auto blob = raw_group.SerializeAsString();

    // search on segment's small index
    void* plan = nullptr;
    auto binary_plan = translate_text_plan_to_binary_plan(serialized_expr_plan);
    status = CreateSearchPlanByExpr(
        collection, binary_plan.data(), binary_plan.size(), &plan);
    ASSERT_EQ(status.error_code, Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(
        plan, blob.data(), blob.length(), &placeholderGroup);
    ASSERT_EQ(status.error_code, Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    Timestamp timestamp = 10000000;

    CSearchResult c_search_result_on_smallIndex;
    auto res_before_load_index = CSearch(segment,
                                         plan,
                                         placeholderGroup,
                                         timestamp,
                                         &c_search_result_on_smallIndex);
    ASSERT_TRUE(res_before_load_index.error_code == Success)
        << res_before_load_index.error_msg;

    // load index to segment
    auto indexing = generate_index(vec_col.data(),
                                   DataType::VECTOR_BINARY,
                                   knowhere::metric::JACCARD,
                                   IndexEnum::INDEX_FAISS_BIN_IVFFLAT,
                                   dim,
                                   N);

    // gen query dataset
    auto query_dataset = knowhere::GenDataSet(num_queries, dim, query_ptr);
    auto vec_index = dynamic_cast<VectorIndex*>(indexing.get());
    auto search_plan = reinterpret_cast<milvus::query::Plan*>(plan);
    SearchInfo search_info = search_plan->plan_node_->search_info_;
    SearchResult result_on_index;
    vec_index->Query(
        query_dataset, search_info, nullptr, nullptr, result_on_index);
    auto ids = result_on_index.seg_offsets_.data();
    auto dis = result_on_index.distances_.data();
    std::vector<int64_t> vec_ids(ids, ids + TOPK * num_queries);
    std::vector<float> vec_dis;
    for (int j = 0; j < TOPK * num_queries; ++j) {
        vec_dis.push_back(dis[j] * -1);
    }

    auto search_result_on_raw_index =
        (SearchResult*)c_search_result_on_smallIndex;
    search_result_on_raw_index->seg_offsets_ = vec_ids;
    search_result_on_raw_index->distances_ = vec_dis;

    auto binary_set = indexing->Serialize(milvus::Config{});
    void* c_load_index_info = nullptr;
    status = NewLoadIndexInfo(&c_load_index_info);
    ASSERT_EQ(status.error_code, Success);
    std::string index_type_key = "index_type";
    std::string index_type_value = IndexEnum::INDEX_FAISS_BIN_IVFFLAT;
    std::string metric_type_key = "metric_type";
    std::string metric_type_value = knowhere::metric::JACCARD;

    AppendIndexParam(
        c_load_index_info, index_type_key.c_str(), index_type_value.c_str());
    AppendIndexParam(
        c_load_index_info, metric_type_key.c_str(), metric_type_value.c_str());
    AppendFieldInfoForTest(
        c_load_index_info, 0, 0, 0, 100, CDataType::BinaryVector, false, "");
    AppendIndexEngineVersionToLoadInfo(
        c_load_index_info,
        knowhere::Version::GetCurrentVersion().VersionNumber());
    AppendIndex(c_load_index_info, (CBinarySet)&binary_set);

    // load index for vec field, load raw data for scalar field
    auto sealed_segment = CreateSealedWithFieldDataLoaded(schema, dataset);
    sealed_segment->DropFieldData(FieldId(100));
    sealed_segment->LoadIndex(*(LoadIndexInfo*)c_load_index_info);

    CSearchResult c_search_result_on_bigIndex;
    auto res_after_load_index = CSearch(sealed_segment.get(),
                                        plan,
                                        placeholderGroup,
                                        timestamp,
                                        &c_search_result_on_bigIndex);
    ASSERT_EQ(res_after_load_index.error_code, Success);

    auto search_result_on_bigIndex = (SearchResult*)c_search_result_on_bigIndex;
    for (int i = 0; i < num_queries; ++i) {
        auto offset = i * TOPK;
        ASSERT_EQ(search_result_on_bigIndex->seg_offsets_[offset], BIAS + i);
        ASSERT_EQ(search_result_on_bigIndex->distances_[offset],
                  search_result_on_raw_index->distances_[offset]);
    }

    DeleteLoadIndexInfo(c_load_index_info);
    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchResult(c_search_result_on_smallIndex);
    DeleteSearchResult(c_search_result_on_bigIndex);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, Indexing_With_binary_Predicate_Term) {
    // insert data to segment
    constexpr auto TOPK = 5;
    auto dim = 16;

    std::string schema_string =
        generate_collection_schema<milvus::BinaryVector>(
            knowhere::metric::JACCARD, dim);
    auto collection =
        NewCollection(schema_string.c_str(), knowhere::metric::JACCARD);
    auto schema = ((segcore::Collection*)collection)->get_schema();
    CSegmentInterface segment;
    auto status = NewSegment(collection, Growing, -1, &segment, false);
    ASSERT_EQ(status.error_code, Success);

    auto N = ROW_COUNT;
    auto dataset = DataGen(schema, N);
    auto vec_col = dataset.get_col<uint8_t>(FieldId(100));
    auto query_ptr = vec_col.data() + BIAS * dim / 8;

    int64_t offset;
    PreInsert(segment, N, &offset);

    auto insert_data = serialize(dataset.raw_);
    auto ins_res = Insert(segment,
                          offset,
                          N,
                          dataset.row_ids_.data(),
                          dataset.timestamps_.data(),
                          insert_data.data(),
                          insert_data.size());
    ASSERT_EQ(ins_res.error_code, Success);

    const char* raw_plan = R"(vector_anns: <
                                             field_id: 100
                                             predicates: <
                                               term_expr: <
                                                 column_info: <
                                                   field_id: 101
                                                   data_type: Int64
                                                 >
                                                 values: <
                                                   int64_val: 4200
                                                 >
                                                 values: <
                                                   int64_val: 4201
                                                 >
                                                 values: <
                                                   int64_val: 4202
                                                 >
                                                 values: <
                                                   int64_val: 4203
                                                 >
                                                 values: <
                                                   int64_val: 4204
                                                 >
                                               >
                                             >
                                             query_info: <
                                               topk: 5
                                               round_decimal: -1
                                               metric_type: "JACCARD"
                                               search_params: "{\"nprobe\": 10}"
                                             >
                                             placeholder_tag: "$0"
     >)";
    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);

    // create place_holder_group
    int num_queries = 5;
    int topK = 5;
    auto raw_group = CreatePlaceholderGroupFromBlob<milvus::BinaryVector>(
        num_queries, dim, query_ptr);
    auto blob = raw_group.SerializeAsString();

    // search on segment's small index
    void* plan = nullptr;
    status = CreateSearchPlanByExpr(
        collection, plan_str.data(), plan_str.size(), &plan);
    ASSERT_EQ(status.error_code, Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(
        plan, blob.data(), blob.length(), &placeholderGroup);
    ASSERT_EQ(status.error_code, Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    Timestamp timestamp = 10000000;

    CSearchResult c_search_result_on_smallIndex;
    auto res_before_load_index = CSearch(segment,
                                         plan,
                                         placeholderGroup,
                                         timestamp,
                                         &c_search_result_on_smallIndex);
    ASSERT_EQ(res_before_load_index.error_code, Success);

    // load index to segment
    auto indexing = generate_index(vec_col.data(),
                                   DataType::VECTOR_BINARY,
                                   knowhere::metric::JACCARD,
                                   IndexEnum::INDEX_FAISS_BIN_IVFFLAT,
                                   dim,
                                   N);

    // gen query dataset
    auto query_dataset = knowhere::GenDataSet(num_queries, dim, query_ptr);
    auto vec_index = dynamic_cast<VectorIndex*>(indexing.get());
    auto search_plan = reinterpret_cast<milvus::query::Plan*>(plan);
    SearchInfo search_info = search_plan->plan_node_->search_info_;
    SearchResult result_on_index;
    vec_index->Query(
        query_dataset, search_info, nullptr, nullptr, result_on_index);
    auto ids = result_on_index.seg_offsets_.data();
    auto dis = result_on_index.distances_.data();
    std::vector<int64_t> vec_ids(ids, ids + TOPK * num_queries);
    std::vector<float> vec_dis;
    for (int j = 0; j < TOPK * num_queries; ++j) {
        vec_dis.push_back(dis[j] * -1);
    }

    auto search_result_on_raw_index =
        (SearchResult*)c_search_result_on_smallIndex;
    search_result_on_raw_index->seg_offsets_ = vec_ids;
    search_result_on_raw_index->distances_ = vec_dis;

    auto binary_set = indexing->Serialize(milvus::Config{});
    void* c_load_index_info = nullptr;
    status = NewLoadIndexInfo(&c_load_index_info);
    ASSERT_EQ(status.error_code, Success);
    std::string index_type_key = "index_type";
    std::string index_type_value = IndexEnum::INDEX_FAISS_BIN_IVFFLAT;
    std::string metric_type_key = "metric_type";
    std::string metric_type_value = knowhere::metric::JACCARD;

    AppendIndexParam(
        c_load_index_info, index_type_key.c_str(), index_type_value.c_str());
    AppendIndexParam(
        c_load_index_info, metric_type_key.c_str(), metric_type_value.c_str());
    AppendFieldInfoForTest(
        c_load_index_info, 0, 0, 0, 100, CDataType::BinaryVector, false, "");
    AppendIndexEngineVersionToLoadInfo(
        c_load_index_info,
        knowhere::Version::GetCurrentVersion().VersionNumber());
    AppendIndex(c_load_index_info, (CBinarySet)&binary_set);

    // load index for vec field, load raw data for scalar field
    auto sealed_segment = CreateSealedWithFieldDataLoaded(schema, dataset);
    sealed_segment->DropFieldData(FieldId(100));
    sealed_segment->LoadIndex(*(LoadIndexInfo*)c_load_index_info);

    CSearchResult c_search_result_on_bigIndex;
    auto res_after_load_index = CSearch(sealed_segment.get(),
                                        plan,
                                        placeholderGroup,
                                        timestamp,
                                        &c_search_result_on_bigIndex);
    ASSERT_EQ(res_after_load_index.error_code, Success);

    std::vector<CSearchResult> results;
    results.push_back(c_search_result_on_bigIndex);

    auto slice_nqs = std::vector<int64_t>{num_queries};
    auto slice_topKs = std::vector<int64_t>{topK};

    CSearchResultDataBlobs cSearchResultData;
    status = ReduceSearchResultsAndFillData({},
                                            &cSearchResultData,
                                            plan,
                                            results.data(),
                                            results.size(),
                                            slice_nqs.data(),
                                            slice_topKs.data(),
                                            slice_nqs.size());
    ASSERT_EQ(status.error_code, Success);

    //    status = ReduceSearchResultsAndFillData(plan, results.data(), results.size());
    //    ASSERT_EQ(status.error_code, Success);

    auto search_result_on_bigIndex = (SearchResult*)c_search_result_on_bigIndex;
    for (int i = 0; i < num_queries; ++i) {
        ASSERT_EQ(search_result_on_bigIndex->topk_per_nq_prefix_sum_.size(),
                  search_result_on_bigIndex->total_nq_ + 1);
        auto offset = search_result_on_bigIndex->topk_per_nq_prefix_sum_[i];
        ASSERT_EQ(search_result_on_bigIndex->seg_offsets_[offset], BIAS + i);
        ASSERT_EQ(search_result_on_bigIndex->distances_[offset],
                  search_result_on_raw_index->distances_[i * TOPK]);
    }

    DeleteLoadIndexInfo(c_load_index_info);
    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchResult(c_search_result_on_smallIndex);
    DeleteSearchResult(c_search_result_on_bigIndex);
    DeleteCollection(collection);
    DeleteSegment(segment);
    DeleteSearchResultDataBlobs(cSearchResultData);
}

TEST(CApiTest, Indexing_Expr_With_binary_Predicate_Term) {
    // insert data to segment
    constexpr auto TOPK = 5;
    auto dim = 16;

    std::string schema_string =
        generate_collection_schema<milvus::BinaryVector>(
            knowhere::metric::JACCARD, dim);
    auto collection =
        NewCollection(schema_string.c_str(), knowhere::metric::JACCARD);
    auto schema = ((segcore::Collection*)collection)->get_schema();
    CSegmentInterface segment;
    auto status = NewSegment(collection, Growing, -1, &segment, false);
    ASSERT_EQ(status.error_code, Success);

    auto N = ROW_COUNT;
    auto dataset = DataGen(schema, N);
    auto vec_col = dataset.get_col<uint8_t>(FieldId(100));
    auto query_ptr = vec_col.data() + BIAS * dim / 8;

    int64_t offset;
    PreInsert(segment, N, &offset);

    auto insert_data = serialize(dataset.raw_);
    auto ins_res = Insert(segment,
                          offset,
                          N,
                          dataset.row_ids_.data(),
                          dataset.timestamps_.data(),
                          insert_data.data(),
                          insert_data.size());
    ASSERT_EQ(ins_res.error_code, Success);

    const char* serialized_expr_plan = R"(vector_anns: <
                                            field_id: 100
                                            predicates: <
                                              term_expr: <
                                                column_info: <
                                                  field_id: 101
                                                  data_type: Int64
                                                >
                                                values: <
                                                  int64_val: 4200
                                                >
                                                values: <
                                                  int64_val: 4201
                                                >
                                                values: <
                                                  int64_val: 4202
                                                >
                                                values: <
                                                  int64_val: 4203
                                                >
                                                values: <
                                                  int64_val: 4204
                                                >
                                              >
                                            >
                                            query_info: <
                                              topk: 5
                                              round_decimal: -1
                                              metric_type: "JACCARD"
                                              search_params: "{\"nprobe\": 10}"
                                            >
                                            placeholder_tag: "$0"
                                        >)";

    // create place_holder_group
    int num_queries = 5;
    int topK = 5;
    auto raw_group = CreatePlaceholderGroupFromBlob<milvus::BinaryVector>(
        num_queries, dim, query_ptr);
    auto blob = raw_group.SerializeAsString();

    // search on segment's small index
    void* plan = nullptr;
    auto binary_plan = translate_text_plan_to_binary_plan(serialized_expr_plan);
    status = CreateSearchPlanByExpr(
        collection, binary_plan.data(), binary_plan.size(), &plan);
    ASSERT_EQ(status.error_code, Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(
        plan, blob.data(), blob.length(), &placeholderGroup);
    ASSERT_EQ(status.error_code, Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    Timestamp timestamp = 10000000;

    CSearchResult c_search_result_on_smallIndex;
    auto res_before_load_index = CSearch(segment,
                                         plan,
                                         placeholderGroup,
                                         timestamp,
                                         &c_search_result_on_smallIndex);
    ASSERT_EQ(res_before_load_index.error_code, Success);

    // load index to segment
    auto indexing = generate_index(vec_col.data(),
                                   DataType::VECTOR_BINARY,
                                   knowhere::metric::JACCARD,
                                   IndexEnum::INDEX_FAISS_BIN_IVFFLAT,
                                   dim,
                                   N);

    // gen query dataset
    auto query_dataset = knowhere::GenDataSet(num_queries, dim, query_ptr);
    auto vec_index = dynamic_cast<VectorIndex*>(indexing.get());
    auto search_plan = reinterpret_cast<milvus::query::Plan*>(plan);
    SearchInfo search_info = search_plan->plan_node_->search_info_;
    SearchResult result_on_index;
    vec_index->Query(
        query_dataset, search_info, nullptr, nullptr, result_on_index);
    auto ids = result_on_index.seg_offsets_.data();
    auto dis = result_on_index.distances_.data();
    std::vector<int64_t> vec_ids(ids, ids + TOPK * num_queries);
    std::vector<float> vec_dis;
    for (int j = 0; j < TOPK * num_queries; ++j) {
        vec_dis.push_back(dis[j] * -1);
    }

    auto search_result_on_raw_index =
        (SearchResult*)c_search_result_on_smallIndex;
    search_result_on_raw_index->seg_offsets_ = vec_ids;
    search_result_on_raw_index->distances_ = vec_dis;

    auto binary_set = indexing->Serialize(milvus::Config{});
    void* c_load_index_info = nullptr;
    status = NewLoadIndexInfo(&c_load_index_info);
    ASSERT_EQ(status.error_code, Success);
    std::string index_type_key = "index_type";
    std::string index_type_value = IndexEnum::INDEX_FAISS_BIN_IVFFLAT;
    std::string metric_type_key = "metric_type";
    std::string metric_type_value = knowhere::metric::JACCARD;

    AppendIndexParam(
        c_load_index_info, index_type_key.c_str(), index_type_value.c_str());
    AppendIndexParam(
        c_load_index_info, metric_type_key.c_str(), metric_type_value.c_str());
    AppendFieldInfoForTest(
        c_load_index_info, 0, 0, 0, 100, CDataType::BinaryVector, false, "");
    AppendIndexEngineVersionToLoadInfo(
        c_load_index_info,
        knowhere::Version::GetCurrentVersion().VersionNumber());
    AppendIndex(c_load_index_info, (CBinarySet)&binary_set);

    // load index for vec field, load raw data for scalar field
    auto sealed_segment = CreateSealedWithFieldDataLoaded(schema, dataset);
    sealed_segment->DropFieldData(FieldId(100));
    sealed_segment->LoadIndex(*(LoadIndexInfo*)c_load_index_info);

    CSearchResult c_search_result_on_bigIndex;
    auto res_after_load_index = CSearch(sealed_segment.get(),
                                        plan,
                                        placeholderGroup,
                                        timestamp,
                                        &c_search_result_on_bigIndex);
    ASSERT_EQ(res_after_load_index.error_code, Success);

    std::vector<CSearchResult> results;
    results.push_back(c_search_result_on_bigIndex);

    auto slice_nqs = std::vector<int64_t>{num_queries};
    auto slice_topKs = std::vector<int64_t>{topK};

    CSearchResultDataBlobs cSearchResultData;
    status = ReduceSearchResultsAndFillData({},
                                            &cSearchResultData,
                                            plan,
                                            results.data(),
                                            results.size(),
                                            slice_nqs.data(),
                                            slice_topKs.data(),
                                            slice_nqs.size());
    ASSERT_EQ(status.error_code, Success);

    auto search_result_on_bigIndex = (SearchResult*)c_search_result_on_bigIndex;
    for (int i = 0; i < num_queries; ++i) {
        ASSERT_EQ(search_result_on_bigIndex->topk_per_nq_prefix_sum_.size(),
                  search_result_on_bigIndex->total_nq_ + 1);
        auto offset = search_result_on_bigIndex->topk_per_nq_prefix_sum_[i];
        ASSERT_EQ(search_result_on_bigIndex->seg_offsets_[offset], BIAS + i);
        ASSERT_EQ(search_result_on_bigIndex->distances_[offset],
                  search_result_on_raw_index->distances_[i * TOPK]);
    }

    DeleteLoadIndexInfo(c_load_index_info);
    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchResult(c_search_result_on_smallIndex);
    DeleteSearchResult(c_search_result_on_bigIndex);
    DeleteCollection(collection);
    DeleteSegment(segment);
    DeleteSearchResultDataBlobs(cSearchResultData);
}