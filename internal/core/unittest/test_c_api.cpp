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

#include <iostream>
#include <string>
#include <random>
#include <gtest/gtest.h>
#include <chrono>
#include <google/protobuf/text_format.h>

#include "common/LoadInfo.h"
#include "index/knowhere/knowhere/index/vector_index/helpers/IndexParameter.h"
#include "index/knowhere/knowhere/index/vector_index/adapter/VectorAdapter.h"
#include "index/knowhere/knowhere/index/vector_index/VecIndexFactory.h"
#include "index/knowhere/knowhere/index/vector_index/IndexIVFPQ.h"
#include "pb/milvus.pb.h"
#include "pb/plan.pb.h"
#include "segcore/Collection.h"
#include "segcore/reduce_c.h"
#include "test_utils/DataGen.h"
#include "utils/Types.h"

namespace chrono = std::chrono;

using namespace milvus;
using namespace milvus::segcore;
using namespace milvus::knowhere;

namespace {
const int DIM = 16;
const int64_t ROW_COUNT = 100 * 1000;

const char*
get_default_schema_config() {
    static std::string conf = R"(name: "default-collection"
                                autoID: true
                                fields: <
                                  fieldID: 100
                                  name: "fakevec"
                                  data_type: FloatVector
                                  type_params: <
                                    key: "dim"
                                    value: "16"
                                  >
                                  index_params: <
                                    key: "metric_type"
                                    value: "L2"
                                  >
                                >
                                fields: <
                                  fieldID: 101
                                  name: "age"
                                  data_type: Int32
                                >)";
    static std::string fake_conf = "";
    return conf.c_str();
}

std::vector<char>
translate_text_plan_to_binary_plan(const char* text_plan) {
    proto::plan::PlanNode plan_node;
    auto ok = google::protobuf::TextFormat::ParseFromString(text_plan, &plan_node);
    AssertInfo(ok, "Failed to parse");

    std::string binary_plan;
    plan_node.SerializeToString(&binary_plan);

    std::vector<char> ret;
    ret.resize(binary_plan.size());
    std::memcpy(ret.data(), binary_plan.c_str(), binary_plan.size());

    return ret;
}

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
        raw_data.insert(raw_data.end(), (const char*)std::begin(vec), (const char*)std::end(vec));
        int age = e() % 100;
        raw_data.insert(raw_data.end(), (const char*)&age, ((const char*)&age) + sizeof(age));
    }
    return std::make_tuple(raw_data, timestamps, uids);
}

std::string
generate_query_data(int nq) {
    namespace ser = milvus::proto::milvus;
    std::default_random_engine e(67);
    int dim = DIM;
    std::normal_distribution<double> dis(0.0, 1.0);
    ser::PlaceholderGroup raw_group;
    auto value = raw_group.add_placeholders();
    value->set_tag("$0");
    value->set_type(ser::PlaceholderType::FloatVector);
    for (int i = 0; i < nq; ++i) {
        std::vector<float> vec;
        for (int d = 0; d < dim; ++d) {
            vec.push_back(dis(e));
        }
        value->add_values(vec.data(), vec.size() * sizeof(float));
    }
    auto blob = raw_group.SerializeAsString();
    return blob;
}

std::string
generate_collection_schema(std::string metric_type, int dim, bool is_binary) {
    namespace schema = milvus::proto::schema;
    schema::CollectionSchema collection_schema;
    collection_schema.set_name("collection_test");
    collection_schema.set_autoid(true);

    auto vec_field_schema = collection_schema.add_fields();
    vec_field_schema->set_name("fakevec");
    vec_field_schema->set_fieldid(100);
    if (is_binary) {
        vec_field_schema->set_data_type(schema::DataType::BinaryVector);
    } else {
        vec_field_schema->set_data_type(schema::DataType::FloatVector);
    }
    auto metric_type_param = vec_field_schema->add_index_params();
    metric_type_param->set_key("metric_type");
    metric_type_param->set_value(metric_type);
    auto dim_param = vec_field_schema->add_type_params();
    dim_param->set_key("dim");
    dim_param->set_value(std::to_string(dim));

    auto other_field_schema = collection_schema.add_fields();
    other_field_schema->set_name("counter");
    other_field_schema->set_fieldid(101);
    other_field_schema->set_data_type(schema::DataType::Int64);

    auto other_field_schema2 = collection_schema.add_fields();
    other_field_schema2->set_name("doubleField");
    other_field_schema2->set_fieldid(102);
    other_field_schema2->set_data_type(schema::DataType::Double);

    std::string schema_string;
    auto marshal = google::protobuf::TextFormat::PrintToString(collection_schema, &schema_string);
    assert(marshal == true);
    return schema_string;
}

VecIndexPtr
generate_index(
    void* raw_data, milvus::knowhere::Config conf, int64_t dim, int64_t topK, int64_t N, std::string index_type) {
    auto indexing = knowhere::VecIndexFactory::GetInstance().CreateVecIndex(index_type, knowhere::IndexMode::MODE_CPU);

    auto database = milvus::knowhere::GenDataset(N, dim, raw_data);
    indexing->Train(database, conf);
    indexing->AddWithoutIds(database, conf);
    EXPECT_EQ(indexing->Count(), N);
    EXPECT_EQ(indexing->Dim(), dim);

    EXPECT_EQ(indexing->Count(), N);
    EXPECT_EQ(indexing->Dim(), dim);
    return indexing;
}
}  // namespace

TEST(CApiTest, CollectionTest) {
    auto collection = NewCollection(get_default_schema_config());
    DeleteCollection(collection);
}

TEST(CApiTest, GetCollectionNameTest) {
    auto collection = NewCollection(get_default_schema_config());
    auto name = GetCollectionName(collection);
    assert(strcmp(name, "default-collection") == 0);
    DeleteCollection(collection);
}

TEST(CApiTest, SegmentTest) {
    auto collection = NewCollection(get_default_schema_config());
    auto segment = NewSegment(collection, 0, Growing);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, InsertTest) {
    auto collection = NewCollection(get_default_schema_config());
    auto segment = NewSegment(collection, 0, Growing);

    int N = 10000;
    auto [raw_data, timestamps, uids] = generate_data(N);
    auto line_sizeof = (sizeof(int) + sizeof(float) * DIM);

    int64_t offset;
    PreInsert(segment, N, &offset);

    auto res = Insert(segment, offset, N, uids.data(), timestamps.data(), raw_data.data(), (int)line_sizeof, N);
    assert(res.error_code == Success);

    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, DeleteTest) {
    auto collection = NewCollection(get_default_schema_config());
    auto segment = NewSegment(collection, 0, Growing);

    long delete_row_ids[] = {100000, 100001, 100002};
    unsigned long delete_timestamps[] = {0, 0, 0};

    auto offset = PreDelete(segment, 3);

    auto del_res = Delete(segment, offset, 3, delete_row_ids, delete_timestamps);
    assert(del_res.error_code == Success);

    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, SearchTest) {
    auto collection = NewCollection(get_default_schema_config());
    auto segment = NewSegment(collection, 0, Growing);

    int N = 10000;
    auto [raw_data, timestamps, uids] = generate_data(N);
    auto line_sizeof = (sizeof(int) + sizeof(float) * DIM);

    int64_t offset;
    PreInsert(segment, N, &offset);

    auto ins_res = Insert(segment, offset, N, uids.data(), timestamps.data(), raw_data.data(), (int)line_sizeof, N);
    ASSERT_EQ(ins_res.error_code, Success);

    const char* dsl_string = R"(
    {
        "bool": {
            "vector": {
                "fakevec": {
                    "metric_type": "L2",
                    "params": {
                        "nprobe": 10
                    },
                    "query": "$0",
                    "topk": 10,
                    "round_decimal": 3
                }
            }
        }
    })";

    int num_queries = 10;
    auto blob = generate_query_data(num_queries);

    void* plan = nullptr;
    auto status = CreateSearchPlan(collection, dsl_string, &plan);
    ASSERT_EQ(status.error_code, Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(plan, blob.data(), blob.length(), &placeholderGroup);
    ASSERT_EQ(status.error_code, Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    timestamps.clear();
    timestamps.push_back(1);

    CSearchResult search_result;
    auto res = Search(segment, plan, placeholderGroup, timestamps[0], &search_result);
    ASSERT_EQ(res.error_code, Success);

    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchResult(search_result);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, SearchTestWithExpr) {
    auto collection = NewCollection(get_default_schema_config());
    auto segment = NewSegment(collection, 0, Growing);

    int N = 10000;
    auto [raw_data, timestamps, uids] = generate_data(N);
    auto line_sizeof = (sizeof(int) + sizeof(float) * DIM);

    int64_t offset;
    PreInsert(segment, N, &offset);

    auto ins_res = Insert(segment, offset, N, uids.data(), timestamps.data(), raw_data.data(), (int)line_sizeof, N);
    ASSERT_EQ(ins_res.error_code, Success);

    const char* serialized_expr_plan = R"(vector_anns: <
                                            field_id: 100
                                            query_info: <
                                                topk: 10
                                                metric_type: "L2"
                                                search_params: "{\"nprobe\": 10}"
                                            >
                                            placeholder_tag: "$0"
                                         >)";

    int num_queries = 10;
    auto blob = generate_query_data(num_queries);

    void* plan = nullptr;
    auto binary_plan = translate_text_plan_to_binary_plan(serialized_expr_plan);
    auto status = CreateSearchPlanByExpr(collection, binary_plan.data(), binary_plan.size(), &plan);
    ASSERT_EQ(status.error_code, Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(plan, blob.data(), blob.length(), &placeholderGroup);
    ASSERT_EQ(status.error_code, Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    timestamps.clear();
    timestamps.push_back(1);

    CSearchResult search_result;
    auto res = Search(segment, plan, placeholderGroup, timestamps[0], &search_result);
    ASSERT_EQ(res.error_code, Success);

    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchResult(search_result);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, GetMemoryUsageInBytesTest) {
    auto collection = NewCollection(get_default_schema_config());
    auto segment = NewSegment(collection, 0, Growing);

    auto old_memory_usage_size = GetMemoryUsageInBytes(segment);
    // std::cout << "old_memory_usage_size = " << old_memory_usage_size << std::endl;
    assert(old_memory_usage_size == 0);

    int N = 10000;
    auto [raw_data, timestamps, uids] = generate_data(N);
    auto line_sizeof = (sizeof(int) + sizeof(float) * DIM);

    int64_t offset;
    PreInsert(segment, N, &offset);

    auto res = Insert(segment, offset, N, uids.data(), timestamps.data(), raw_data.data(), (int)line_sizeof, N);
    assert(res.error_code == Success);

    auto memory_usage_size = GetMemoryUsageInBytes(segment);
    // std::cout << "new_memory_usage_size = " << memory_usage_size << std::endl;
    assert(memory_usage_size == 2785280);

    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, GetDeletedCountTest) {
    auto collection = NewCollection(get_default_schema_config());
    auto segment = NewSegment(collection, 0, Growing);

    long delete_row_ids[] = {100000, 100001, 100002};
    unsigned long delete_timestamps[] = {0, 0, 0};

    auto offset = PreDelete(segment, 3);

    auto del_res = Delete(segment, offset, 3, delete_row_ids, delete_timestamps);
    assert(del_res.error_code == Success);

    // TODO: assert(deleted_count == len(delete_row_ids))
    auto deleted_count = GetDeletedCount(segment);
    assert(deleted_count == 0);

    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, GetRowCountTest) {
    auto collection = NewCollection(get_default_schema_config());
    auto segment = NewSegment(collection, 0, Growing);

    int N = 10000;
    auto [raw_data, timestamps, uids] = generate_data(N);
    auto line_sizeof = (sizeof(int) + sizeof(float) * DIM);

    int64_t offset;
    PreInsert(segment, N, &offset);
    auto res = Insert(segment, offset, N, uids.data(), timestamps.data(), raw_data.data(), (int)line_sizeof, N);
    assert(res.error_code == Success);

    auto row_count = GetRowCount(segment);
    assert(row_count == N);

    DeleteCollection(collection);
    DeleteSegment(segment);
}

// TEST(CApiTest, SchemaTest) {
//    std::string schema_string =
//        "id: 6873737669791618215\nname: \"collection0\"\nschema: \u003c\n  "
//        "field_metas: \u003c\n    field_name: \"age\"\n    type: INT32\n    dim: 1\n  \u003e\n  "
//        "field_metas: \u003c\n    field_name: \"field_1\"\n    type: VECTOR_FLOAT\n    dim: 16\n  \u003e\n"
//        "\u003e\ncreate_time: 1600416765\nsegment_ids: 6873737669791618215\npartition_tags: \"default\"\n";
//
//    auto collection = NewCollection(schema_string.data());
//    auto segment = NewSegment(collection, 0, Growing);
//    DeleteCollection(collection);
//    DeleteSegment(segment);
//}

TEST(CApiTest, MergeInto) {
    std::vector<int64_t> uids;
    std::vector<float> distance;

    std::vector<int64_t> new_uids;
    std::vector<float> new_distance;

    int64_t num_queries = 1;
    int64_t topk = 2;

    uids.push_back(1);
    uids.push_back(2);
    distance.push_back(5);
    distance.push_back(1000);

    new_uids.push_back(3);
    new_uids.push_back(4);
    new_distance.push_back(2);
    new_distance.push_back(6);

    auto res = MergeInto(num_queries, topk, distance.data(), uids.data(), new_distance.data(), new_uids.data());

    ASSERT_EQ(res, 0);
    ASSERT_EQ(uids[0], 3);
    ASSERT_EQ(distance[0], 2);
    ASSERT_EQ(uids[1], 1);
    ASSERT_EQ(distance[1], 5);
}

TEST(CApiTest, Reduce) {
    auto collection = NewCollection(get_default_schema_config());
    auto segment = NewSegment(collection, 0, Growing);

    int N = 10000;
    auto [raw_data, timestamps, uids] = generate_data(N);
    auto line_sizeof = (sizeof(int) + sizeof(float) * DIM);

    int64_t offset;
    PreInsert(segment, N, &offset);
    auto ins_res = Insert(segment, offset, N, uids.data(), timestamps.data(), raw_data.data(), (int)line_sizeof, N);
    assert(ins_res.error_code == Success);

    const char* dsl_string = R"(
    {
        "bool": {
            "vector": {
                "fakevec": {
                    "metric_type": "L2",
                    "params": {
                        "nprobe": 10
                    },
                    "query": "$0",
                    "topk": 10,
                    "round_decimal": 3
                }
            }
        }
    })";

    int num_queries = 10;
    auto blob = generate_query_data(num_queries);

    void* plan = nullptr;
    auto status = CreateSearchPlan(collection, dsl_string, &plan);
    assert(status.error_code == Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(plan, blob.data(), blob.length(), &placeholderGroup);
    assert(status.error_code == Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    timestamps.clear();
    timestamps.push_back(1);

    std::vector<CSearchResult> results;
    CSearchResult res1;
    CSearchResult res2;
    auto res = Search(segment, plan, placeholderGroup, timestamps[0], &res1);
    assert(res.error_code == Success);
    res = Search(segment, plan, placeholderGroup, timestamps[0], &res2);
    assert(res.error_code == Success);
    results.push_back(res1);
    results.push_back(res2);

    status = ReduceSearchResultsAndFillData(plan, results.data(), results.size());
    assert(status.error_code == Success);
    void* reorganize_search_result = nullptr;
    status = ReorganizeSearchResults(&reorganize_search_result, results.data(), results.size());
    assert(status.error_code == Success);
    auto hits_blob_size = GetHitsBlobSize(reorganize_search_result);
    assert(hits_blob_size > 0);
    std::vector<char> hits_blob;
    hits_blob.resize(hits_blob_size);
    GetHitsBlob(reorganize_search_result, hits_blob.data());
    assert(hits_blob.data() != nullptr);
    auto num_queries_group = GetNumQueriesPerGroup(reorganize_search_result, 0);
    assert(num_queries_group == num_queries);
    std::vector<int64_t> hit_size_per_query;
    hit_size_per_query.resize(num_queries_group);
    GetHitSizePerQueries(reorganize_search_result, 0, hit_size_per_query.data());
    assert(hit_size_per_query[0] > 0);

    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchResult(res1);
    DeleteSearchResult(res2);
    DeleteMarshaledHits(reorganize_search_result);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, ReduceSearchWithExpr) {
    auto collection = NewCollection(get_default_schema_config());
    auto segment = NewSegment(collection, 0, Growing);

    int N = 10000;
    auto [raw_data, timestamps, uids] = generate_data(N);
    auto line_sizeof = (sizeof(int) + sizeof(float) * DIM);

    int64_t offset;
    PreInsert(segment, N, &offset);
    auto ins_res = Insert(segment, offset, N, uids.data(), timestamps.data(), raw_data.data(), (int)line_sizeof, N);
    assert(ins_res.error_code == Success);

    const char* serialized_expr_plan = R"(vector_anns: <
                                            field_id: 100
                                            query_info: <
                                                topk: 10
                                                metric_type: "L2"
                                                search_params: "{\"nprobe\": 10}"
                                            >
                                            placeholder_tag: "$0"
                                         >)";

    int num_queries = 10;
    auto blob = generate_query_data(num_queries);

    void* plan = nullptr;
    auto binary_plan = translate_text_plan_to_binary_plan(serialized_expr_plan);
    auto status = CreateSearchPlanByExpr(collection, binary_plan.data(), binary_plan.size(), &plan);
    assert(status.error_code == Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(plan, blob.data(), blob.length(), &placeholderGroup);
    assert(status.error_code == Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    timestamps.clear();
    timestamps.push_back(1);

    std::vector<CSearchResult> results;
    CSearchResult res1;
    CSearchResult res2;
    auto res = Search(segment, plan, placeholderGroup, timestamps[0], &res1);
    assert(res.error_code == Success);
    res = Search(segment, plan, placeholderGroup, timestamps[0], &res2);
    assert(res.error_code == Success);
    results.push_back(res1);
    results.push_back(res2);

    status = ReduceSearchResultsAndFillData(plan, results.data(), results.size());
    assert(status.error_code == Success);
    void* reorganize_search_result = nullptr;
    status = ReorganizeSearchResults(&reorganize_search_result, results.data(), results.size());
    assert(status.error_code == Success);
    auto hits_blob_size = GetHitsBlobSize(reorganize_search_result);
    assert(hits_blob_size > 0);
    std::vector<char> hits_blob;
    hits_blob.resize(hits_blob_size);
    GetHitsBlob(reorganize_search_result, hits_blob.data());
    assert(hits_blob.data() != nullptr);
    auto num_queries_group = GetNumQueriesPerGroup(reorganize_search_result, 0);
    assert(num_queries_group == num_queries);
    std::vector<int64_t> hit_size_per_query;
    hit_size_per_query.resize(num_queries_group);
    GetHitSizePerQueries(reorganize_search_result, 0, hit_size_per_query.data());
    assert(hit_size_per_query[0] > 0);

    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchResult(res1);
    DeleteSearchResult(res2);
    DeleteMarshaledHits(reorganize_search_result);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, LoadIndexInfo) {
    // generator index
    constexpr auto TOPK = 10;

    auto N = 1024 * 10;
    auto [raw_data, timestamps, uids] = generate_data(N);
    auto indexing = std::make_shared<milvus::knowhere::IVFPQ>();
    auto conf = milvus::knowhere::Config{{milvus::knowhere::meta::DIM, DIM},
                                         {milvus::knowhere::meta::TOPK, TOPK},
                                         {milvus::knowhere::IndexParams::nlist, 100},
                                         {milvus::knowhere::IndexParams::nprobe, 4},
                                         {milvus::knowhere::IndexParams::m, 4},
                                         {milvus::knowhere::IndexParams::nbits, 8},
                                         {milvus::knowhere::Metric::TYPE, milvus::knowhere::Metric::L2},
                                         {milvus::knowhere::meta::DEVICEID, 0}};

    auto database = milvus::knowhere::GenDataset(N, DIM, raw_data.data());
    indexing->Train(database, conf);
    indexing->AddWithoutIds(database, conf);
    EXPECT_EQ(indexing->Count(), N);
    EXPECT_EQ(indexing->Dim(), DIM);
    auto binary_set = indexing->Serialize(conf);
    CBinarySet c_binary_set = (CBinarySet)&binary_set;

    void* c_load_index_info = nullptr;
    auto status = NewLoadIndexInfo(&c_load_index_info);
    assert(status.error_code == Success);
    std::string index_param_key1 = "index_type";
    std::string index_param_value1 = "IVF_PQ";
    status = AppendIndexParam(c_load_index_info, index_param_key1.data(), index_param_value1.data());
    std::string index_param_key2 = "index_mode";
    std::string index_param_value2 = "cpu";
    status = AppendIndexParam(c_load_index_info, index_param_key2.data(), index_param_value2.data());
    assert(status.error_code == Success);
    std::string field_name = "field0";
    status = AppendFieldInfo(c_load_index_info, 0);
    assert(status.error_code == Success);
    status = AppendIndex(c_load_index_info, c_binary_set);
    assert(status.error_code == Success);
    DeleteLoadIndexInfo(c_load_index_info);
}

TEST(CApiTest, LoadIndex_Search) {
    // generator index
    constexpr auto TOPK = 10;

    auto N = 1024 * 1024;
    auto num_query = 100;
    auto [raw_data, timestamps, uids] = generate_data(N);
    auto indexing = std::make_shared<milvus::knowhere::IVFPQ>();
    auto conf = milvus::knowhere::Config{{milvus::knowhere::meta::DIM, DIM},
                                         {milvus::knowhere::meta::TOPK, TOPK},
                                         {milvus::knowhere::IndexParams::nlist, 100},
                                         {milvus::knowhere::IndexParams::nprobe, 4},
                                         {milvus::knowhere::IndexParams::m, 4},
                                         {milvus::knowhere::IndexParams::nbits, 8},
                                         {milvus::knowhere::Metric::TYPE, milvus::knowhere::Metric::L2},
                                         {milvus::knowhere::meta::DEVICEID, 0}};

    auto database = milvus::knowhere::GenDataset(N, DIM, raw_data.data());
    indexing->Train(database, conf);
    indexing->AddWithoutIds(database, conf);

    EXPECT_EQ(indexing->Count(), N);
    EXPECT_EQ(indexing->Dim(), DIM);

    // serializ index to binarySet
    auto binary_set = indexing->Serialize(conf);

    // fill loadIndexInfo
    LoadIndexInfo load_index_info;
    auto& index_params = load_index_info.index_params;
    index_params["index_type"] = "IVF_PQ";
    index_params["index_mode"] = "CPU";
    auto mode = milvus::knowhere::IndexMode::MODE_CPU;
    load_index_info.index =
        milvus::knowhere::VecIndexFactory::GetInstance().CreateVecIndex(index_params["index_type"], mode);
    load_index_info.index->Load(binary_set);

    // search
    auto query_dataset = milvus::knowhere::GenDataset(num_query, DIM, raw_data.data() + DIM * 4200);

    auto result = indexing->Query(query_dataset, conf, nullptr);

    auto ids = result->Get<int64_t*>(milvus::knowhere::meta::IDS);
    auto dis = result->Get<float*>(milvus::knowhere::meta::DISTANCE);
    // for (int i = 0; i < std::min(num_query * K, 100); ++i) {
    //    std::cout << ids[i] << "->" << dis[i] << std::endl;
    //}
}

TEST(CApiTest, Indexing_Without_Predicate) {
    // insert data to segment
    constexpr auto TOPK = 5;

    std::string schema_string = generate_collection_schema("L2", DIM, false);
    auto collection = NewCollection(schema_string.c_str());
    auto schema = ((segcore::Collection*)collection)->get_schema();
    auto segment = NewSegment(collection, 0, Growing);

    auto N = ROW_COUNT;
    auto dataset = DataGen(schema, N);
    auto vec_col = dataset.get_col<float>(0);
    auto query_ptr = vec_col.data() + 42000 * DIM;

    int64_t offset;
    PreInsert(segment, N, &offset);
    auto ins_res = Insert(segment, offset, N, dataset.row_ids_.data(), dataset.timestamps_.data(),
                          dataset.raw_.raw_data, dataset.raw_.sizeof_per_row, dataset.raw_.count);
    assert(ins_res.error_code == Success);

    const char* dsl_string = R"(
     {
         "bool": {
             "vector": {
                 "fakevec": {
                     "metric_type": "L2",
                     "params": {
                         "nprobe": 10
                     },
                     "query": "$0",
                     "topk": 5,
                     "round_decimal": -1
                 }
             }
         }
     })";

    // create place_holder_group
    int num_queries = 5;
    auto raw_group = CreatePlaceholderGroupFromBlob(num_queries, DIM, query_ptr);
    auto blob = raw_group.SerializeAsString();

    // search on segment's small index
    void* plan = nullptr;
    auto status = CreateSearchPlan(collection, dsl_string, &plan);
    assert(status.error_code == Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(plan, blob.data(), blob.length(), &placeholderGroup);
    assert(status.error_code == Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    Timestamp time = 10000000;

    CSearchResult c_search_result_on_smallIndex;
    auto res_before_load_index = Search(segment, plan, placeholderGroup, time, &c_search_result_on_smallIndex);
    assert(res_before_load_index.error_code == Success);

    // load index to segment
    auto conf = milvus::knowhere::Config{{milvus::knowhere::meta::DIM, DIM},
                                         {milvus::knowhere::meta::TOPK, TOPK},
                                         {milvus::knowhere::IndexParams::nlist, 100},
                                         {milvus::knowhere::IndexParams::nprobe, 10},
                                         {milvus::knowhere::IndexParams::m, 4},
                                         {milvus::knowhere::IndexParams::nbits, 8},
                                         {milvus::knowhere::Metric::TYPE, milvus::knowhere::Metric::L2},
                                         {milvus::knowhere::meta::DEVICEID, 0}};
    auto indexing = generate_index(vec_col.data(), conf, DIM, TOPK, N, IndexEnum::INDEX_FAISS_IVFPQ);

    // gen query dataset
    auto query_dataset = milvus::knowhere::GenDataset(num_queries, DIM, query_ptr);
    auto result_on_index = indexing->Query(query_dataset, conf, nullptr);
    auto ids = result_on_index->Get<int64_t*>(milvus::knowhere::meta::IDS);
    auto dis = result_on_index->Get<float*>(milvus::knowhere::meta::DISTANCE);
    std::vector<int64_t> vec_ids(ids, ids + TOPK * num_queries);
    std::vector<float> vec_dis;
    for (int j = 0; j < TOPK * num_queries; ++j) {
        vec_dis.push_back(dis[j] * -1);
    }

    auto search_result_on_raw_index = (SearchResult*)c_search_result_on_smallIndex;
    search_result_on_raw_index->internal_seg_offsets_ = vec_ids;
    search_result_on_raw_index->result_distances_ = vec_dis;

    auto binary_set = indexing->Serialize(conf);
    void* c_load_index_info = nullptr;
    status = NewLoadIndexInfo(&c_load_index_info);
    assert(status.error_code == Success);
    std::string index_type_key = "index_type";
    std::string index_type_value = "IVF_PQ";
    std::string index_mode_key = "index_mode";
    std::string index_mode_value = "cpu";
    std::string metric_type_key = "metric_type";
    std::string metric_type_value = "L2";

    AppendIndexParam(c_load_index_info, index_type_key.c_str(), index_type_value.c_str());
    AppendIndexParam(c_load_index_info, index_mode_key.c_str(), index_mode_value.c_str());
    AppendIndexParam(c_load_index_info, metric_type_key.c_str(), metric_type_value.c_str());
    AppendFieldInfo(c_load_index_info, 100);
    AppendIndex(c_load_index_info, (CBinarySet)&binary_set);

    auto sealed_segment = SealedCreator(schema, dataset, *(LoadIndexInfo*)c_load_index_info);
    CSearchResult c_search_result_on_bigIndex;
    auto res_after_load_index =
        Search(sealed_segment.get(), plan, placeholderGroup, time, &c_search_result_on_bigIndex);
    assert(res_after_load_index.error_code == Success);

    auto search_result_on_raw_index_json = SearchResultToJson(*search_result_on_raw_index);
    auto search_result_on_bigIndex_json = SearchResultToJson((*(SearchResult*)c_search_result_on_bigIndex));
    // std::cout << search_result_on_raw_index_json.dump(1) << std::endl;
    // std::cout << search_result_on_bigIndex_json.dump(1) << std::endl;

    ASSERT_EQ(search_result_on_raw_index_json.dump(1), search_result_on_bigIndex_json.dump(1));

    DeleteLoadIndexInfo(c_load_index_info);
    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchResult(c_search_result_on_smallIndex);
    DeleteSearchResult(c_search_result_on_bigIndex);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, Indexing_Expr_Without_Predicate) {
    // insert data to segment
    constexpr auto TOPK = 5;

    std::string schema_string = generate_collection_schema("L2", DIM, false);
    auto collection = NewCollection(schema_string.c_str());
    auto schema = ((segcore::Collection*)collection)->get_schema();
    auto segment = NewSegment(collection, 0, Growing);

    auto N = ROW_COUNT;
    auto dataset = DataGen(schema, N);
    auto vec_col = dataset.get_col<float>(0);
    auto query_ptr = vec_col.data() + 42000 * DIM;

    int64_t offset;
    PreInsert(segment, N, &offset);
    auto ins_res = Insert(segment, offset, N, dataset.row_ids_.data(), dataset.timestamps_.data(),
                          dataset.raw_.raw_data, dataset.raw_.sizeof_per_row, dataset.raw_.count);
    assert(ins_res.error_code == Success);

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
    auto raw_group = CreatePlaceholderGroupFromBlob(num_queries, DIM, query_ptr);
    auto blob = raw_group.SerializeAsString();

    // search on segment's small index
    void* plan = nullptr;
    auto binary_plan = translate_text_plan_to_binary_plan(serialized_expr_plan);
    auto status = CreateSearchPlanByExpr(collection, binary_plan.data(), binary_plan.size(), &plan);
    assert(status.error_code == Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(plan, blob.data(), blob.length(), &placeholderGroup);
    assert(status.error_code == Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    Timestamp time = 10000000;

    CSearchResult c_search_result_on_smallIndex;
    auto res_before_load_index = Search(segment, plan, placeholderGroup, time, &c_search_result_on_smallIndex);
    assert(res_before_load_index.error_code == Success);

    // load index to segment
    auto conf = milvus::knowhere::Config{{milvus::knowhere::meta::DIM, DIM},
                                         {milvus::knowhere::meta::TOPK, TOPK},
                                         {milvus::knowhere::IndexParams::nlist, 100},
                                         {milvus::knowhere::IndexParams::nprobe, 10},
                                         {milvus::knowhere::IndexParams::m, 4},
                                         {milvus::knowhere::IndexParams::nbits, 8},
                                         {milvus::knowhere::Metric::TYPE, milvus::knowhere::Metric::L2},
                                         {milvus::knowhere::meta::DEVICEID, 0}};
    auto indexing = generate_index(vec_col.data(), conf, DIM, TOPK, N, IndexEnum::INDEX_FAISS_IVFPQ);

    // gen query dataset
    auto query_dataset = milvus::knowhere::GenDataset(num_queries, DIM, query_ptr);
    auto result_on_index = indexing->Query(query_dataset, conf, nullptr);
    auto ids = result_on_index->Get<int64_t*>(milvus::knowhere::meta::IDS);
    auto dis = result_on_index->Get<float*>(milvus::knowhere::meta::DISTANCE);
    std::vector<int64_t> vec_ids(ids, ids + TOPK * num_queries);
    std::vector<float> vec_dis;
    for (int j = 0; j < TOPK * num_queries; ++j) {
        vec_dis.push_back(dis[j] * -1);
    }

    auto search_result_on_raw_index = (SearchResult*)c_search_result_on_smallIndex;
    search_result_on_raw_index->internal_seg_offsets_ = vec_ids;
    search_result_on_raw_index->result_distances_ = vec_dis;

    auto binary_set = indexing->Serialize(conf);
    void* c_load_index_info = nullptr;
    status = NewLoadIndexInfo(&c_load_index_info);
    assert(status.error_code == Success);
    std::string index_type_key = "index_type";
    std::string index_type_value = "IVF_PQ";
    std::string index_mode_key = "index_mode";
    std::string index_mode_value = "cpu";
    std::string metric_type_key = "metric_type";
    std::string metric_type_value = "L2";

    AppendIndexParam(c_load_index_info, index_type_key.c_str(), index_type_value.c_str());
    AppendIndexParam(c_load_index_info, index_mode_key.c_str(), index_mode_value.c_str());
    AppendIndexParam(c_load_index_info, metric_type_key.c_str(), metric_type_value.c_str());
    AppendFieldInfo(c_load_index_info, 100);
    AppendIndex(c_load_index_info, (CBinarySet)&binary_set);

    auto sealed_segment = SealedCreator(schema, dataset, *(LoadIndexInfo*)c_load_index_info);
    CSearchResult c_search_result_on_bigIndex;
    auto res_after_load_index =
        Search(sealed_segment.get(), plan, placeholderGroup, time, &c_search_result_on_bigIndex);
    assert(res_after_load_index.error_code == Success);

    auto search_result_on_raw_index_json = SearchResultToJson(*search_result_on_raw_index);
    auto search_result_on_bigIndex_json = SearchResultToJson((*(SearchResult*)c_search_result_on_bigIndex));
    // std::cout << search_result_on_raw_index_json.dump(1) << std::endl;
    // std::cout << search_result_on_bigIndex_json.dump(1) << std::endl;

    ASSERT_EQ(search_result_on_raw_index_json.dump(1), search_result_on_bigIndex_json.dump(1));

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

    std::string schema_string = generate_collection_schema("L2", DIM, false);
    auto collection = NewCollection(schema_string.c_str());
    auto schema = ((segcore::Collection*)collection)->get_schema();
    auto segment = NewSegment(collection, 0, Growing);

    auto N = ROW_COUNT;
    auto dataset = DataGen(schema, N);
    auto vec_col = dataset.get_col<float>(0);
    auto query_ptr = vec_col.data() + 42000 * DIM;

    int64_t offset;
    PreInsert(segment, N, &offset);
    auto ins_res = Insert(segment, offset, N, dataset.row_ids_.data(), dataset.timestamps_.data(),
                          dataset.raw_.raw_data, dataset.raw_.sizeof_per_row, dataset.raw_.count);
    assert(ins_res.error_code == Success);

    const char* dsl_string = R"({
         "bool": {
             "must": [
             {
                 "range": {
                     "counter": {
                         "GE": 42000,
                         "LT": 42010
                     }
                 }
             },
             {
                 "vector": {
                     "fakevec": {
                         "metric_type": "L2",
                         "params": {
                             "nprobe": 10
                         },
                         "query": "$0",
                         "topk": 5,
                         "round_decimal": -1

                     }
                 }
             }
             ]
         }
     })";

    // create place_holder_group
    int num_queries = 10;
    auto raw_group = CreatePlaceholderGroupFromBlob(num_queries, DIM, query_ptr);
    auto blob = raw_group.SerializeAsString();

    // search on segment's small index
    void* plan = nullptr;
    auto status = CreateSearchPlan(collection, dsl_string, &plan);
    assert(status.error_code == Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(plan, blob.data(), blob.length(), &placeholderGroup);
    assert(status.error_code == Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    Timestamp time = 10000000;

    CSearchResult c_search_result_on_smallIndex;
    auto res_before_load_index = Search(segment, plan, placeholderGroup, time, &c_search_result_on_smallIndex);
    assert(res_before_load_index.error_code == Success);

    // load index to segment
    auto conf = milvus::knowhere::Config{{milvus::knowhere::meta::DIM, DIM},
                                         {milvus::knowhere::meta::TOPK, TOPK},
                                         {milvus::knowhere::IndexParams::nlist, 100},
                                         {milvus::knowhere::IndexParams::nprobe, 10},
                                         {milvus::knowhere::IndexParams::m, 4},
                                         {milvus::knowhere::IndexParams::nbits, 8},
                                         {milvus::knowhere::Metric::TYPE, milvus::knowhere::Metric::L2},
                                         {milvus::knowhere::meta::DEVICEID, 0}};

    auto indexing = generate_index(vec_col.data(), conf, DIM, TOPK, N, IndexEnum::INDEX_FAISS_IVFPQ);

    // gen query dataset
    auto query_dataset = milvus::knowhere::GenDataset(num_queries, DIM, query_ptr);
    auto result_on_index = indexing->Query(query_dataset, conf, nullptr);
    auto ids = result_on_index->Get<int64_t*>(milvus::knowhere::meta::IDS);
    auto dis = result_on_index->Get<float*>(milvus::knowhere::meta::DISTANCE);
    std::vector<int64_t> vec_ids(ids, ids + TOPK * num_queries);
    std::vector<float> vec_dis;
    for (int j = 0; j < TOPK * num_queries; ++j) {
        vec_dis.push_back(dis[j] * -1);
    }

    auto search_result_on_raw_index = (SearchResult*)c_search_result_on_smallIndex;
    search_result_on_raw_index->internal_seg_offsets_ = vec_ids;
    search_result_on_raw_index->result_distances_ = vec_dis;

    auto binary_set = indexing->Serialize(conf);
    void* c_load_index_info = nullptr;
    status = NewLoadIndexInfo(&c_load_index_info);
    assert(status.error_code == Success);
    std::string index_type_key = "index_type";
    std::string index_type_value = "IVF_PQ";
    std::string index_mode_key = "index_mode";
    std::string index_mode_value = "cpu";
    std::string metric_type_key = "metric_type";
    std::string metric_type_value = "L2";

    AppendIndexParam(c_load_index_info, index_type_key.c_str(), index_type_value.c_str());
    AppendIndexParam(c_load_index_info, index_mode_key.c_str(), index_mode_value.c_str());
    AppendIndexParam(c_load_index_info, metric_type_key.c_str(), metric_type_value.c_str());
    AppendFieldInfo(c_load_index_info, 100);
    AppendIndex(c_load_index_info, (CBinarySet)&binary_set);

    auto sealed_segment = SealedCreator(schema, dataset, *(LoadIndexInfo*)c_load_index_info);
    CSearchResult c_search_result_on_bigIndex;
    auto res_after_load_index =
        Search(sealed_segment.get(), plan, placeholderGroup, time, &c_search_result_on_bigIndex);
    assert(res_after_load_index.error_code == Success);

    auto search_result_on_bigIndex = (*(SearchResult*)c_search_result_on_bigIndex);
    for (int i = 0; i < num_queries; ++i) {
        auto offset = i * TOPK;
        ASSERT_EQ(search_result_on_bigIndex.internal_seg_offsets_[offset], 42000 + i);
        ASSERT_EQ(search_result_on_bigIndex.result_distances_[offset],
                  search_result_on_raw_index->result_distances_[offset]);
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

    std::string schema_string = generate_collection_schema("L2", DIM, false);
    auto collection = NewCollection(schema_string.c_str());
    auto schema = ((segcore::Collection*)collection)->get_schema();
    auto segment = NewSegment(collection, 0, Growing);

    auto N = 1000 * 1000;
    auto dataset = DataGen(schema, N);
    auto vec_col = dataset.get_col<float>(0);
    auto query_ptr = vec_col.data() + 420000 * DIM;

    {
        int64_t offset;
        PreInsert(segment, N, &offset);
        auto ins_res = Insert(segment, offset, N, dataset.row_ids_.data(), dataset.timestamps_.data(),
                              dataset.raw_.raw_data, dataset.raw_.sizeof_per_row, dataset.raw_.count);
        assert(ins_res.error_code == Success);
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
                                                       int64_val: 420000
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
                                                       int64_val: 420010
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
    auto raw_group = CreatePlaceholderGroupFromBlob(num_queries, DIM, query_ptr);
    auto blob = raw_group.SerializeAsString();

    // search on segment's small index
    void* plan = nullptr;
    auto binary_plan = translate_text_plan_to_binary_plan(serialized_expr_plan);
    auto status = CreateSearchPlanByExpr(collection, binary_plan.data(), binary_plan.size(), &plan);
    assert(status.error_code == Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(plan, blob.data(), blob.length(), &placeholderGroup);
    assert(status.error_code == Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    Timestamp time = 10000000;

    CSearchResult c_search_result_on_smallIndex;
    auto res_before_load_index = Search(segment, plan, placeholderGroup, time, &c_search_result_on_smallIndex);
    assert(res_before_load_index.error_code == Success);

    // load index to segment
    auto conf = milvus::knowhere::Config{{milvus::knowhere::meta::DIM, DIM},
                                         {milvus::knowhere::meta::TOPK, TOPK},
                                         {milvus::knowhere::IndexParams::nlist, 100},
                                         {milvus::knowhere::IndexParams::nprobe, 10},
                                         {milvus::knowhere::IndexParams::m, 4},
                                         {milvus::knowhere::IndexParams::nbits, 8},
                                         {milvus::knowhere::Metric::TYPE, milvus::knowhere::Metric::L2},
                                         {milvus::knowhere::meta::DEVICEID, 0}};

    auto indexing = generate_index(vec_col.data(), conf, DIM, TOPK, N, IndexEnum::INDEX_FAISS_IVFPQ);

    // gen query dataset
    auto query_dataset = milvus::knowhere::GenDataset(num_queries, DIM, query_ptr);
    auto result_on_index = indexing->Query(query_dataset, conf, nullptr);
    auto ids = result_on_index->Get<int64_t*>(milvus::knowhere::meta::IDS);
    auto dis = result_on_index->Get<float*>(milvus::knowhere::meta::DISTANCE);
    std::vector<int64_t> vec_ids(ids, ids + TOPK * num_queries);
    std::vector<float> vec_dis;
    for (int j = 0; j < TOPK * num_queries; ++j) {
        vec_dis.push_back(dis[j] * -1);
    }

    auto search_result_on_raw_index = (SearchResult*)c_search_result_on_smallIndex;
    search_result_on_raw_index->internal_seg_offsets_ = vec_ids;
    search_result_on_raw_index->result_distances_ = vec_dis;

    auto binary_set = indexing->Serialize(conf);
    void* c_load_index_info = nullptr;
    status = NewLoadIndexInfo(&c_load_index_info);
    assert(status.error_code == Success);
    std::string index_type_key = "index_type";
    std::string index_type_value = "IVF_PQ";
    std::string index_mode_key = "index_mode";
    std::string index_mode_value = "cpu";
    std::string metric_type_key = "metric_type";
    std::string metric_type_value = "L2";

    AppendIndexParam(c_load_index_info, index_type_key.c_str(), index_type_value.c_str());
    AppendIndexParam(c_load_index_info, index_mode_key.c_str(), index_mode_value.c_str());
    AppendIndexParam(c_load_index_info, metric_type_key.c_str(), metric_type_value.c_str());
    AppendFieldInfo(c_load_index_info, 100);
    AppendIndex(c_load_index_info, (CBinarySet)&binary_set);

    auto sealed_segment = SealedCreator(schema, dataset, *(LoadIndexInfo*)c_load_index_info);
    CSearchResult c_search_result_on_bigIndex;
    auto res_after_load_index =
        Search(sealed_segment.get(), plan, placeholderGroup, time, &c_search_result_on_bigIndex);
    assert(res_after_load_index.error_code == Success);

    auto search_result_on_bigIndex = (*(SearchResult*)c_search_result_on_bigIndex);
    for (int i = 0; i < num_queries; ++i) {
        auto offset = i * TOPK;
        ASSERT_EQ(search_result_on_bigIndex.internal_seg_offsets_[offset], 420000 + i);
        ASSERT_EQ(search_result_on_bigIndex.result_distances_[offset],
                  search_result_on_raw_index->result_distances_[offset]);
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

    std::string schema_string = generate_collection_schema("L2", DIM, false);
    auto collection = NewCollection(schema_string.c_str());
    auto schema = ((segcore::Collection*)collection)->get_schema();
    auto segment = NewSegment(collection, 0, Growing);

    auto N = ROW_COUNT;
    auto dataset = DataGen(schema, N);
    auto vec_col = dataset.get_col<float>(0);
    auto query_ptr = vec_col.data() + 42000 * DIM;

    int64_t offset;
    PreInsert(segment, N, &offset);
    auto ins_res = Insert(segment, offset, N, dataset.row_ids_.data(), dataset.timestamps_.data(),
                          dataset.raw_.raw_data, dataset.raw_.sizeof_per_row, dataset.raw_.count);
    assert(ins_res.error_code == Success);

    const char* dsl_string = R"({
         "bool": {
             "must": [
             {
                 "term": {
                     "counter": {
                         "values": [42000, 42001, 42002, 42003, 42004]
                     }
                 }
             },
             {
                 "vector": {
                     "fakevec": {
                         "metric_type": "L2",
                         "params": {
                             "nprobe": 10
                         },
                         "query": "$0",
                         "topk": 5,
                         "round_decimal": -1
                     }
                 }
             }
             ]
         }
     })";

    // create place_holder_group
    int num_queries = 5;
    auto raw_group = CreatePlaceholderGroupFromBlob(num_queries, DIM, query_ptr);
    auto blob = raw_group.SerializeAsString();

    // search on segment's small index
    void* plan = nullptr;
    auto status = CreateSearchPlan(collection, dsl_string, &plan);
    assert(status.error_code == Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(plan, blob.data(), blob.length(), &placeholderGroup);
    assert(status.error_code == Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    Timestamp time = 10000000;

    CSearchResult c_search_result_on_smallIndex;
    auto res_before_load_index = Search(segment, plan, placeholderGroup, time, &c_search_result_on_smallIndex);
    assert(res_before_load_index.error_code == Success);

    // load index to segment
    auto conf = milvus::knowhere::Config{{milvus::knowhere::meta::DIM, DIM},
                                         {milvus::knowhere::meta::TOPK, TOPK},
                                         {milvus::knowhere::IndexParams::nlist, 100},
                                         {milvus::knowhere::IndexParams::nprobe, 10},
                                         {milvus::knowhere::IndexParams::m, 4},
                                         {milvus::knowhere::IndexParams::nbits, 8},
                                         {milvus::knowhere::Metric::TYPE, milvus::knowhere::Metric::L2},
                                         {milvus::knowhere::meta::DEVICEID, 0}};

    auto indexing = generate_index(vec_col.data(), conf, DIM, TOPK, N, IndexEnum::INDEX_FAISS_IVFPQ);

    // gen query dataset
    auto query_dataset = milvus::knowhere::GenDataset(num_queries, DIM, query_ptr);
    auto result_on_index = indexing->Query(query_dataset, conf, nullptr);
    auto ids = result_on_index->Get<int64_t*>(milvus::knowhere::meta::IDS);
    auto dis = result_on_index->Get<float*>(milvus::knowhere::meta::DISTANCE);
    std::vector<int64_t> vec_ids(ids, ids + TOPK * num_queries);
    std::vector<float> vec_dis;
    for (int j = 0; j < TOPK * num_queries; ++j) {
        vec_dis.push_back(dis[j] * -1);
    }

    auto search_result_on_raw_index = (SearchResult*)c_search_result_on_smallIndex;
    search_result_on_raw_index->internal_seg_offsets_ = vec_ids;
    search_result_on_raw_index->result_distances_ = vec_dis;

    auto binary_set = indexing->Serialize(conf);
    void* c_load_index_info = nullptr;
    status = NewLoadIndexInfo(&c_load_index_info);
    assert(status.error_code == Success);
    std::string index_type_key = "index_type";
    std::string index_type_value = "IVF_PQ";
    std::string index_mode_key = "index_mode";
    std::string index_mode_value = "cpu";
    std::string metric_type_key = "metric_type";
    std::string metric_type_value = "L2";

    AppendIndexParam(c_load_index_info, index_type_key.c_str(), index_type_value.c_str());
    AppendIndexParam(c_load_index_info, index_mode_key.c_str(), index_mode_value.c_str());
    AppendIndexParam(c_load_index_info, metric_type_key.c_str(), metric_type_value.c_str());
    AppendFieldInfo(c_load_index_info, 100);
    AppendIndex(c_load_index_info, (CBinarySet)&binary_set);

    auto sealed_segment = SealedCreator(schema, dataset, *(LoadIndexInfo*)c_load_index_info);
    CSearchResult c_search_result_on_bigIndex;
    auto res_after_load_index =
        Search(sealed_segment.get(), plan, placeholderGroup, time, &c_search_result_on_bigIndex);
    assert(res_after_load_index.error_code == Success);

    auto search_result_on_bigIndex = (*(SearchResult*)c_search_result_on_bigIndex);
    for (int i = 0; i < num_queries; ++i) {
        auto offset = i * TOPK;
        ASSERT_EQ(search_result_on_bigIndex.internal_seg_offsets_[offset], 42000 + i);
        ASSERT_EQ(search_result_on_bigIndex.result_distances_[offset],
                  search_result_on_raw_index->result_distances_[offset]);
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

    std::string schema_string = generate_collection_schema("L2", DIM, false);
    auto collection = NewCollection(schema_string.c_str());
    auto schema = ((segcore::Collection*)collection)->get_schema();
    auto segment = NewSegment(collection, 0, Growing);

    auto N = 1000 * 1000;
    auto dataset = DataGen(schema, N);
    auto vec_col = dataset.get_col<float>(0);
    auto query_ptr = vec_col.data() + 420000 * DIM;

    int64_t offset;
    PreInsert(segment, N, &offset);
    auto ins_res = Insert(segment, offset, N, dataset.row_ids_.data(), dataset.timestamps_.data(),
                          dataset.raw_.raw_data, dataset.raw_.sizeof_per_row, dataset.raw_.count);
    assert(ins_res.error_code == Success);

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
         int64_val: 420000
       >
       values: <
         int64_val: 420001
       >
       values: <
         int64_val: 420002
       >
       values: <
         int64_val: 420003
       >
       values: <
         int64_val: 420004
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
    auto raw_group = CreatePlaceholderGroupFromBlob(num_queries, DIM, query_ptr);
    auto blob = raw_group.SerializeAsString();

    // search on segment's small index
    void* plan = nullptr;
    auto binary_plan = translate_text_plan_to_binary_plan(serialized_expr_plan);
    auto status = CreateSearchPlanByExpr(collection, binary_plan.data(), binary_plan.size(), &plan);
    assert(status.error_code == Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(plan, blob.data(), blob.length(), &placeholderGroup);
    assert(status.error_code == Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    Timestamp time = 10000000;

    CSearchResult c_search_result_on_smallIndex;
    auto res_before_load_index = Search(segment, plan, placeholderGroup, time, &c_search_result_on_smallIndex);
    assert(res_before_load_index.error_code == Success);

    // load index to segment
    auto conf = milvus::knowhere::Config{{milvus::knowhere::meta::DIM, DIM},
                                         {milvus::knowhere::meta::TOPK, TOPK},
                                         {milvus::knowhere::IndexParams::nlist, 100},
                                         {milvus::knowhere::IndexParams::nprobe, 10},
                                         {milvus::knowhere::IndexParams::m, 4},
                                         {milvus::knowhere::IndexParams::nbits, 8},
                                         {milvus::knowhere::Metric::TYPE, milvus::knowhere::Metric::L2},
                                         {milvus::knowhere::meta::DEVICEID, 0}};

    auto indexing = generate_index(vec_col.data(), conf, DIM, TOPK, N, IndexEnum::INDEX_FAISS_IVFPQ);

    // gen query dataset
    auto query_dataset = milvus::knowhere::GenDataset(num_queries, DIM, query_ptr);
    auto result_on_index = indexing->Query(query_dataset, conf, nullptr);
    auto ids = result_on_index->Get<int64_t*>(milvus::knowhere::meta::IDS);
    auto dis = result_on_index->Get<float*>(milvus::knowhere::meta::DISTANCE);
    std::vector<int64_t> vec_ids(ids, ids + TOPK * num_queries);
    std::vector<float> vec_dis;
    for (int j = 0; j < TOPK * num_queries; ++j) {
        vec_dis.push_back(dis[j] * -1);
    }

    auto search_result_on_raw_index = (SearchResult*)c_search_result_on_smallIndex;
    search_result_on_raw_index->internal_seg_offsets_ = vec_ids;
    search_result_on_raw_index->result_distances_ = vec_dis;

    auto binary_set = indexing->Serialize(conf);
    void* c_load_index_info = nullptr;
    status = NewLoadIndexInfo(&c_load_index_info);
    assert(status.error_code == Success);
    std::string index_type_key = "index_type";
    std::string index_type_value = "IVF_PQ";
    std::string index_mode_key = "index_mode";
    std::string index_mode_value = "cpu";
    std::string metric_type_key = "metric_type";
    std::string metric_type_value = "L2";

    AppendIndexParam(c_load_index_info, index_type_key.c_str(), index_type_value.c_str());
    AppendIndexParam(c_load_index_info, index_mode_key.c_str(), index_mode_value.c_str());
    AppendIndexParam(c_load_index_info, metric_type_key.c_str(), metric_type_value.c_str());
    AppendFieldInfo(c_load_index_info, 100);
    AppendIndex(c_load_index_info, (CBinarySet)&binary_set);

    auto sealed_segment = SealedCreator(schema, dataset, *(LoadIndexInfo*)c_load_index_info);
    CSearchResult c_search_result_on_bigIndex;
    auto res_after_load_index =
        Search(sealed_segment.get(), plan, placeholderGroup, time, &c_search_result_on_bigIndex);
    assert(res_after_load_index.error_code == Success);

    auto search_result_on_bigIndex = (*(SearchResult*)c_search_result_on_bigIndex);
    for (int i = 0; i < num_queries; ++i) {
        auto offset = i * TOPK;
        ASSERT_EQ(search_result_on_bigIndex.internal_seg_offsets_[offset], 420000 + i);
        ASSERT_EQ(search_result_on_bigIndex.result_distances_[offset],
                  search_result_on_raw_index->result_distances_[offset]);
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
    // insert data to segment
    constexpr auto TOPK = 5;

    std::string schema_string = generate_collection_schema("JACCARD", DIM, true);
    auto collection = NewCollection(schema_string.c_str());
    auto schema = ((segcore::Collection*)collection)->get_schema();
    auto segment = NewSegment(collection, 0, Growing);

    auto N = 1000 * 1000;
    auto dataset = DataGen(schema, N);
    auto vec_col = dataset.get_col<uint8_t>(0);
    auto query_ptr = vec_col.data() + 420000 * DIM / 8;

    int64_t offset;
    PreInsert(segment, N, &offset);
    auto ins_res = Insert(segment, offset, N, dataset.row_ids_.data(), dataset.timestamps_.data(),
                          dataset.raw_.raw_data, dataset.raw_.sizeof_per_row, dataset.raw_.count);
    assert(ins_res.error_code == Success);

    const char* dsl_string = R"({
         "bool": {
             "must": [
             {
                 "range": {
                     "counter": {
                         "GE": 420000,
                         "LT": 420010
                     }
                 }
             },
             {
                 "vector": {
                     "fakevec": {
                         "metric_type": "JACCARD",
                         "params": {
                             "nprobe": 10
                         },
                         "query": "$0",
                         "topk": 5,
                         "round_decimal": -1
                     }
                 }
             }
             ]
         }
     })";

    // create place_holder_group
    int num_queries = 5;
    auto raw_group = CreateBinaryPlaceholderGroupFromBlob(num_queries, DIM, query_ptr);
    auto blob = raw_group.SerializeAsString();

    // search on segment's small index
    void* plan = nullptr;
    auto status = CreateSearchPlan(collection, dsl_string, &plan);
    assert(status.error_code == Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(plan, blob.data(), blob.length(), &placeholderGroup);
    assert(status.error_code == Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    Timestamp time = 10000000;

    CSearchResult c_search_result_on_smallIndex;
    auto res_before_load_index = Search(segment, plan, placeholderGroup, time, &c_search_result_on_smallIndex);
    assert(res_before_load_index.error_code == Success);

    // load index to segment
    auto conf = milvus::knowhere::Config{
        {milvus::knowhere::meta::DIM, DIM},
        {milvus::knowhere::meta::TOPK, TOPK},
        {milvus::knowhere::IndexParams::nprobe, 10},
        {milvus::knowhere::IndexParams::nlist, 100},
        {milvus::knowhere::IndexParams::m, 4},
        {milvus::knowhere::IndexParams::nbits, 8},
        {milvus::knowhere::Metric::TYPE, milvus::knowhere::Metric::JACCARD},
    };

    auto indexing = generate_index(vec_col.data(), conf, DIM, TOPK, N, IndexEnum::INDEX_FAISS_BIN_IVFFLAT);

    // gen query dataset
    auto query_dataset = milvus::knowhere::GenDataset(num_queries, DIM, query_ptr);
    auto result_on_index = indexing->Query(query_dataset, conf, nullptr);
    auto ids = result_on_index->Get<int64_t*>(milvus::knowhere::meta::IDS);
    auto dis = result_on_index->Get<float*>(milvus::knowhere::meta::DISTANCE);
    std::vector<int64_t> vec_ids(ids, ids + TOPK * num_queries);
    std::vector<float> vec_dis;
    for (int j = 0; j < TOPK * num_queries; ++j) {
        vec_dis.push_back(dis[j] * -1);
    }

    auto search_result_on_raw_index = (SearchResult*)c_search_result_on_smallIndex;
    search_result_on_raw_index->internal_seg_offsets_ = vec_ids;
    search_result_on_raw_index->result_distances_ = vec_dis;

    auto binary_set = indexing->Serialize(conf);
    void* c_load_index_info = nullptr;
    status = NewLoadIndexInfo(&c_load_index_info);
    assert(status.error_code == Success);
    std::string index_type_key = "index_type";
    std::string index_type_value = "BIN_IVF_FLAT";
    std::string index_mode_key = "index_mode";
    std::string index_mode_value = "cpu";
    std::string metric_type_key = "metric_type";
    std::string metric_type_value = "JACCARD";

    AppendIndexParam(c_load_index_info, index_type_key.c_str(), index_type_value.c_str());
    AppendIndexParam(c_load_index_info, index_mode_key.c_str(), index_mode_value.c_str());
    AppendIndexParam(c_load_index_info, metric_type_key.c_str(), metric_type_value.c_str());
    AppendFieldInfo(c_load_index_info, 100);
    AppendIndex(c_load_index_info, (CBinarySet)&binary_set);

    auto sealed_segment = SealedCreator(schema, dataset, *(LoadIndexInfo*)c_load_index_info);
    CSearchResult c_search_result_on_bigIndex;
    auto res_after_load_index =
        Search(sealed_segment.get(), plan, placeholderGroup, time, &c_search_result_on_bigIndex);
    assert(res_after_load_index.error_code == Success);

    auto search_result_on_bigIndex = (*(SearchResult*)c_search_result_on_bigIndex);
    for (int i = 0; i < num_queries; ++i) {
        auto offset = i * TOPK;
        ASSERT_EQ(search_result_on_bigIndex.internal_seg_offsets_[offset], 420000 + i);
        ASSERT_EQ(search_result_on_bigIndex.result_distances_[offset],
                  search_result_on_raw_index->result_distances_[offset]);
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

    std::string schema_string = generate_collection_schema("JACCARD", DIM, true);
    auto collection = NewCollection(schema_string.c_str());
    auto schema = ((segcore::Collection*)collection)->get_schema();
    auto segment = NewSegment(collection, 0, Growing);

    auto N = ROW_COUNT;
    auto dataset = DataGen(schema, N);
    auto vec_col = dataset.get_col<uint8_t>(0);
    auto query_ptr = vec_col.data() + 42000 * DIM / 8;

    int64_t offset;
    PreInsert(segment, N, &offset);
    auto ins_res = Insert(segment, offset, N, dataset.row_ids_.data(), dataset.timestamps_.data(),
                          dataset.raw_.raw_data, dataset.raw_.sizeof_per_row, dataset.raw_.count);
    assert(ins_res.error_code == Success);

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
                                                      int64_val: 42000
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
                                                      int64_val: 42010
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
    auto raw_group = CreateBinaryPlaceholderGroupFromBlob(num_queries, DIM, query_ptr);
    auto blob = raw_group.SerializeAsString();

    // search on segment's small index
    void* plan = nullptr;
    auto binary_plan = translate_text_plan_to_binary_plan(serialized_expr_plan);
    auto status = CreateSearchPlanByExpr(collection, binary_plan.data(), binary_plan.size(), &plan);
    assert(status.error_code == Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(plan, blob.data(), blob.length(), &placeholderGroup);
    assert(status.error_code == Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    Timestamp time = 10000000;

    CSearchResult c_search_result_on_smallIndex;
    auto res_before_load_index = Search(segment, plan, placeholderGroup, time, &c_search_result_on_smallIndex);
    ASSERT_TRUE(res_before_load_index.error_code == Success) << res_before_load_index.error_msg;

    // load index to segment
    auto conf = milvus::knowhere::Config{
        {milvus::knowhere::meta::DIM, DIM},
        {milvus::knowhere::meta::TOPK, TOPK},
        {milvus::knowhere::IndexParams::nprobe, 10},
        {milvus::knowhere::IndexParams::nlist, 100},
        {milvus::knowhere::IndexParams::m, 4},
        {milvus::knowhere::IndexParams::nbits, 8},
        {milvus::knowhere::Metric::TYPE, milvus::knowhere::Metric::JACCARD},
    };

    auto indexing = generate_index(vec_col.data(), conf, DIM, TOPK, N, IndexEnum::INDEX_FAISS_BIN_IVFFLAT);

    // gen query dataset
    auto query_dataset = milvus::knowhere::GenDataset(num_queries, DIM, query_ptr);
    auto result_on_index = indexing->Query(query_dataset, conf, nullptr);
    auto ids = result_on_index->Get<int64_t*>(milvus::knowhere::meta::IDS);
    auto dis = result_on_index->Get<float*>(milvus::knowhere::meta::DISTANCE);
    std::vector<int64_t> vec_ids(ids, ids + TOPK * num_queries);
    std::vector<float> vec_dis;
    for (int j = 0; j < TOPK * num_queries; ++j) {
        vec_dis.push_back(dis[j] * -1);
    }

    auto search_result_on_raw_index = (SearchResult*)c_search_result_on_smallIndex;
    search_result_on_raw_index->internal_seg_offsets_ = vec_ids;
    search_result_on_raw_index->result_distances_ = vec_dis;

    auto binary_set = indexing->Serialize(conf);
    void* c_load_index_info = nullptr;
    status = NewLoadIndexInfo(&c_load_index_info);
    assert(status.error_code == Success);
    std::string index_type_key = "index_type";
    std::string index_type_value = "BIN_IVF_FLAT";
    std::string index_mode_key = "index_mode";
    std::string index_mode_value = "cpu";
    std::string metric_type_key = "metric_type";
    std::string metric_type_value = "JACCARD";

    AppendIndexParam(c_load_index_info, index_type_key.c_str(), index_type_value.c_str());
    AppendIndexParam(c_load_index_info, index_mode_key.c_str(), index_mode_value.c_str());
    AppendIndexParam(c_load_index_info, metric_type_key.c_str(), metric_type_value.c_str());
    AppendFieldInfo(c_load_index_info, 100);
    AppendIndex(c_load_index_info, (CBinarySet)&binary_set);

    auto sealed_segment = SealedCreator(schema, dataset, *(LoadIndexInfo*)c_load_index_info);
    CSearchResult c_search_result_on_bigIndex;
    auto res_after_load_index =
        Search(sealed_segment.get(), plan, placeholderGroup, time, &c_search_result_on_bigIndex);
    assert(res_after_load_index.error_code == Success);

    auto search_result_on_bigIndex = (*(SearchResult*)c_search_result_on_bigIndex);
    for (int i = 0; i < num_queries; ++i) {
        auto offset = i * TOPK;
        ASSERT_EQ(search_result_on_bigIndex.internal_seg_offsets_[offset], 42000 + i);
        ASSERT_EQ(search_result_on_bigIndex.result_distances_[offset],
                  search_result_on_raw_index->result_distances_[offset]);
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

    std::string schema_string = generate_collection_schema("JACCARD", DIM, true);
    auto collection = NewCollection(schema_string.c_str());
    auto schema = ((segcore::Collection*)collection)->get_schema();
    auto segment = NewSegment(collection, 0, Growing);

    auto N = ROW_COUNT;
    auto dataset = DataGen(schema, N);
    auto vec_col = dataset.get_col<uint8_t>(0);
    auto query_ptr = vec_col.data() + 42000 * DIM / 8;

    int64_t offset;
    PreInsert(segment, N, &offset);
    auto ins_res = Insert(segment, offset, N, dataset.row_ids_.data(), dataset.timestamps_.data(),
                          dataset.raw_.raw_data, dataset.raw_.sizeof_per_row, dataset.raw_.count);
    assert(ins_res.error_code == Success);

    const char* dsl_string = R"({
        "bool": {
            "must": [
            {
                "term": {
                    "counter": {
                        "values": [42000, 42001, 42002, 42003, 42004]
                    }
                }
            },
            {
                "vector": {
                    "fakevec": {
                        "metric_type": "JACCARD",
                        "params": {
                            "nprobe": 10
                        },
                        "query": "$0",
                        "topk": 5,
                        "round_decimal": -1
                    }
                }
            }
            ]
        }
    })";

    // create place_holder_group
    int num_queries = 5;
    auto raw_group = CreateBinaryPlaceholderGroupFromBlob(num_queries, DIM, query_ptr);
    auto blob = raw_group.SerializeAsString();

    // search on segment's small index
    void* plan = nullptr;
    auto status = CreateSearchPlan(collection, dsl_string, &plan);
    assert(status.error_code == Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(plan, blob.data(), blob.length(), &placeholderGroup);
    assert(status.error_code == Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    Timestamp time = 10000000;

    CSearchResult c_search_result_on_smallIndex;
    auto res_before_load_index = Search(segment, plan, placeholderGroup, time, &c_search_result_on_smallIndex);
    assert(res_before_load_index.error_code == Success);

    // load index to segment
    auto conf = milvus::knowhere::Config{
        {milvus::knowhere::meta::DIM, DIM},
        {milvus::knowhere::meta::TOPK, TOPK},
        {milvus::knowhere::IndexParams::nprobe, 10},
        {milvus::knowhere::IndexParams::nlist, 100},
        {milvus::knowhere::IndexParams::m, 4},
        {milvus::knowhere::IndexParams::nbits, 8},
        {milvus::knowhere::Metric::TYPE, milvus::knowhere::Metric::JACCARD},
    };

    auto indexing = generate_index(vec_col.data(), conf, DIM, TOPK, N, IndexEnum::INDEX_FAISS_BIN_IVFFLAT);

    // gen query dataset
    auto query_dataset = milvus::knowhere::GenDataset(num_queries, DIM, query_ptr);
    auto result_on_index = indexing->Query(query_dataset, conf, nullptr);
    auto ids = result_on_index->Get<int64_t*>(milvus::knowhere::meta::IDS);
    auto dis = result_on_index->Get<float*>(milvus::knowhere::meta::DISTANCE);
    std::vector<int64_t> vec_ids(ids, ids + TOPK * num_queries);
    std::vector<float> vec_dis;
    for (int j = 0; j < TOPK * num_queries; ++j) {
        vec_dis.push_back(dis[j] * -1);
    }

    auto search_result_on_raw_index = (SearchResult*)c_search_result_on_smallIndex;
    search_result_on_raw_index->internal_seg_offsets_ = vec_ids;
    search_result_on_raw_index->result_distances_ = vec_dis;

    auto binary_set = indexing->Serialize(conf);
    void* c_load_index_info = nullptr;
    status = NewLoadIndexInfo(&c_load_index_info);
    assert(status.error_code == Success);
    std::string index_type_key = "index_type";
    std::string index_type_value = "BIN_IVF_FLAT";
    std::string index_mode_key = "index_mode";
    std::string index_mode_value = "cpu";
    std::string metric_type_key = "metric_type";
    std::string metric_type_value = "JACCARD";

    AppendIndexParam(c_load_index_info, index_type_key.c_str(), index_type_value.c_str());
    AppendIndexParam(c_load_index_info, index_mode_key.c_str(), index_mode_value.c_str());
    AppendIndexParam(c_load_index_info, metric_type_key.c_str(), metric_type_value.c_str());
    AppendFieldInfo(c_load_index_info, 100);
    AppendIndex(c_load_index_info, (CBinarySet)&binary_set);

    auto sealed_segment = SealedCreator(schema, dataset, *(LoadIndexInfo*)c_load_index_info);
    CSearchResult c_search_result_on_bigIndex;
    auto res_after_load_index =
        Search(sealed_segment.get(), plan, placeholderGroup, time, &c_search_result_on_bigIndex);
    assert(res_after_load_index.error_code == Success);

    std::vector<CSearchResult> results;
    results.push_back(c_search_result_on_bigIndex);
    status = ReduceSearchResultsAndFillData(plan, results.data(), results.size());
    assert(status.error_code == Success);

    auto search_result_on_bigIndex = (*(SearchResult*)c_search_result_on_bigIndex);
    for (int i = 0; i < num_queries; ++i) {
        auto offset = i * TOPK;
        ASSERT_EQ(search_result_on_bigIndex.internal_seg_offsets_[offset], 42000 + i);
        ASSERT_EQ(search_result_on_bigIndex.result_distances_[offset],
                  search_result_on_raw_index->result_distances_[offset]);
    }

    DeleteLoadIndexInfo(c_load_index_info);
    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchResult(c_search_result_on_smallIndex);
    DeleteSearchResult(c_search_result_on_bigIndex);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, Indexing_Expr_With_binary_Predicate_Term) {
    // insert data to segment
    constexpr auto TOPK = 5;

    std::string schema_string = generate_collection_schema("JACCARD", DIM, true);
    auto collection = NewCollection(schema_string.c_str());
    auto schema = ((segcore::Collection*)collection)->get_schema();
    auto segment = NewSegment(collection, 0, Growing);

    auto N = ROW_COUNT;
    auto dataset = DataGen(schema, N);
    auto vec_col = dataset.get_col<uint8_t>(0);
    auto query_ptr = vec_col.data() + 42000 * DIM / 8;

    int64_t offset;
    PreInsert(segment, N, &offset);
    auto ins_res = Insert(segment, offset, N, dataset.row_ids_.data(), dataset.timestamps_.data(),
                          dataset.raw_.raw_data, dataset.raw_.sizeof_per_row, dataset.raw_.count);
    assert(ins_res.error_code == Success);

    const char* serialized_expr_plan = R"(vector_anns: <
                                            field_id: 100
                                            predicates: <
                                              term_expr: <
                                                column_info: <
                                                  field_id: 101
                                                  data_type: Int64
                                                >
                                                values: <
                                                  int64_val: 42000
                                                >
                                                values: <
                                                  int64_val: 42001
                                                >
                                                values: <
                                                  int64_val: 42002
                                                >
                                                values: <
                                                  int64_val: 42003
                                                >
                                                values: <
                                                  int64_val: 42004
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
    auto raw_group = CreateBinaryPlaceholderGroupFromBlob(num_queries, DIM, query_ptr);
    auto blob = raw_group.SerializeAsString();

    // search on segment's small index
    void* plan = nullptr;
    auto binary_plan = translate_text_plan_to_binary_plan(serialized_expr_plan);
    auto status = CreateSearchPlanByExpr(collection, binary_plan.data(), binary_plan.size(), &plan);
    assert(status.error_code == Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(plan, blob.data(), blob.length(), &placeholderGroup);
    assert(status.error_code == Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    Timestamp time = 10000000;

    CSearchResult c_search_result_on_smallIndex;
    auto res_before_load_index = Search(segment, plan, placeholderGroup, time, &c_search_result_on_smallIndex);
    assert(res_before_load_index.error_code == Success);

    // load index to segment
    auto conf = milvus::knowhere::Config{
        {milvus::knowhere::meta::DIM, DIM},
        {milvus::knowhere::meta::TOPK, TOPK},
        {milvus::knowhere::IndexParams::nprobe, 10},
        {milvus::knowhere::IndexParams::nlist, 100},
        {milvus::knowhere::IndexParams::m, 4},
        {milvus::knowhere::IndexParams::nbits, 8},
        {milvus::knowhere::Metric::TYPE, milvus::knowhere::Metric::JACCARD},
    };

    auto indexing = generate_index(vec_col.data(), conf, DIM, TOPK, N, IndexEnum::INDEX_FAISS_BIN_IVFFLAT);

    // gen query dataset
    auto query_dataset = milvus::knowhere::GenDataset(num_queries, DIM, query_ptr);
    auto result_on_index = indexing->Query(query_dataset, conf, nullptr);
    auto ids = result_on_index->Get<int64_t*>(milvus::knowhere::meta::IDS);
    auto dis = result_on_index->Get<float*>(milvus::knowhere::meta::DISTANCE);
    std::vector<int64_t> vec_ids(ids, ids + TOPK * num_queries);
    std::vector<float> vec_dis;
    for (int j = 0; j < TOPK * num_queries; ++j) {
        vec_dis.push_back(dis[j] * -1);
    }

    auto search_result_on_raw_index = (SearchResult*)c_search_result_on_smallIndex;
    search_result_on_raw_index->internal_seg_offsets_ = vec_ids;
    search_result_on_raw_index->result_distances_ = vec_dis;

    auto binary_set = indexing->Serialize(conf);
    void* c_load_index_info = nullptr;
    status = NewLoadIndexInfo(&c_load_index_info);
    assert(status.error_code == Success);
    std::string index_type_key = "index_type";
    std::string index_type_value = "BIN_IVF_FLAT";
    std::string index_mode_key = "index_mode";
    std::string index_mode_value = "cpu";
    std::string metric_type_key = "metric_type";
    std::string metric_type_value = "JACCARD";

    AppendIndexParam(c_load_index_info, index_type_key.c_str(), index_type_value.c_str());
    AppendIndexParam(c_load_index_info, index_mode_key.c_str(), index_mode_value.c_str());
    AppendIndexParam(c_load_index_info, metric_type_key.c_str(), metric_type_value.c_str());
    AppendFieldInfo(c_load_index_info, 100);
    AppendIndex(c_load_index_info, (CBinarySet)&binary_set);

    auto sealed_segment = SealedCreator(schema, dataset, *(LoadIndexInfo*)c_load_index_info);
    CSearchResult c_search_result_on_bigIndex;
    auto res_after_load_index =
        Search(sealed_segment.get(), plan, placeholderGroup, time, &c_search_result_on_bigIndex);
    assert(res_after_load_index.error_code == Success);

    std::vector<CSearchResult> results;
    results.push_back(c_search_result_on_bigIndex);
    status = ReduceSearchResultsAndFillData(plan, results.data(), results.size());
    assert(status.error_code == Success);

    auto search_result_on_bigIndex = (*(SearchResult*)c_search_result_on_bigIndex);
    for (int i = 0; i < num_queries; ++i) {
        auto offset = i * TOPK;
        ASSERT_EQ(search_result_on_bigIndex.internal_seg_offsets_[offset], 42000 + i);
        ASSERT_EQ(search_result_on_bigIndex.result_distances_[offset],
                  search_result_on_raw_index->result_distances_[offset]);
    }

    DeleteLoadIndexInfo(c_load_index_info);
    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchResult(c_search_result_on_smallIndex);
    DeleteSearchResult(c_search_result_on_bigIndex);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, SealedSegmentTest) {
    auto schema_tmp_conf = R"(name: "test"
                                autoID: true
                                fields: <
                                  fieldID: 100
                                  name: "vec"
                                  data_type: FloatVector
                                  type_params: <
                                    key: "dim"
                                    value: "16"
                                  >
                                  index_params: <
                                    key: "metric_type"
                                    value: "L2"
                                  >
                                >
                                fields: <
                                  fieldID: 101
                                  name: "age"
                                  data_type: Int32
                                  type_params: <
                                    key: "dim"
                                    value: "1"
                                  >
                                >)";
    auto collection = NewCollection(schema_tmp_conf);
    auto segment = NewSegment(collection, 0, Sealed);

    int N = 10000;
    std::default_random_engine e(67);
    auto ages = std::vector<int32_t>(N);
    for (auto& age : ages) {
        age = e() % 2000;
    }
    auto blob = (void*)(&ages[0]);

    auto load_info = CLoadFieldDataInfo{101, blob, N};

    auto res = LoadFieldData(segment, load_info);
    assert(res.error_code == Success);
    auto count = GetRowCount(segment);
    assert(count == N);

    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, SealedSegment_search_float_Predicate_Range) {
    constexpr auto TOPK = 5;

    std::string schema_string = generate_collection_schema("L2", DIM, false);
    auto collection = NewCollection(schema_string.c_str());
    auto schema = ((segcore::Collection*)collection)->get_schema();
    auto segment = NewSegment(collection, 0, Sealed);

    auto N = ROW_COUNT;
    auto dataset = DataGen(schema, N);
    auto vec_col = dataset.get_col<float>(0);
    auto counter_col = dataset.get_col<int64_t>(1);
    auto query_ptr = vec_col.data() + 42000 * DIM;

    const char* dsl_string = R"({
        "bool": {
            "must": [
            {
                "range": {
                    "counter": {
                        "GE": 42000,
                        "LT": 42010
                    }
                }
            },
            {
                "vector": {
                    "fakevec": {
                        "metric_type": "L2",
                        "params": {
                            "nprobe": 10
                        },
                        "query": "$0",
                        "topk": 5,
                        "round_decimal": -1
                    }
                }
            }
            ]
        }
    })";

    // create place_holder_group
    int num_queries = 10;
    auto raw_group = CreatePlaceholderGroupFromBlob(num_queries, DIM, query_ptr);
    auto blob = raw_group.SerializeAsString();

    // search on segment's small index
    void* plan = nullptr;
    auto status = CreateSearchPlan(collection, dsl_string, &plan);
    assert(status.error_code == Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(plan, blob.data(), blob.length(), &placeholderGroup);
    assert(status.error_code == Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    Timestamp time = 10000000;

    // load index to segment
    auto conf = milvus::knowhere::Config{{milvus::knowhere::meta::DIM, DIM},
                                         {milvus::knowhere::meta::TOPK, TOPK},
                                         {milvus::knowhere::IndexParams::nlist, 100},
                                         {milvus::knowhere::IndexParams::nprobe, 10},
                                         {milvus::knowhere::IndexParams::m, 4},
                                         {milvus::knowhere::IndexParams::nbits, 8},
                                         {milvus::knowhere::Metric::TYPE, milvus::knowhere::Metric::L2},
                                         {milvus::knowhere::meta::DEVICEID, 0}};

    auto indexing = generate_index(vec_col.data(), conf, DIM, TOPK, N, IndexEnum::INDEX_FAISS_IVFPQ);

    // gen query dataset
    auto query_dataset = milvus::knowhere::GenDataset(num_queries, DIM, query_ptr);
    auto result_on_index = indexing->Query(query_dataset, conf, nullptr);
    auto ids = result_on_index->Get<int64_t*>(milvus::knowhere::meta::IDS);
    auto dis = result_on_index->Get<float*>(milvus::knowhere::meta::DISTANCE);
    std::vector<int64_t> vec_ids(ids, ids + TOPK * num_queries);
    std::vector<float> vec_dis;
    for (int j = 0; j < TOPK * num_queries; ++j) {
        vec_dis.push_back(dis[j] * -1);
    }

    auto binary_set = indexing->Serialize(conf);
    void* c_load_index_info = nullptr;
    status = NewLoadIndexInfo(&c_load_index_info);
    assert(status.error_code == Success);
    std::string index_type_key = "index_type";
    std::string index_type_value = "IVF_PQ";
    std::string index_mode_key = "index_mode";
    std::string index_mode_value = "cpu";
    std::string metric_type_key = "metric_type";
    std::string metric_type_value = "L2";

    AppendIndexParam(c_load_index_info, index_type_key.c_str(), index_type_value.c_str());
    AppendIndexParam(c_load_index_info, index_mode_key.c_str(), index_mode_value.c_str());
    AppendIndexParam(c_load_index_info, metric_type_key.c_str(), metric_type_value.c_str());
    AppendFieldInfo(c_load_index_info, 100);
    AppendIndex(c_load_index_info, (CBinarySet)&binary_set);

    auto load_index_info = (LoadIndexInfo*)c_load_index_info;
    auto query_dataset2 = milvus::knowhere::GenDataset(num_queries, DIM, query_ptr);
    auto fuck2 = load_index_info->index;
    auto result_on_index2 = fuck2->Query(query_dataset2, conf, nullptr);
    auto ids2 = result_on_index2->Get<int64_t*>(milvus::knowhere::meta::IDS);
    auto dis2 = result_on_index2->Get<float*>(milvus::knowhere::meta::DISTANCE);
    int i = 1 + 1;
    ++i;

    auto c_counter_field_data = CLoadFieldDataInfo{
        101,
        counter_col.data(),
        N,
    };
    status = LoadFieldData(segment, c_counter_field_data);
    assert(status.error_code == Success);

    auto c_id_field_data = CLoadFieldDataInfo{
        0,
        counter_col.data(),
        N,
    };
    status = LoadFieldData(segment, c_id_field_data);
    assert(status.error_code == Success);

    auto c_ts_field_data = CLoadFieldDataInfo{
        1,
        counter_col.data(),
        N,
    };
    status = LoadFieldData(segment, c_ts_field_data);
    assert(status.error_code == Success);

    auto sealed_segment = SealedCreator(schema, dataset, *(LoadIndexInfo*)c_load_index_info);
    CSearchResult c_search_result_on_bigIndex;
    auto res_after_load_index =
        Search(sealed_segment.get(), plan, placeholderGroup, time, &c_search_result_on_bigIndex);
    assert(res_after_load_index.error_code == Success);

    auto search_result_on_bigIndex = (*(SearchResult*)c_search_result_on_bigIndex);
    for (int i = 0; i < num_queries; ++i) {
        auto offset = i * TOPK;
        ASSERT_EQ(search_result_on_bigIndex.internal_seg_offsets_[offset], 42000 + i);
    }

    DeleteLoadIndexInfo(c_load_index_info);
    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchResult(c_search_result_on_bigIndex);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, SealedSegment_search_float_With_Expr_Predicate_Range) {
    constexpr auto TOPK = 5;

    std::string schema_string = generate_collection_schema("L2", DIM, false);
    auto collection = NewCollection(schema_string.c_str());
    auto schema = ((segcore::Collection*)collection)->get_schema();
    auto segment = NewSegment(collection, 0, Sealed);

    auto N = ROW_COUNT;
    auto dataset = DataGen(schema, N);
    auto vec_col = dataset.get_col<float>(0);
    auto counter_col = dataset.get_col<int64_t>(1);
    auto query_ptr = vec_col.data() + 42000 * DIM;

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
                                                      int64_val: 42000
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
                                                      int64_val: 42010
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
    auto raw_group = CreatePlaceholderGroupFromBlob(num_queries, DIM, query_ptr);
    auto blob = raw_group.SerializeAsString();

    // search on segment's small index
    void* plan = nullptr;
    auto binary_plan = translate_text_plan_to_binary_plan(serialized_expr_plan);
    auto status = CreateSearchPlanByExpr(collection, binary_plan.data(), binary_plan.size(), &plan);
    assert(status.error_code == Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(plan, blob.data(), blob.length(), &placeholderGroup);
    assert(status.error_code == Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    Timestamp time = 10000000;

    // load index to segment
    auto conf = milvus::knowhere::Config{{milvus::knowhere::meta::DIM, DIM},
                                         {milvus::knowhere::meta::TOPK, TOPK},
                                         {milvus::knowhere::IndexParams::nlist, 100},
                                         {milvus::knowhere::IndexParams::nprobe, 10},
                                         {milvus::knowhere::IndexParams::m, 4},
                                         {milvus::knowhere::IndexParams::nbits, 8},
                                         {milvus::knowhere::Metric::TYPE, milvus::knowhere::Metric::L2},
                                         {milvus::knowhere::meta::DEVICEID, 0}};

    auto indexing = generate_index(vec_col.data(), conf, DIM, TOPK, N, IndexEnum::INDEX_FAISS_IVFPQ);

    // gen query dataset
    auto query_dataset = milvus::knowhere::GenDataset(num_queries, DIM, query_ptr);
    auto result_on_index = indexing->Query(query_dataset, conf, nullptr);
    auto ids = result_on_index->Get<int64_t*>(milvus::knowhere::meta::IDS);
    auto dis = result_on_index->Get<float*>(milvus::knowhere::meta::DISTANCE);
    std::vector<int64_t> vec_ids(ids, ids + TOPK * num_queries);
    std::vector<float> vec_dis;
    for (int j = 0; j < TOPK * num_queries; ++j) {
        vec_dis.push_back(dis[j] * -1);
    }

    auto binary_set = indexing->Serialize(conf);
    void* c_load_index_info = nullptr;
    status = NewLoadIndexInfo(&c_load_index_info);
    assert(status.error_code == Success);
    std::string index_type_key = "index_type";
    std::string index_type_value = "IVF_PQ";
    std::string index_mode_key = "index_mode";
    std::string index_mode_value = "cpu";
    std::string metric_type_key = "metric_type";
    std::string metric_type_value = "L2";

    AppendIndexParam(c_load_index_info, index_type_key.c_str(), index_type_value.c_str());
    AppendIndexParam(c_load_index_info, index_mode_key.c_str(), index_mode_value.c_str());
    AppendIndexParam(c_load_index_info, metric_type_key.c_str(), metric_type_value.c_str());
    AppendFieldInfo(c_load_index_info, 100);
    AppendIndex(c_load_index_info, (CBinarySet)&binary_set);

    auto load_index_info = (LoadIndexInfo*)c_load_index_info;
    auto query_dataset2 = milvus::knowhere::GenDataset(num_queries, DIM, query_ptr);
    auto fuck2 = load_index_info->index;
    auto result_on_index2 = fuck2->Query(query_dataset2, conf, nullptr);
    auto ids2 = result_on_index2->Get<int64_t*>(milvus::knowhere::meta::IDS);
    auto dis2 = result_on_index2->Get<float*>(milvus::knowhere::meta::DISTANCE);
    int i = 1 + 1;
    ++i;

    auto c_counter_field_data = CLoadFieldDataInfo{
        101,
        counter_col.data(),
        N,
    };
    status = LoadFieldData(segment, c_counter_field_data);
    assert(status.error_code == Success);

    auto c_id_field_data = CLoadFieldDataInfo{
        0,
        counter_col.data(),
        N,
    };
    status = LoadFieldData(segment, c_id_field_data);
    assert(status.error_code == Success);

    auto c_ts_field_data = CLoadFieldDataInfo{
        1,
        counter_col.data(),
        N,
    };
    status = LoadFieldData(segment, c_ts_field_data);
    assert(status.error_code == Success);

    status = UpdateSealedSegmentIndex(segment, c_load_index_info);
    assert(status.error_code == Success);

    CSearchResult c_search_result_on_bigIndex;
    auto res_after_load_index = Search(segment, plan, placeholderGroup, time, &c_search_result_on_bigIndex);
    assert(res_after_load_index.error_code == Success);

    auto search_result_on_bigIndex = (*(SearchResult*)c_search_result_on_bigIndex);
    for (int i = 0; i < num_queries; ++i) {
        auto offset = i * TOPK;
        ASSERT_EQ(search_result_on_bigIndex.internal_seg_offsets_[offset], 42000 + i);
    }

    DeleteLoadIndexInfo(c_load_index_info);
    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchResult(c_search_result_on_bigIndex);
    DeleteCollection(collection);
    DeleteSegment(segment);
}