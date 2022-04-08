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
#include <chrono>
#include <google/protobuf/text_format.h>
#include <iostream>
#include <random>
#include <string>
#include <unordered_set>
#include <knowhere/index/vector_index/helpers/IndexParameter.h>
#include <knowhere/index/vector_index/adapter/VectorAdapter.h>
#include <knowhere/index/vector_index/VecIndexFactory.h>
#include <knowhere/index/vector_index/IndexIVFPQ.h>

#include "common/LoadInfo.h"
#include "pb/milvus.pb.h"
#include "pb/plan.pb.h"
#include "query/ExprImpl.h"
#include "segcore/Collection.h"
#include "segcore/reduce_c.h"
#include "segcore/Reduce.h"
#include "test_utils/DataGen.h"
#include "utils/Types.h"

namespace chrono = std::chrono;

using namespace milvus;
using namespace milvus::segcore;
using namespace knowhere;

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

auto
generate_column_data(int N) {
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
    }
    for (int i = 0; i < N; ++i) {
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
generate_index(void* raw_data, knowhere::Config conf, int64_t dim, int64_t topK, int64_t N, std::string index_type) {
    auto indexing = knowhere::VecIndexFactory::GetInstance().CreateVecIndex(index_type, knowhere::IndexMode::MODE_CPU);

    auto database = knowhere::GenDataset(N, dim, raw_data);
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
    auto segment = NewSegment(collection, Growing, -1);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, InsertTest) {
    auto collection = NewCollection(get_default_schema_config());
    auto segment = NewSegment(collection, Growing, -1);

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

TEST(CApiTest, InsertColumnDataTest) {
    auto collection = NewCollection(get_default_schema_config());
    auto segment = NewSegment(collection, Growing, -1);

    int N = 10000;
    auto [raw_data, timestamps, uids] = generate_column_data(N);

    int64_t offset;
    PreInsert(segment, N, &offset);

    auto res = InsertColumnData(segment, offset, N, uids.data(), timestamps.data(), raw_data.data(), N);
    assert(res.error_code == Success);

    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, DeleteTest) {
    auto collection = NewCollection(get_default_schema_config());
    auto segment = NewSegment(collection, Growing, -1);

    int64_t delete_row_ids[] = {100000, 100001, 100002};
    uint64_t delete_timestamps[] = {0, 0, 0};

    auto offset = PreDelete(segment, 3);

    auto del_res = Delete(segment, offset, 3, delete_row_ids, delete_timestamps);
    assert(del_res.error_code == Success);

    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, SearchTest) {
    auto collection = NewCollection(get_default_schema_config());
    auto segment = NewSegment(collection, Growing, -1);

    int N = 10000;
    auto [raw_data, timestamps, uids] = generate_data(N);
    auto line_sizeof = (sizeof(int) + sizeof(float) * DIM);

    int64_t ts_offset = 1000;
    for (int i = 0; i < N; i++) {
        timestamps[i] = ts_offset + i;
    }

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
    auto res = Search(segment, plan, placeholderGroup, N + ts_offset, &search_result, -1);
    ASSERT_EQ(res.error_code, Success);

    CSearchResult search_result2;
    auto res2 = Search(segment, plan, placeholderGroup, ts_offset, &search_result2, -1);
    ASSERT_EQ(res2.error_code, Success);

    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchResult(search_result);
    DeleteSearchResult(search_result2);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, SearchTest2) {
    auto collection = NewCollection(get_default_schema_config());
    auto segment = NewSegment(collection, Growing, -1);

    int N = 10000;
    auto [raw_data, timestamps, uids] = generate_column_data(N);

    int64_t ts_offset = 1000;
    for (int i = 0; i < N; i++) {
        timestamps[i] = ts_offset + i;
    }

    int64_t offset;
    PreInsert(segment, N, &offset);

    auto ins_res = InsertColumnData(segment, offset, N, uids.data(), timestamps.data(), raw_data.data(), N);
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
    auto res = Search(segment, plan, placeholderGroup, N + ts_offset, &search_result, -1);
    ASSERT_EQ(res.error_code, Success);

    CSearchResult search_result2;
    auto res2 = Search(segment, plan, placeholderGroup, ts_offset, &search_result2, -1);
    ASSERT_EQ(res2.error_code, Success);

    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchResult(search_result);
    DeleteSearchResult(search_result2);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, SearchTestWithExpr) {
    auto collection = NewCollection(get_default_schema_config());
    auto segment = NewSegment(collection, Growing, -1);

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
    auto res = Search(segment, plan, placeholderGroup, timestamps[0], &search_result, -1);
    ASSERT_EQ(res.error_code, Success);

    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchResult(search_result);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, SearchTestWithExpr2) {
    auto collection = NewCollection(get_default_schema_config());
    auto segment = NewSegment(collection, Growing, -1);

    int N = 10000;
    auto [raw_data, timestamps, uids] = generate_column_data(N);

    int64_t offset;
    PreInsert(segment, N, &offset);

    auto ins_res = InsertColumnData(segment, offset, N, uids.data(), timestamps.data(), raw_data.data(), N);
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
    auto res = Search(segment, plan, placeholderGroup, timestamps[0], &search_result, -1);
    ASSERT_EQ(res.error_code, Success);

    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchResult(search_result);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, RetrieveTestWithExpr) {
    auto collection = NewCollection(get_default_schema_config());
    auto segment = NewSegment(collection, Growing, -1);

    int N = 10000;
    auto [raw_data, timestamps, uids] = generate_data(N);
    auto line_sizeof = (sizeof(int) + sizeof(float) * DIM);

    int64_t offset;
    PreInsert(segment, N, &offset);

    auto ins_res = Insert(segment, offset, N, uids.data(), timestamps.data(), raw_data.data(), (int)line_sizeof, N);
    ASSERT_EQ(ins_res.error_code, Success);

    auto schema = ((milvus::segcore::Collection*)collection)->get_schema();
    auto plan = std::make_unique<query::RetrievePlan>(*schema);

    // create retrieve plan "age in [0]"
    std::vector<int64_t> values(1, 0);
    auto term_expr = std::make_unique<query::TermExprImpl<int64_t>>(FieldOffset(1), DataType::INT32, values);

    plan->plan_node_ = std::make_unique<query::RetrievePlanNode>();
    plan->plan_node_->predicate_ = std::move(term_expr);
    std::vector<FieldOffset> target_offsets{FieldOffset(0), FieldOffset(1)};
    plan->field_offsets_ = target_offsets;

    CRetrieveResult retrieve_result;
    auto res = Retrieve(segment, plan.release(), timestamps[0], &retrieve_result);
    ASSERT_EQ(res.error_code, Success);

    DeleteRetrievePlan(plan.release());
    DeleteRetrieveResult(&retrieve_result);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, RetrieveTestWithExpr2) {
    auto collection = NewCollection(get_default_schema_config());
    auto segment = NewSegment(collection, Growing, -1);

    int N = 10000;
    auto [raw_data, timestamps, uids] = generate_column_data(N);

    int64_t offset;
    PreInsert(segment, N, &offset);

    auto ins_res = InsertColumnData(segment, offset, N, uids.data(), timestamps.data(), raw_data.data(), N);
    ASSERT_EQ(ins_res.error_code, Success);

    auto schema = ((milvus::segcore::Collection*)collection)->get_schema();
    auto plan = std::make_unique<query::RetrievePlan>(*schema);

    // create retrieve plan "age in [0]"
    std::vector<int64_t> values(1, 0);
    auto term_expr = std::make_unique<query::TermExprImpl<int64_t>>(FieldOffset(1), DataType::INT32, values);

    plan->plan_node_ = std::make_unique<query::RetrievePlanNode>();
    plan->plan_node_->predicate_ = std::move(term_expr);
    std::vector<FieldOffset> target_offsets{FieldOffset(0), FieldOffset(1)};
    plan->field_offsets_ = target_offsets;

    CRetrieveResult retrieve_result;
    auto res = Retrieve(segment, plan.release(), timestamps[0], &retrieve_result);
    ASSERT_EQ(res.error_code, Success);

    DeleteRetrievePlan(plan.release());
    DeleteRetrieveResult(&retrieve_result);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, GetMemoryUsageInBytesTest) {
    auto collection = NewCollection(get_default_schema_config());
    auto segment = NewSegment(collection, Growing, -1);

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

TEST(CApiTest, GetMemoryUsageInBytesTest2) {
    auto collection = NewCollection(get_default_schema_config());
    auto segment = NewSegment(collection, Growing, -1);

    auto old_memory_usage_size = GetMemoryUsageInBytes(segment);
    // std::cout << "old_memory_usage_size = " << old_memory_usage_size << std::endl;
    assert(old_memory_usage_size == 0);

    int N = 10000;
    auto [raw_data, timestamps, uids] = generate_column_data(N);

    int64_t offset;
    PreInsert(segment, N, &offset);

    auto res = InsertColumnData(segment, offset, N, uids.data(), timestamps.data(), raw_data.data(), N);
    assert(res.error_code == Success);

    auto memory_usage_size = GetMemoryUsageInBytes(segment);
    // std::cout << "new_memory_usage_size = " << memory_usage_size << std::endl;
    assert(memory_usage_size == 2785280);

    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, GetDeletedCountTest) {
    auto collection = NewCollection(get_default_schema_config());
    auto segment = NewSegment(collection, Growing, -1);

    int64_t delete_row_ids[] = {100000, 100001, 100002};
    uint64_t delete_timestamps[] = {0, 0, 0};

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
    auto segment = NewSegment(collection, Growing, -1);

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
//    auto segment = NewSegment(collection, Growing, -1);
//    DeleteCollection(collection);
//    DeleteSegment(segment);
//}

void
CheckSearchResultDuplicate(const std::vector<CSearchResult>& results) {
    auto sr = (SearchResult*)results[0];
    auto topk = sr->topk_;
    auto num_queries = sr->num_queries_;

    // fill primary keys
    std::vector<int64_t> result_pks(num_queries * topk);
    for (int i = 0; i < results.size(); i++) {
        auto search_result = (SearchResult*)results[i];
        auto size = search_result->result_offsets_.size();
        if (size == 0) {
            continue;
        }
        for (int j = 0; j < size; j++) {
            auto offset = search_result->result_offsets_[j];
            result_pks[offset] = search_result->primary_keys_[j];
        }
    }

    // check primary key duplicates
    int64_t cnt = 0;
    std::unordered_set<int64_t> pk_set;
    for (int qi = 0; qi < num_queries; qi++) {
        pk_set.clear();
        for (int k = 0; k < topk; k++) {
            int64_t idx = topk * qi + k;
            pk_set.insert(result_pks[idx]);
        }
        cnt += pk_set.size();
    }
    assert(cnt == topk * num_queries);
}

TEST(CApiTest, ReduceRemoveDuplicates) {
    auto collection = NewCollection(get_default_schema_config());
    auto segment = NewSegment(collection, Growing, -1);

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

    {
        std::vector<CSearchResult> results;
        CSearchResult res1, res2;
        status = Search(segment, plan, placeholderGroup, timestamps[0], &res1, -1);
        assert(status.error_code == Success);
        status = Search(segment, plan, placeholderGroup, timestamps[0], &res2, -1);
        assert(status.error_code == Success);
        results.push_back(res1);
        results.push_back(res2);

        status = ReduceSearchResultsAndFillData(plan, results.data(), results.size());
        assert(status.error_code == Success);
        CheckSearchResultDuplicate(results);

        DeleteSearchResult(res1);
        DeleteSearchResult(res2);
    }
    {
        std::vector<CSearchResult> results;
        CSearchResult res1, res2, res3;
        status = Search(segment, plan, placeholderGroup, timestamps[0], &res1, -1);
        assert(status.error_code == Success);
        status = Search(segment, plan, placeholderGroup, timestamps[0], &res2, -1);
        assert(status.error_code == Success);
        status = Search(segment, plan, placeholderGroup, timestamps[0], &res3, -1);
        assert(status.error_code == Success);
        results.push_back(res1);
        results.push_back(res2);
        results.push_back(res3);

        status = ReduceSearchResultsAndFillData(plan, results.data(), results.size());
        assert(status.error_code == Success);
        CheckSearchResultDuplicate(results);

        DeleteSearchResult(res1);
        DeleteSearchResult(res2);
        DeleteSearchResult(res3);
    }

    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, ReduceSearchWithExpr) {
    auto collection = NewCollection(get_default_schema_config());
    auto segment = NewSegment(collection, Growing, -1);

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
                                            placeholder_tag: "$0">
                                            output_field_ids: 100)";

    int topK = 10;
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
    auto res = Search(segment, plan, placeholderGroup, timestamps[0], &res1, -1);
    assert(res.error_code == Success);
    res = Search(segment, plan, placeholderGroup, timestamps[0], &res2, -1);
    assert(res.error_code == Success);
    results.push_back(res1);
    results.push_back(res2);

    // 1. reduce
    status = ReduceSearchResultsAndFillData(plan, results.data(), results.size());
    assert(status.error_code == Success);

    // 2. marshal
    CSearchResultDataBlobs cSearchResultData;
    auto req_sizes = std::vector<int32_t>{5, 5};
    status = Marshal(&cSearchResultData, results.data(), results.size(), req_sizes.data(), req_sizes.size());
    assert(status.error_code == Success);
    auto search_result_data_blobs = reinterpret_cast<milvus::segcore::SearchResultDataBlobs*>(cSearchResultData);

    // check result
    for (int i = 0; i < req_sizes.size(); i++) {
        milvus::proto::schema::SearchResultData search_result_data;
        auto suc = search_result_data.ParseFromArray(search_result_data_blobs->blobs[i].data(),
                                                     search_result_data_blobs->blobs[i].size());
        assert(suc);
        assert(search_result_data.top_k() == topK);
        assert(search_result_data.num_queries() == num_queries);
        assert(search_result_data.scores().size() == topK * req_sizes[i]);
        assert(search_result_data.ids().int_id().data_size() == topK * req_sizes[i]);
    }

    DeleteSearchResultDataBlobs(cSearchResultData);
    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchResult(res1);
    DeleteSearchResult(res2);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, LoadIndexInfo) {
    // generator index
    constexpr auto TOPK = 10;

    auto N = 1024 * 10;
    auto [raw_data, timestamps, uids] = generate_data(N);
    auto indexing = std::make_shared<knowhere::IVFPQ>();
    auto conf = knowhere::Config{{knowhere::meta::DIM, DIM},
                                 {knowhere::meta::TOPK, TOPK},
                                 {knowhere::IndexParams::nlist, 100},
                                 {knowhere::IndexParams::nprobe, 4},
                                 {knowhere::IndexParams::m, 4},
                                 {knowhere::IndexParams::nbits, 8},
                                 {knowhere::Metric::TYPE, knowhere::Metric::L2},
                                 {knowhere::meta::DEVICEID, 0}};

    auto database = knowhere::GenDataset(N, DIM, raw_data.data());
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
    auto indexing = std::make_shared<knowhere::IVFPQ>();
    auto conf = knowhere::Config{{knowhere::meta::DIM, DIM},
                                 {knowhere::meta::TOPK, TOPK},
                                 {knowhere::IndexParams::nlist, 100},
                                 {knowhere::IndexParams::nprobe, 4},
                                 {knowhere::IndexParams::m, 4},
                                 {knowhere::IndexParams::nbits, 8},
                                 {knowhere::Metric::TYPE, knowhere::Metric::L2},
                                 {knowhere::meta::DEVICEID, 0}};

    auto database = knowhere::GenDataset(N, DIM, raw_data.data());
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
    auto mode = knowhere::IndexMode::MODE_CPU;
    load_index_info.index = knowhere::VecIndexFactory::GetInstance().CreateVecIndex(index_params["index_type"], mode);
    load_index_info.index->Load(binary_set);

    // search
    auto query_dataset = knowhere::GenDataset(num_query, DIM, raw_data.data() + DIM * 4200);

    auto result = indexing->Query(query_dataset, conf, nullptr);

    auto ids = result->Get<int64_t*>(knowhere::meta::IDS);
    auto dis = result->Get<float*>(knowhere::meta::DISTANCE);
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
    auto segment = NewSegment(collection, Growing, -1);

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
    auto res_before_load_index = Search(segment, plan, placeholderGroup, time, &c_search_result_on_smallIndex, -1);
    assert(res_before_load_index.error_code == Success);

    // load index to segment
    auto conf = knowhere::Config{{knowhere::meta::DIM, DIM},
                                 {knowhere::meta::TOPK, TOPK},
                                 {knowhere::IndexParams::nlist, 100},
                                 {knowhere::IndexParams::nprobe, 10},
                                 {knowhere::IndexParams::m, 4},
                                 {knowhere::IndexParams::nbits, 8},
                                 {knowhere::Metric::TYPE, knowhere::Metric::L2},
                                 {knowhere::meta::DEVICEID, 0}};
    auto indexing = generate_index(vec_col.data(), conf, DIM, TOPK, N, IndexEnum::INDEX_FAISS_IVFPQ);

    // gen query dataset
    auto query_dataset = knowhere::GenDataset(num_queries, DIM, query_ptr);
    auto result_on_index = indexing->Query(query_dataset, conf, nullptr);
    auto ids = result_on_index->Get<int64_t*>(knowhere::meta::IDS);
    auto dis = result_on_index->Get<float*>(knowhere::meta::DISTANCE);
    std::vector<int64_t> vec_ids(ids, ids + TOPK * num_queries);
    std::vector<float> vec_dis;
    for (int j = 0; j < TOPK * num_queries; ++j) {
        vec_dis.push_back(dis[j] * -1);
    }

    auto search_result_on_raw_index = (SearchResult*)c_search_result_on_smallIndex;
    search_result_on_raw_index->ids_ = vec_ids;
    search_result_on_raw_index->distances_ = vec_dis;

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
        Search(sealed_segment.get(), plan, placeholderGroup, time, &c_search_result_on_bigIndex, -1);
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
    auto segment = NewSegment(collection, Growing, -1);

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
    auto res_before_load_index = Search(segment, plan, placeholderGroup, time, &c_search_result_on_smallIndex, -1);
    assert(res_before_load_index.error_code == Success);

    // load index to segment
    auto conf = knowhere::Config{{knowhere::meta::DIM, DIM},
                                 {knowhere::meta::TOPK, TOPK},
                                 {knowhere::IndexParams::nlist, 100},
                                 {knowhere::IndexParams::nprobe, 10},
                                 {knowhere::IndexParams::m, 4},
                                 {knowhere::IndexParams::nbits, 8},
                                 {knowhere::Metric::TYPE, knowhere::Metric::L2},
                                 {knowhere::meta::DEVICEID, 0}};
    auto indexing = generate_index(vec_col.data(), conf, DIM, TOPK, N, IndexEnum::INDEX_FAISS_IVFPQ);

    // gen query dataset
    auto query_dataset = knowhere::GenDataset(num_queries, DIM, query_ptr);
    auto result_on_index = indexing->Query(query_dataset, conf, nullptr);
    auto ids = result_on_index->Get<int64_t*>(knowhere::meta::IDS);
    auto dis = result_on_index->Get<float*>(knowhere::meta::DISTANCE);
    std::vector<int64_t> vec_ids(ids, ids + TOPK * num_queries);
    std::vector<float> vec_dis;
    for (int j = 0; j < TOPK * num_queries; ++j) {
        vec_dis.push_back(dis[j] * -1);
    }

    auto search_result_on_raw_index = (SearchResult*)c_search_result_on_smallIndex;
    search_result_on_raw_index->ids_ = vec_ids;
    search_result_on_raw_index->distances_ = vec_dis;

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
        Search(sealed_segment.get(), plan, placeholderGroup, time, &c_search_result_on_bigIndex, -1);
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
    auto segment = NewSegment(collection, Growing, -1);

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
    auto res_before_load_index = Search(segment, plan, placeholderGroup, time, &c_search_result_on_smallIndex, -1);
    assert(res_before_load_index.error_code == Success);

    // load index to segment
    auto conf = knowhere::Config{{knowhere::meta::DIM, DIM},
                                 {knowhere::meta::TOPK, TOPK},
                                 {knowhere::IndexParams::nlist, 100},
                                 {knowhere::IndexParams::nprobe, 10},
                                 {knowhere::IndexParams::m, 4},
                                 {knowhere::IndexParams::nbits, 8},
                                 {knowhere::Metric::TYPE, knowhere::Metric::L2},
                                 {knowhere::meta::DEVICEID, 0}};

    auto indexing = generate_index(vec_col.data(), conf, DIM, TOPK, N, IndexEnum::INDEX_FAISS_IVFPQ);

    // gen query dataset
    auto query_dataset = knowhere::GenDataset(num_queries, DIM, query_ptr);
    auto result_on_index = indexing->Query(query_dataset, conf, nullptr);
    auto ids = result_on_index->Get<int64_t*>(knowhere::meta::IDS);
    auto dis = result_on_index->Get<float*>(knowhere::meta::DISTANCE);
    std::vector<int64_t> vec_ids(ids, ids + TOPK * num_queries);
    std::vector<float> vec_dis;
    for (int j = 0; j < TOPK * num_queries; ++j) {
        vec_dis.push_back(dis[j] * -1);
    }

    auto search_result_on_raw_index = (SearchResult*)c_search_result_on_smallIndex;
    search_result_on_raw_index->ids_ = vec_ids;
    search_result_on_raw_index->distances_ = vec_dis;

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
        Search(sealed_segment.get(), plan, placeholderGroup, time, &c_search_result_on_bigIndex, -1);
    assert(res_after_load_index.error_code == Success);

    auto search_result_on_bigIndex = (*(SearchResult*)c_search_result_on_bigIndex);
    for (int i = 0; i < num_queries; ++i) {
        auto offset = i * TOPK;
        ASSERT_EQ(search_result_on_bigIndex.ids_[offset], 42000 + i);
        ASSERT_EQ(search_result_on_bigIndex.distances_[offset], search_result_on_raw_index->distances_[offset]);
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
    auto segment = NewSegment(collection, Growing, -1);

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
    auto res_before_load_index = Search(segment, plan, placeholderGroup, time, &c_search_result_on_smallIndex, -1);
    assert(res_before_load_index.error_code == Success);

    // load index to segment
    auto conf = knowhere::Config{{knowhere::meta::DIM, DIM},
                                 {knowhere::meta::TOPK, TOPK},
                                 {knowhere::IndexParams::nlist, 100},
                                 {knowhere::IndexParams::nprobe, 10},
                                 {knowhere::IndexParams::m, 4},
                                 {knowhere::IndexParams::nbits, 8},
                                 {knowhere::Metric::TYPE, knowhere::Metric::L2},
                                 {knowhere::meta::DEVICEID, 0}};

    auto indexing = generate_index(vec_col.data(), conf, DIM, TOPK, N, IndexEnum::INDEX_FAISS_IVFPQ);

    // gen query dataset
    auto query_dataset = knowhere::GenDataset(num_queries, DIM, query_ptr);
    auto result_on_index = indexing->Query(query_dataset, conf, nullptr);
    auto ids = result_on_index->Get<int64_t*>(knowhere::meta::IDS);
    auto dis = result_on_index->Get<float*>(knowhere::meta::DISTANCE);
    std::vector<int64_t> vec_ids(ids, ids + TOPK * num_queries);
    std::vector<float> vec_dis;
    for (int j = 0; j < TOPK * num_queries; ++j) {
        vec_dis.push_back(dis[j] * -1);
    }

    auto search_result_on_raw_index = (SearchResult*)c_search_result_on_smallIndex;
    search_result_on_raw_index->ids_ = vec_ids;
    search_result_on_raw_index->distances_ = vec_dis;

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
        Search(sealed_segment.get(), plan, placeholderGroup, time, &c_search_result_on_bigIndex, -1);
    assert(res_after_load_index.error_code == Success);

    auto search_result_on_bigIndex = (*(SearchResult*)c_search_result_on_bigIndex);
    for (int i = 0; i < num_queries; ++i) {
        auto offset = i * TOPK;
        ASSERT_EQ(search_result_on_bigIndex.ids_[offset], 420000 + i);
        ASSERT_EQ(search_result_on_bigIndex.distances_[offset], search_result_on_raw_index->distances_[offset]);
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
    auto segment = NewSegment(collection, Growing, -1);

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
    auto res_before_load_index = Search(segment, plan, placeholderGroup, time, &c_search_result_on_smallIndex, -1);
    assert(res_before_load_index.error_code == Success);

    // load index to segment
    auto conf = knowhere::Config{{knowhere::meta::DIM, DIM},
                                 {knowhere::meta::TOPK, TOPK},
                                 {knowhere::IndexParams::nlist, 100},
                                 {knowhere::IndexParams::nprobe, 10},
                                 {knowhere::IndexParams::m, 4},
                                 {knowhere::IndexParams::nbits, 8},
                                 {knowhere::Metric::TYPE, knowhere::Metric::L2},
                                 {knowhere::meta::DEVICEID, 0}};

    auto indexing = generate_index(vec_col.data(), conf, DIM, TOPK, N, IndexEnum::INDEX_FAISS_IVFPQ);

    // gen query dataset
    auto query_dataset = knowhere::GenDataset(num_queries, DIM, query_ptr);
    auto result_on_index = indexing->Query(query_dataset, conf, nullptr);
    auto ids = result_on_index->Get<int64_t*>(knowhere::meta::IDS);
    auto dis = result_on_index->Get<float*>(knowhere::meta::DISTANCE);
    std::vector<int64_t> vec_ids(ids, ids + TOPK * num_queries);
    std::vector<float> vec_dis;
    for (int j = 0; j < TOPK * num_queries; ++j) {
        vec_dis.push_back(dis[j] * -1);
    }

    auto search_result_on_raw_index = (SearchResult*)c_search_result_on_smallIndex;
    search_result_on_raw_index->ids_ = vec_ids;
    search_result_on_raw_index->distances_ = vec_dis;

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
        Search(sealed_segment.get(), plan, placeholderGroup, time, &c_search_result_on_bigIndex, -1);
    assert(res_after_load_index.error_code == Success);

    auto search_result_on_bigIndex = (*(SearchResult*)c_search_result_on_bigIndex);
    for (int i = 0; i < num_queries; ++i) {
        auto offset = i * TOPK;
        ASSERT_EQ(search_result_on_bigIndex.ids_[offset], 42000 + i);
        ASSERT_EQ(search_result_on_bigIndex.distances_[offset], search_result_on_raw_index->distances_[offset]);
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
    auto segment = NewSegment(collection, Growing, -1);

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
    auto res_before_load_index = Search(segment, plan, placeholderGroup, time, &c_search_result_on_smallIndex, -1);
    assert(res_before_load_index.error_code == Success);

    // load index to segment
    auto conf = knowhere::Config{{knowhere::meta::DIM, DIM},
                                 {knowhere::meta::TOPK, TOPK},
                                 {knowhere::IndexParams::nlist, 100},
                                 {knowhere::IndexParams::nprobe, 10},
                                 {knowhere::IndexParams::m, 4},
                                 {knowhere::IndexParams::nbits, 8},
                                 {knowhere::Metric::TYPE, knowhere::Metric::L2},
                                 {knowhere::meta::DEVICEID, 0}};

    auto indexing = generate_index(vec_col.data(), conf, DIM, TOPK, N, IndexEnum::INDEX_FAISS_IVFPQ);

    // gen query dataset
    auto query_dataset = knowhere::GenDataset(num_queries, DIM, query_ptr);
    auto result_on_index = indexing->Query(query_dataset, conf, nullptr);
    auto ids = result_on_index->Get<int64_t*>(knowhere::meta::IDS);
    auto dis = result_on_index->Get<float*>(knowhere::meta::DISTANCE);
    std::vector<int64_t> vec_ids(ids, ids + TOPK * num_queries);
    std::vector<float> vec_dis;
    for (int j = 0; j < TOPK * num_queries; ++j) {
        vec_dis.push_back(dis[j] * -1);
    }

    auto search_result_on_raw_index = (SearchResult*)c_search_result_on_smallIndex;
    search_result_on_raw_index->ids_ = vec_ids;
    search_result_on_raw_index->distances_ = vec_dis;

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
        Search(sealed_segment.get(), plan, placeholderGroup, time, &c_search_result_on_bigIndex, -1);
    assert(res_after_load_index.error_code == Success);

    auto search_result_on_bigIndex = (*(SearchResult*)c_search_result_on_bigIndex);
    for (int i = 0; i < num_queries; ++i) {
        auto offset = i * TOPK;
        ASSERT_EQ(search_result_on_bigIndex.ids_[offset], 420000 + i);
        ASSERT_EQ(search_result_on_bigIndex.distances_[offset], search_result_on_raw_index->distances_[offset]);
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
    auto segment = NewSegment(collection, Growing, -1);

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
    auto res_before_load_index = Search(segment, plan, placeholderGroup, time, &c_search_result_on_smallIndex, -1);
    assert(res_before_load_index.error_code == Success);

    // load index to segment
    auto conf = knowhere::Config{
        {knowhere::meta::DIM, DIM},
        {knowhere::meta::TOPK, TOPK},
        {knowhere::IndexParams::nprobe, 10},
        {knowhere::IndexParams::nlist, 100},
        {knowhere::IndexParams::m, 4},
        {knowhere::IndexParams::nbits, 8},
        {knowhere::Metric::TYPE, knowhere::Metric::JACCARD},
    };

    auto indexing = generate_index(vec_col.data(), conf, DIM, TOPK, N, IndexEnum::INDEX_FAISS_BIN_IVFFLAT);

    // gen query dataset
    auto query_dataset = knowhere::GenDataset(num_queries, DIM, query_ptr);
    auto result_on_index = indexing->Query(query_dataset, conf, nullptr);
    auto ids = result_on_index->Get<int64_t*>(knowhere::meta::IDS);
    auto dis = result_on_index->Get<float*>(knowhere::meta::DISTANCE);
    std::vector<int64_t> vec_ids(ids, ids + TOPK * num_queries);
    std::vector<float> vec_dis;
    for (int j = 0; j < TOPK * num_queries; ++j) {
        vec_dis.push_back(dis[j] * -1);
    }

    auto search_result_on_raw_index = (SearchResult*)c_search_result_on_smallIndex;
    search_result_on_raw_index->ids_ = vec_ids;
    search_result_on_raw_index->distances_ = vec_dis;

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
        Search(sealed_segment.get(), plan, placeholderGroup, time, &c_search_result_on_bigIndex, -1);
    assert(res_after_load_index.error_code == Success);

    auto search_result_on_bigIndex = (*(SearchResult*)c_search_result_on_bigIndex);
    for (int i = 0; i < num_queries; ++i) {
        auto offset = i * TOPK;
        ASSERT_EQ(search_result_on_bigIndex.ids_[offset], 420000 + i);
        ASSERT_EQ(search_result_on_bigIndex.distances_[offset], search_result_on_raw_index->distances_[offset]);
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
    auto segment = NewSegment(collection, Growing, -1);

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
    auto res_before_load_index = Search(segment, plan, placeholderGroup, time, &c_search_result_on_smallIndex, -1);
    ASSERT_TRUE(res_before_load_index.error_code == Success) << res_before_load_index.error_msg;

    // load index to segment
    auto conf = knowhere::Config{
        {knowhere::meta::DIM, DIM},
        {knowhere::meta::TOPK, TOPK},
        {knowhere::IndexParams::nprobe, 10},
        {knowhere::IndexParams::nlist, 100},
        {knowhere::IndexParams::m, 4},
        {knowhere::IndexParams::nbits, 8},
        {knowhere::Metric::TYPE, knowhere::Metric::JACCARD},
    };

    auto indexing = generate_index(vec_col.data(), conf, DIM, TOPK, N, IndexEnum::INDEX_FAISS_BIN_IVFFLAT);

    // gen query dataset
    auto query_dataset = knowhere::GenDataset(num_queries, DIM, query_ptr);
    auto result_on_index = indexing->Query(query_dataset, conf, nullptr);
    auto ids = result_on_index->Get<int64_t*>(knowhere::meta::IDS);
    auto dis = result_on_index->Get<float*>(knowhere::meta::DISTANCE);
    std::vector<int64_t> vec_ids(ids, ids + TOPK * num_queries);
    std::vector<float> vec_dis;
    for (int j = 0; j < TOPK * num_queries; ++j) {
        vec_dis.push_back(dis[j] * -1);
    }

    auto search_result_on_raw_index = (SearchResult*)c_search_result_on_smallIndex;
    search_result_on_raw_index->ids_ = vec_ids;
    search_result_on_raw_index->distances_ = vec_dis;

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
        Search(sealed_segment.get(), plan, placeholderGroup, time, &c_search_result_on_bigIndex, -1);
    assert(res_after_load_index.error_code == Success);

    auto search_result_on_bigIndex = (*(SearchResult*)c_search_result_on_bigIndex);
    for (int i = 0; i < num_queries; ++i) {
        auto offset = i * TOPK;
        ASSERT_EQ(search_result_on_bigIndex.ids_[offset], 42000 + i);
        ASSERT_EQ(search_result_on_bigIndex.distances_[offset], search_result_on_raw_index->distances_[offset]);
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
    auto segment = NewSegment(collection, Growing, -1);

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
    auto res_before_load_index = Search(segment, plan, placeholderGroup, time, &c_search_result_on_smallIndex, -1);
    assert(res_before_load_index.error_code == Success);

    // load index to segment
    auto conf = knowhere::Config{
        {knowhere::meta::DIM, DIM},
        {knowhere::meta::TOPK, TOPK},
        {knowhere::IndexParams::nprobe, 10},
        {knowhere::IndexParams::nlist, 100},
        {knowhere::IndexParams::m, 4},
        {knowhere::IndexParams::nbits, 8},
        {knowhere::Metric::TYPE, knowhere::Metric::JACCARD},
    };

    auto indexing = generate_index(vec_col.data(), conf, DIM, TOPK, N, IndexEnum::INDEX_FAISS_BIN_IVFFLAT);

    // gen query dataset
    auto query_dataset = knowhere::GenDataset(num_queries, DIM, query_ptr);
    auto result_on_index = indexing->Query(query_dataset, conf, nullptr);
    auto ids = result_on_index->Get<int64_t*>(knowhere::meta::IDS);
    auto dis = result_on_index->Get<float*>(knowhere::meta::DISTANCE);
    std::vector<int64_t> vec_ids(ids, ids + TOPK * num_queries);
    std::vector<float> vec_dis;
    for (int j = 0; j < TOPK * num_queries; ++j) {
        vec_dis.push_back(dis[j] * -1);
    }

    auto search_result_on_raw_index = (SearchResult*)c_search_result_on_smallIndex;
    search_result_on_raw_index->ids_ = vec_ids;
    search_result_on_raw_index->distances_ = vec_dis;

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
        Search(sealed_segment.get(), plan, placeholderGroup, time, &c_search_result_on_bigIndex, -1);
    assert(res_after_load_index.error_code == Success);

    std::vector<CSearchResult> results;
    results.push_back(c_search_result_on_bigIndex);
    status = ReduceSearchResultsAndFillData(plan, results.data(), results.size());
    assert(status.error_code == Success);

    auto search_result_on_bigIndex = (*(SearchResult*)c_search_result_on_bigIndex);
    for (int i = 0; i < num_queries; ++i) {
        auto offset = i * TOPK;
        ASSERT_EQ(search_result_on_bigIndex.ids_[offset], 42000 + i);
        ASSERT_EQ(search_result_on_bigIndex.distances_[offset], search_result_on_raw_index->distances_[offset]);
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
    auto segment = NewSegment(collection, Growing, -1);

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
    auto res_before_load_index = Search(segment, plan, placeholderGroup, time, &c_search_result_on_smallIndex, -1);
    assert(res_before_load_index.error_code == Success);

    // load index to segment
    auto conf = knowhere::Config{
        {knowhere::meta::DIM, DIM},
        {knowhere::meta::TOPK, TOPK},
        {knowhere::IndexParams::nprobe, 10},
        {knowhere::IndexParams::nlist, 100},
        {knowhere::IndexParams::m, 4},
        {knowhere::IndexParams::nbits, 8},
        {knowhere::Metric::TYPE, knowhere::Metric::JACCARD},
    };

    auto indexing = generate_index(vec_col.data(), conf, DIM, TOPK, N, IndexEnum::INDEX_FAISS_BIN_IVFFLAT);

    // gen query dataset
    auto query_dataset = knowhere::GenDataset(num_queries, DIM, query_ptr);
    auto result_on_index = indexing->Query(query_dataset, conf, nullptr);
    auto ids = result_on_index->Get<int64_t*>(knowhere::meta::IDS);
    auto dis = result_on_index->Get<float*>(knowhere::meta::DISTANCE);
    std::vector<int64_t> vec_ids(ids, ids + TOPK * num_queries);
    std::vector<float> vec_dis;
    for (int j = 0; j < TOPK * num_queries; ++j) {
        vec_dis.push_back(dis[j] * -1);
    }

    auto search_result_on_raw_index = (SearchResult*)c_search_result_on_smallIndex;
    search_result_on_raw_index->ids_ = vec_ids;
    search_result_on_raw_index->distances_ = vec_dis;

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
        Search(sealed_segment.get(), plan, placeholderGroup, time, &c_search_result_on_bigIndex, -1);
    assert(res_after_load_index.error_code == Success);

    std::vector<CSearchResult> results;
    results.push_back(c_search_result_on_bigIndex);
    status = ReduceSearchResultsAndFillData(plan, results.data(), results.size());
    assert(status.error_code == Success);

    auto search_result_on_bigIndex = (*(SearchResult*)c_search_result_on_bigIndex);
    for (int i = 0; i < num_queries; ++i) {
        auto offset = i * TOPK;
        ASSERT_EQ(search_result_on_bigIndex.ids_[offset], 42000 + i);
        ASSERT_EQ(search_result_on_bigIndex.distances_[offset], search_result_on_raw_index->distances_[offset]);
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
    auto segment = NewSegment(collection, Sealed, -1);

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
    auto segment = NewSegment(collection, Sealed, -1);

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
    auto conf = knowhere::Config{{knowhere::meta::DIM, DIM},
                                 {knowhere::meta::TOPK, TOPK},
                                 {knowhere::IndexParams::nlist, 100},
                                 {knowhere::IndexParams::nprobe, 10},
                                 {knowhere::IndexParams::m, 4},
                                 {knowhere::IndexParams::nbits, 8},
                                 {knowhere::Metric::TYPE, knowhere::Metric::L2},
                                 {knowhere::meta::DEVICEID, 0}};

    auto indexing = generate_index(vec_col.data(), conf, DIM, TOPK, N, IndexEnum::INDEX_FAISS_IVFPQ);

    // gen query dataset
    auto query_dataset = knowhere::GenDataset(num_queries, DIM, query_ptr);
    auto result_on_index = indexing->Query(query_dataset, conf, nullptr);
    auto ids = result_on_index->Get<int64_t*>(knowhere::meta::IDS);
    auto dis = result_on_index->Get<float*>(knowhere::meta::DISTANCE);
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
    auto query_dataset2 = knowhere::GenDataset(num_queries, DIM, query_ptr);
    auto fuck2 = load_index_info->index;
    auto result_on_index2 = fuck2->Query(query_dataset2, conf, nullptr);
    auto ids2 = result_on_index2->Get<int64_t*>(knowhere::meta::IDS);
    auto dis2 = result_on_index2->Get<float*>(knowhere::meta::DISTANCE);
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
        Search(sealed_segment.get(), plan, placeholderGroup, time, &c_search_result_on_bigIndex, -1);
    assert(res_after_load_index.error_code == Success);

    auto search_result_on_bigIndex = (*(SearchResult*)c_search_result_on_bigIndex);
    for (int i = 0; i < num_queries; ++i) {
        auto offset = i * TOPK;
        ASSERT_EQ(search_result_on_bigIndex.ids_[offset], 42000 + i);
    }

    DeleteLoadIndexInfo(c_load_index_info);
    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchResult(c_search_result_on_bigIndex);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, SealedSegment_search_without_predicates) {
    constexpr auto TOPK = 5;
    std::string schema_string = generate_collection_schema("L2", DIM, false);
    auto collection = NewCollection(schema_string.c_str());
    auto schema = ((segcore::Collection*)collection)->get_schema();
    auto segment = NewSegment(collection, Sealed, -1);

    auto N = ROW_COUNT;
    uint64_t ts_offset = 1000;
    auto dataset = DataGen(schema, N, ts_offset);
    auto vec_col = dataset.get_col<float>(0);
    auto counter_col = dataset.get_col<int64_t>(1);
    auto query_ptr = vec_col.data() + 42000 * DIM;

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

    auto c_vec_field_data = CLoadFieldDataInfo{
        100,
        vec_col.data(),
        N,
    };
    auto status = LoadFieldData(segment, c_vec_field_data);
    assert(status.error_code == Success);

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

    int num_queries = 10;
    auto blob = generate_query_data(num_queries);

    void* plan = nullptr;
    status = CreateSearchPlan(collection, dsl_string, &plan);
    ASSERT_EQ(status.error_code, Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(plan, blob.data(), blob.length(), &placeholderGroup);
    ASSERT_EQ(status.error_code, Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    CSearchResult search_result;
    auto res = Search(segment, plan, placeholderGroup, N + ts_offset, &search_result, -1);
    std::cout << res.error_msg << std::endl;
    ASSERT_EQ(res.error_code, Success);

    CSearchResult search_result2;
    auto res2 = Search(segment, plan, placeholderGroup, ts_offset, &search_result2, -1);
    ASSERT_EQ(res2.error_code, Success);

    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchResult(search_result);
    DeleteSearchResult(search_result2);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, SealedSegment_search_float_With_Expr_Predicate_Range) {
    constexpr auto TOPK = 5;

    std::string schema_string = generate_collection_schema("L2", DIM, false);
    auto collection = NewCollection(schema_string.c_str());
    auto schema = ((segcore::Collection*)collection)->get_schema();
    auto segment = NewSegment(collection, Sealed, -1);

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
    auto conf = knowhere::Config{{knowhere::meta::DIM, DIM},
                                 {knowhere::meta::TOPK, TOPK},
                                 {knowhere::IndexParams::nlist, 100},
                                 {knowhere::IndexParams::nprobe, 10},
                                 {knowhere::IndexParams::m, 4},
                                 {knowhere::IndexParams::nbits, 8},
                                 {knowhere::Metric::TYPE, knowhere::Metric::L2},
                                 {knowhere::meta::DEVICEID, 0}};

    auto indexing = generate_index(vec_col.data(), conf, DIM, TOPK, N, IndexEnum::INDEX_FAISS_IVFPQ);

    // gen query dataset
    auto query_dataset = knowhere::GenDataset(num_queries, DIM, query_ptr);
    auto result_on_index = indexing->Query(query_dataset, conf, nullptr);
    auto ids = result_on_index->Get<int64_t*>(knowhere::meta::IDS);
    auto dis = result_on_index->Get<float*>(knowhere::meta::DISTANCE);
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
    auto query_dataset2 = knowhere::GenDataset(num_queries, DIM, query_ptr);
    auto fuck2 = load_index_info->index;
    auto result_on_index2 = fuck2->Query(query_dataset2, conf, nullptr);
    auto ids2 = result_on_index2->Get<int64_t*>(knowhere::meta::IDS);
    auto dis2 = result_on_index2->Get<float*>(knowhere::meta::DISTANCE);
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
    auto res_after_load_index = Search(segment, plan, placeholderGroup, time, &c_search_result_on_bigIndex, -1);
    assert(res_after_load_index.error_code == Success);

    auto search_result_on_bigIndex = (*(SearchResult*)c_search_result_on_bigIndex);
    for (int i = 0; i < num_queries; ++i) {
        auto offset = i * TOPK;
        ASSERT_EQ(search_result_on_bigIndex.ids_[offset], 42000 + i);
    }

    DeleteLoadIndexInfo(c_load_index_info);
    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchResult(c_search_result_on_bigIndex);
    DeleteCollection(collection);
    DeleteSegment(segment);
}
