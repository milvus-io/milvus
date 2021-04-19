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

#include "pb/service_msg.pb.h"
#include "segcore/reduce_c.h"

#include <index/knowhere/knowhere/index/vector_index/helpers/IndexParameter.h>
#include <index/knowhere/knowhere/index/vector_index/adapter/VectorAdapter.h>
#include <index/knowhere/knowhere/index/vector_index/VecIndexFactory.h>
#include <index/knowhere/knowhere/index/vector_index/IndexIVFPQ.h>
#include <common/LoadInfo.h>
#include <utils/Types.h>
#include <segcore/Collection.h>
#include "test_utils/DataGen.h"

namespace chrono = std::chrono;

using namespace milvus;
using namespace milvus::segcore;
using namespace milvus::proto;
using namespace milvus::knowhere;

TEST(CApiTest, CollectionTest) {
    auto schema_tmp_conf = "";
    auto collection = NewCollection(schema_tmp_conf);
    DeleteCollection(collection);
}

TEST(CApiTest, GetCollectionNameTest) {
    auto schema_tmp_conf = "";
    auto collection = NewCollection(schema_tmp_conf);
    auto name = GetCollectionName(collection);
    assert(strcmp(name, "default-collection") == 0);
    DeleteCollection(collection);
}

TEST(CApiTest, SegmentTest) {
    auto schema_tmp_conf = "";
    auto collection = NewCollection(schema_tmp_conf);
    auto segment = NewSegment(collection, 0);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, InsertTest) {
    auto schema_tmp_conf = "";
    auto collection = NewCollection(schema_tmp_conf);
    auto segment = NewSegment(collection, 0);

    std::vector<char> raw_data;
    std::vector<uint64_t> timestamps;
    std::vector<int64_t> uids;
    int N = 10000;
    std::default_random_engine e(67);
    for (int i = 0; i < N; ++i) {
        uids.push_back(100000 + i);
        timestamps.push_back(0);
        // append vec
        float vec[16];
        for (auto& x : vec) {
            x = e() % 2000 * 0.001 - 1.0;
        }
        raw_data.insert(raw_data.end(), (const char*)std::begin(vec), (const char*)std::end(vec));
        int age = e() % 100;
        raw_data.insert(raw_data.end(), (const char*)&age, ((const char*)&age) + sizeof(age));
    }

    auto line_sizeof = (sizeof(int) + sizeof(float) * 16);

    auto offset = PreInsert(segment, N);

    auto res = Insert(segment, offset, N, uids.data(), timestamps.data(), raw_data.data(), (int)line_sizeof, N);

    assert(res.error_code == Success);

    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, DeleteTest) {
    auto schema_tmp_conf = "";
    auto collection = NewCollection(schema_tmp_conf);
    auto segment = NewSegment(collection, 0);

    long delete_row_ids[] = {100000, 100001, 100002};
    unsigned long delete_timestamps[] = {0, 0, 0};

    auto offset = PreDelete(segment, 3);

    auto del_res = Delete(segment, offset, 3, delete_row_ids, delete_timestamps);
    assert(del_res.error_code == Success);

    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, SearchTest) {
    auto schema_tmp_conf = "";
    auto collection = NewCollection(schema_tmp_conf);
    auto segment = NewSegment(collection, 0);

    std::vector<char> raw_data;
    std::vector<uint64_t> timestamps;
    std::vector<int64_t> uids;
    int N = 10000;
    std::default_random_engine e(67);
    for (int i = 0; i < N; ++i) {
        uids.push_back(100000 + i);
        timestamps.push_back(0);
        // append vec
        float vec[16];
        for (auto& x : vec) {
            x = e() % 2000 * 0.001 - 1.0;
        }
        raw_data.insert(raw_data.end(), (const char*)std::begin(vec), (const char*)std::end(vec));
        int age = e() % 100;
        raw_data.insert(raw_data.end(), (const char*)&age, ((const char*)&age) + sizeof(age));
    }

    auto line_sizeof = (sizeof(int) + sizeof(float) * 16);

    auto offset = PreInsert(segment, N);

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
                    "topk": 10
                }
            }
        }
    })";

    namespace ser = milvus::proto::service;
    int num_queries = 10;
    int dim = 16;
    std::normal_distribution<double> dis(0, 1);
    ser::PlaceholderGroup raw_group;
    auto value = raw_group.add_placeholders();
    value->set_tag("$0");
    value->set_type(ser::PlaceholderType::VECTOR_FLOAT);
    for (int i = 0; i < num_queries; ++i) {
        std::vector<float> vec;
        for (int d = 0; d < dim; ++d) {
            vec.push_back(dis(e));
        }
        // std::string line((char*)vec.data(), (char*)vec.data() + vec.size() * sizeof(float));
        value->add_values(vec.data(), vec.size() * sizeof(float));
    }
    auto blob = raw_group.SerializeAsString();

    void* plan = nullptr;

    auto status = CreatePlan(collection, dsl_string, &plan);
    ASSERT_EQ(status.error_code, Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(plan, blob.data(), blob.length(), &placeholderGroup);
    ASSERT_EQ(status.error_code, Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    timestamps.clear();
    timestamps.push_back(1);

    CQueryResult search_result;
    auto res = Search(segment, plan, placeholderGroups.data(), timestamps.data(), 1, &search_result);
    ASSERT_EQ(res.error_code, Success);

    DeletePlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteQueryResult(search_result);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

// TEST(CApiTest, BuildIndexTest) {
//    auto schema_tmp_conf = "";
//    auto collection = NewCollection(schema_tmp_conf);
//    auto segment = NewSegment(collection, 0);
//
//    std::vector<char> raw_data;
//    std::vector<uint64_t> timestamps;
//    std::vector<int64_t> uids;
//    int N = 10000;
//    std::default_random_engine e(67);
//    for (int i = 0; i < N; ++i) {
//        uids.push_back(100000 + i);
//        timestamps.push_back(0);
//        // append vec
//        float vec[16];
//        for (auto& x : vec) {
//            x = e() % 2000 * 0.001 - 1.0;
//        }
//        raw_data.insert(raw_data.end(), (const char*)std::begin(vec), (const char*)std::end(vec));
//        int age = e() % 100;
//        raw_data.insert(raw_data.end(), (const char*)&age, ((const char*)&age) + sizeof(age));
//    }
//
//    auto line_sizeof = (sizeof(int) + sizeof(float) * 16);
//
//    auto offset = PreInsert(segment, N);
//
//    auto ins_res = Insert(segment, offset, N, uids.data(), timestamps.data(), raw_data.data(), (int)line_sizeof, N);
//    assert(ins_res == 0);
//
//    // TODO: add index ptr
//    Close(segment);
//    BuildIndex(collection, segment);
//
//    const char* dsl_string = R"(
//    {
//        "bool": {
//            "vector": {
//                "fakevec": {
//                    "metric_type": "L2",
//                    "params": {
//                        "nprobe": 10
//                    },
//                    "query": "$0",
//                    "topk": 10
//                }
//            }
//        }
//    })";
//
//    namespace ser = milvus::proto::service;
//    int num_queries = 10;
//    int dim = 16;
//    std::normal_distribution<double> dis(0, 1);
//    ser::PlaceholderGroup raw_group;
//    auto value = raw_group.add_placeholders();
//    value->set_tag("$0");
//    value->set_type(ser::PlaceholderType::VECTOR_FLOAT);
//    for (int i = 0; i < num_queries; ++i) {
//      std::vector<float> vec;
//      for (int d = 0; d < dim; ++d) {
//        vec.push_back(dis(e));
//      }
//      // std::string line((char*)vec.data(), (char*)vec.data() + vec.size() * sizeof(float));
//      value->add_values(vec.data(), vec.size() * sizeof(float));
//    }
//    auto blob = raw_group.SerializeAsString();
//
//    auto plan = CreatePlan(collection, dsl_string);
//    auto placeholderGroup = ParsePlaceholderGroup(plan, blob.data(), blob.length());
//    std::vector<CPlaceholderGroup> placeholderGroups;
//    placeholderGroups.push_back(placeholderGroup);
//    timestamps.clear();
//    timestamps.push_back(1);
//
//    auto search_res = Search(segment, plan, placeholderGroups.data(), timestamps.data(), 1);
//
//    DeletePlan(plan);
//    DeletePlaceholderGroup(placeholderGroup);
//    DeleteQueryResult(search_res);
//    DeleteCollection(collection);
//    DeleteSegment(segment);
//}

TEST(CApiTest, IsOpenedTest) {
    auto schema_tmp_conf = "";
    auto collection = NewCollection(schema_tmp_conf);
    auto segment = NewSegment(collection, 0);

    auto is_opened = IsOpened(segment);
    assert(is_opened);

    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, CloseTest) {
    auto schema_tmp_conf = "";
    auto collection = NewCollection(schema_tmp_conf);
    auto segment = NewSegment(collection, 0);

    auto status = Close(segment);
    assert(status == 0);

    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, GetMemoryUsageInBytesTest) {
    auto schema_tmp_conf = "";
    auto collection = NewCollection(schema_tmp_conf);
    auto segment = NewSegment(collection, 0);

    auto old_memory_usage_size = GetMemoryUsageInBytes(segment);
    std::cout << "old_memory_usage_size = " << old_memory_usage_size << std::endl;

    std::vector<char> raw_data;
    std::vector<uint64_t> timestamps;
    std::vector<int64_t> uids;
    int N = 10000;
    std::default_random_engine e(67);
    for (int i = 0; i < N; ++i) {
        uids.push_back(100000 + i);
        timestamps.push_back(0);
        // append vec
        float vec[16];
        for (auto& x : vec) {
            x = e() % 2000 * 0.001 - 1.0;
        }
        raw_data.insert(raw_data.end(), (const char*)std::begin(vec), (const char*)std::end(vec));
        int age = e() % 100;
        raw_data.insert(raw_data.end(), (const char*)&age, ((const char*)&age) + sizeof(age));
    }

    auto line_sizeof = (sizeof(int) + sizeof(float) * 16);

    auto offset = PreInsert(segment, N);

    auto res = Insert(segment, offset, N, uids.data(), timestamps.data(), raw_data.data(), (int)line_sizeof, N);

    assert(res.error_code == Success);

    auto memory_usage_size = GetMemoryUsageInBytes(segment);

    std::cout << "new_memory_usage_size = " << memory_usage_size << std::endl;

    assert(memory_usage_size == 2785280);

    DeleteCollection(collection);
    DeleteSegment(segment);
}

namespace {
auto
generate_data(int N) {
    std::vector<char> raw_data;
    std::vector<uint64_t> timestamps;
    std::vector<int64_t> uids;
    std::default_random_engine er(42);
    std::normal_distribution<> distribution(0.0, 1.0);
    std::default_random_engine ei(42);
    for (int i = 0; i < N; ++i) {
        uids.push_back(10 * N + i);
        timestamps.push_back(0);
        // append vec
        float vec[16];
        for (auto& x : vec) {
            x = distribution(er);
        }
        raw_data.insert(raw_data.end(), (const char*)std::begin(vec), (const char*)std::end(vec));
        int age = ei() % 100;
        raw_data.insert(raw_data.end(), (const char*)&age, ((const char*)&age) + sizeof(age));
    }
    return std::make_tuple(raw_data, timestamps, uids);
}

std::string
generate_collection_shema(std::string metric_type, std::string dim, bool is_binary) {
    schema::CollectionSchema collection_schema;
    collection_schema.set_name("collection_test");
    collection_schema.set_autoid(true);

    auto vec_field_schema = collection_schema.add_fields();
    vec_field_schema->set_name("fakevec");
    vec_field_schema->set_fieldid(100);
    if (is_binary) {
        vec_field_schema->set_data_type(schema::DataType::VECTOR_BINARY);
    } else {
        vec_field_schema->set_data_type(schema::DataType::VECTOR_FLOAT);
    }
    auto metric_type_param = vec_field_schema->add_index_params();
    metric_type_param->set_key("metric_type");
    metric_type_param->set_value(metric_type);
    auto dim_param = vec_field_schema->add_type_params();
    dim_param->set_key("dim");
    dim_param->set_value(dim);

    auto other_field_schema = collection_schema.add_fields();
    ;
    other_field_schema->set_name("counter");
    other_field_schema->set_fieldid(101);
    other_field_schema->set_data_type(schema::DataType::INT64);

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

// TEST(CApiTest, TestSearchPreference) {
//    auto schema_tmp_conf = "";
//    auto collection = NewCollection(schema_tmp_conf);
//    auto segment = NewSegment(collection, 0);
//
//    auto beg = chrono::high_resolution_clock::now();
//    auto next = beg;
//    int N = 1000 * 1000 * 10;
//    auto [raw_data, timestamps, uids] = generate_data(N);
//    auto line_sizeof = (sizeof(int) + sizeof(float) * 16);
//
//    next = chrono::high_resolution_clock::now();
//    std::cout << "generate_data: " << chrono::duration_cast<chrono::milliseconds>(next - beg).count() << "ms"
//              << std::endl;
//    beg = next;
//
//    auto offset = PreInsert(segment, N);
//    auto res = Insert(segment, offset, N, uids.data(), timestamps.data(), raw_data.data(), (int)line_sizeof, N);
//    assert(res == 0);
//    next = chrono::high_resolution_clock::now();
//    std::cout << "insert: " << chrono::duration_cast<chrono::milliseconds>(next - beg).count() << "ms" << std::endl;
//    beg = next;
//
//    auto N_del = N / 100;
//    std::vector<uint64_t> del_ts(N_del, 100);
//    auto pre_off = PreDelete(segment, N_del);
//    Delete(segment, pre_off, N_del, uids.data(), del_ts.data());
//
//    next = chrono::high_resolution_clock::now();
//    std::cout << "delete1: " << chrono::duration_cast<chrono::milliseconds>(next - beg).count() << "ms" << std::endl;
//    beg = next;
//
//    auto row_count = GetRowCount(segment);
//    assert(row_count == N);
//
//    std::vector<long> result_ids(10 * 16);
//    std::vector<float> result_distances(10 * 16);
//
//    CQueryInfo queryInfo{1, 10, "fakevec"};
//    auto sea_res =
//        Search(segment, queryInfo, 104, (float*)raw_data.data(), 16, result_ids.data(), result_distances.data());
//
//    //    ASSERT_EQ(sea_res, 0);
//    //    ASSERT_EQ(result_ids[0], 10 * N);
//    //    ASSERT_EQ(result_distances[0], 0);
//
//    next = chrono::high_resolution_clock::now();
//    std::cout << "query1: " << chrono::duration_cast<chrono::milliseconds>(next - beg).count() << "ms" << std::endl;
//    beg = next;
//    sea_res = Search(segment, queryInfo, 104, (float*)raw_data.data(), 16, result_ids.data(),
//    result_distances.data());
//
//    //    ASSERT_EQ(sea_res, 0);
//    //    ASSERT_EQ(result_ids[0], 10 * N);
//    //    ASSERT_EQ(result_distances[0], 0);
//
//    next = chrono::high_resolution_clock::now();
//    std::cout << "query2: " << chrono::duration_cast<chrono::milliseconds>(next - beg).count() << "ms" << std::endl;
//    beg = next;
//
//    // Close(segment);
//    // BuildIndex(segment);
//
//    next = chrono::high_resolution_clock::now();
//    std::cout << "build index: " << chrono::duration_cast<chrono::milliseconds>(next - beg).count() << "ms"
//              << std::endl;
//    beg = next;
//
//    std::vector<int64_t> result_ids2(10);
//    std::vector<float> result_distances2(10);
//
//    sea_res =
//        Search(segment, queryInfo, 104, (float*)raw_data.data(), 16, result_ids2.data(), result_distances2.data());
//
//    //    sea_res = Search(segment, nullptr, 104, result_ids2.data(),
//    //    result_distances2.data());
//
//    next = chrono::high_resolution_clock::now();
//    std::cout << "search10: " << chrono::duration_cast<chrono::milliseconds>(next - beg).count() << "ms" << std::endl;
//    beg = next;
//
//    sea_res =
//        Search(segment, queryInfo, 104, (float*)raw_data.data(), 16, result_ids2.data(), result_distances2.data());
//
//    next = chrono::high_resolution_clock::now();
//    std::cout << "search11: " << chrono::duration_cast<chrono::milliseconds>(next - beg).count() << "ms" << std::endl;
//    beg = next;
//
//    //    std::cout << "case 1" << std::endl;
//    //    for (int i = 0; i < 10; ++i) {
//    //        std::cout << result_ids[i] << "->" << result_distances[i] << std::endl;
//    //    }
//    //    std::cout << "case 2" << std::endl;
//    //    for (int i = 0; i < 10; ++i) {
//    //        std::cout << result_ids2[i] << "->" << result_distances2[i] << std::endl;
//    //    }
//    //
//    //    for (auto x : result_ids2) {
//    //        ASSERT_GE(x, 10 * N + N_del);
//    //        ASSERT_LT(x, 10 * N + N);
//    //    }
//
//    //    auto iter = 0;
//    //    for(int i = 0; i < result_ids.size(); ++i) {
//    //        auto uid = result_ids[i];
//    //        auto dis = result_distances[i];
//    //        if(uid >= 10 * N + N_del) {
//    //            auto uid2 = result_ids2[iter];
//    //            auto dis2 = result_distances2[iter];
//    //            ASSERT_EQ(uid, uid2);
//    //            ASSERT_EQ(dis, dis2);
//    //            ++iter;
//    //        }
//    //    }
//
//    DeleteCollection(collection);
//    DeleteSegment(segment);
//}

TEST(CApiTest, GetDeletedCountTest) {
    auto schema_tmp_conf = "";
    auto collection = NewCollection(schema_tmp_conf);
    auto segment = NewSegment(collection, 0);

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
    auto schema_tmp_conf = "";
    auto collection = NewCollection(schema_tmp_conf);
    auto segment = NewSegment(collection, 0);

    int N = 10000;
    auto [raw_data, timestamps, uids] = generate_data(N);
    auto line_sizeof = (sizeof(int) + sizeof(float) * 16);
    auto offset = PreInsert(segment, N);
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
//    auto segment = NewSegment(collection, 0);
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
    auto schema_tmp_conf = "";
    auto collection = NewCollection(schema_tmp_conf);
    auto segment = NewSegment(collection, 0);

    std::vector<char> raw_data;
    std::vector<uint64_t> timestamps;
    std::vector<int64_t> uids;
    int N = 10000;
    std::default_random_engine e(67);
    for (int i = 0; i < N; ++i) {
        uids.push_back(100000 + i);
        timestamps.push_back(0);
        // append vec
        float vec[16];
        for (auto& x : vec) {
            x = e() % 2000 * 0.001 - 1.0;
        }
        raw_data.insert(raw_data.end(), (const char*)std::begin(vec), (const char*)std::end(vec));
        int age = e() % 100;
        raw_data.insert(raw_data.end(), (const char*)&age, ((const char*)&age) + sizeof(age));
    }

    auto line_sizeof = (sizeof(int) + sizeof(float) * 16);

    auto offset = PreInsert(segment, N);

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
                    "topk": 10
                }
            }
        }
    })";

    namespace ser = milvus::proto::service;
    int num_queries = 10;
    int dim = 16;
    std::normal_distribution<double> dis(0, 1);
    ser::PlaceholderGroup raw_group;
    auto value = raw_group.add_placeholders();
    value->set_tag("$0");
    value->set_type(ser::PlaceholderType::VECTOR_FLOAT);
    for (int i = 0; i < num_queries; ++i) {
        std::vector<float> vec;
        for (int d = 0; d < dim; ++d) {
            vec.push_back(dis(e));
        }
        // std::string line((char*)vec.data(), (char*)vec.data() + vec.size() * sizeof(float));
        value->add_values(vec.data(), vec.size() * sizeof(float));
    }
    auto blob = raw_group.SerializeAsString();

    void* plan = nullptr;

    auto status = CreatePlan(collection, dsl_string, &plan);
    assert(status.error_code == Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(plan, blob.data(), blob.length(), &placeholderGroup);
    assert(status.error_code == Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    timestamps.clear();
    timestamps.push_back(1);

    std::vector<CQueryResult> results;
    CQueryResult res1;
    CQueryResult res2;
    auto res = Search(segment, plan, placeholderGroups.data(), timestamps.data(), 1, &res1);
    assert(res.error_code == Success);
    res = Search(segment, plan, placeholderGroups.data(), timestamps.data(), 1, &res2);
    assert(res.error_code == Success);
    results.push_back(res1);
    results.push_back(res2);

    bool is_selected[1] = {false};
    status = ReduceQueryResults(results.data(), 1, is_selected);
    assert(status.error_code == Success);
    FillTargetEntry(segment, plan, res1);
    void* reorganize_search_result = nullptr;
    status = ReorganizeQueryResults(&reorganize_search_result, placeholderGroups.data(), 1, results.data(), is_selected,
                                    1, plan);
    assert(status.error_code == Success);
    auto hits_blob_size = GetHitsBlobSize(reorganize_search_result);
    assert(hits_blob_size > 0);
    std::vector<char> hits_blob;
    hits_blob.resize(hits_blob_size);
    GetHitsBlob(reorganize_search_result, hits_blob.data());
    assert(hits_blob.data() != nullptr);
    auto num_queries_group = GetNumQueriesPeerGroup(reorganize_search_result, 0);
    assert(num_queries_group == 10);
    std::vector<int64_t> hit_size_peer_query;
    hit_size_peer_query.resize(num_queries_group);
    GetHitSizePeerQueries(reorganize_search_result, 0, hit_size_peer_query.data());
    assert(hit_size_peer_query[0] > 0);

    DeletePlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteQueryResult(res1);
    DeleteQueryResult(res2);
    DeleteMarshaledHits(reorganize_search_result);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, LoadIndexInfo) {
    // generator index
    constexpr auto DIM = 16;
    constexpr auto K = 10;

    auto N = 1024 * 10;
    auto [raw_data, timestamps, uids] = generate_data(N);
    auto indexing = std::make_shared<milvus::knowhere::IVFPQ>();
    auto conf = milvus::knowhere::Config{{milvus::knowhere::meta::DIM, DIM},
                                         {milvus::knowhere::meta::TOPK, K},
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
    status = AppendFieldInfo(c_load_index_info, field_name.data(), 0);
    assert(status.error_code == Success);
    status = AppendIndex(c_load_index_info, c_binary_set);
    assert(status.error_code == Success);
    DeleteLoadIndexInfo(c_load_index_info);
}

TEST(CApiTest, LoadIndex_Search) {
    // generator index
    constexpr auto DIM = 16;
    constexpr auto K = 10;

    auto N = 1024 * 1024 * 10;
    auto num_query = 100;
    auto [raw_data, timestamps, uids] = generate_data(N);
    auto indexing = std::make_shared<milvus::knowhere::IVFPQ>();
    auto conf = milvus::knowhere::Config{{milvus::knowhere::meta::DIM, DIM},
                                         {milvus::knowhere::meta::TOPK, K},
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
    for (int i = 0; i < std::min(num_query * K, 100); ++i) {
        std::cout << ids[i] << "->" << dis[i] << std::endl;
    }
}

TEST(CApiTest, UpdateSegmentIndex_Without_Predicate) {
    // insert data to segment
    constexpr auto DIM = 16;
    constexpr auto K = 5;

    std::string schema_string = generate_collection_shema("L2", "16", false);
    auto collection = NewCollection(schema_string.c_str());
    auto schema = ((segcore::Collection*)collection)->get_schema();
    auto segment = NewSegment(collection, 0);

    auto N = 1000 * 1000;
    auto dataset = DataGen(schema, N);
    auto vec_col = dataset.get_col<float>(0);
    auto query_ptr = vec_col.data() + 420000 * DIM;

    PreInsert(segment, N);
    auto ins_res = Insert(segment, 0, N, dataset.row_ids_.data(), dataset.timestamps_.data(), dataset.raw_.raw_data,
                          dataset.raw_.sizeof_per_row, dataset.raw_.count);
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
                    "topk": 5
                }
            }
        }
    })";

    // create place_holder_group
    int num_queries = 5;
    auto raw_group = CreatePlaceholderGroupFromBlob(num_queries, 16, query_ptr);
    auto blob = raw_group.SerializeAsString();

    // search on segment's small index
    void* plan = nullptr;
    auto status = CreatePlan(collection, dsl_string, &plan);
    assert(status.error_code == Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(plan, blob.data(), blob.length(), &placeholderGroup);
    assert(status.error_code == Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    Timestamp time = 10000000;

    CQueryResult c_search_result_on_smallIndex;
    auto res_before_load_index =
        Search(segment, plan, placeholderGroups.data(), &time, 1, &c_search_result_on_smallIndex);
    assert(res_before_load_index.error_code == Success);

    // load index to segment
    auto conf = milvus::knowhere::Config{{milvus::knowhere::meta::DIM, DIM},
                                         {milvus::knowhere::meta::TOPK, K},
                                         {milvus::knowhere::IndexParams::nlist, 100},
                                         {milvus::knowhere::IndexParams::nprobe, 10},
                                         {milvus::knowhere::IndexParams::m, 4},
                                         {milvus::knowhere::IndexParams::nbits, 8},
                                         {milvus::knowhere::Metric::TYPE, milvus::knowhere::Metric::L2},
                                         {milvus::knowhere::meta::DEVICEID, 0}};
    auto indexing = generate_index(vec_col.data(), conf, DIM, K, N, IndexEnum::INDEX_FAISS_IVFPQ);

    // gen query dataset
    auto query_dataset = milvus::knowhere::GenDataset(num_queries, DIM, query_ptr);
    auto result_on_index = indexing->Query(query_dataset, conf, nullptr);
    auto ids = result_on_index->Get<int64_t*>(milvus::knowhere::meta::IDS);
    auto dis = result_on_index->Get<float*>(milvus::knowhere::meta::DISTANCE);
    std::vector<int64_t> vec_ids(ids, ids + K * num_queries);
    std::vector<float> vec_dis;
    for (int j = 0; j < K * num_queries; ++j) {
        vec_dis.push_back(dis[j] * -1);
    }

    auto search_result_on_raw_index = (QueryResult*)c_search_result_on_smallIndex;
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
    AppendFieldInfo(c_load_index_info, "fakevec", 0);
    AppendIndex(c_load_index_info, (CBinarySet)&binary_set);

    status = UpdateSegmentIndex(segment, c_load_index_info);
    assert(status.error_code == Success);

    CQueryResult c_search_result_on_bigIndex;
    auto res_after_load_index = Search(segment, plan, placeholderGroups.data(), &time, 1, &c_search_result_on_bigIndex);
    assert(res_after_load_index.error_code == Success);

    auto search_result_on_raw_index_json = QueryResultToJson(*search_result_on_raw_index);
    auto search_result_on_bigIndex_json = QueryResultToJson((*(QueryResult*)c_search_result_on_bigIndex));
    std::cout << search_result_on_raw_index_json.dump(1) << std::endl;
    std::cout << search_result_on_bigIndex_json.dump(1) << std::endl;

    ASSERT_EQ(search_result_on_raw_index_json.dump(1), search_result_on_bigIndex_json.dump(1));

    DeleteLoadIndexInfo(c_load_index_info);
    DeletePlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteQueryResult(c_search_result_on_smallIndex);
    DeleteQueryResult(c_search_result_on_bigIndex);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, UpdateSegmentIndex_With_float_Predicate_Range) {
    // insert data to segment
    constexpr auto DIM = 16;
    constexpr auto K = 5;

    std::string schema_string = generate_collection_shema("L2", "16", false);
    auto collection = NewCollection(schema_string.c_str());
    auto schema = ((segcore::Collection*)collection)->get_schema();
    auto segment = NewSegment(collection, 0);

    auto N = 1000 * 1000;
    auto dataset = DataGen(schema, N);
    auto vec_col = dataset.get_col<float>(0);
    auto query_ptr = vec_col.data() + 420000 * DIM;

    PreInsert(segment, N);
    auto ins_res = Insert(segment, 0, N, dataset.row_ids_.data(), dataset.timestamps_.data(), dataset.raw_.raw_data,
                          dataset.raw_.sizeof_per_row, dataset.raw_.count);
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
                        "metric_type": "L2",
                        "params": {
                            "nprobe": 10
                        },
                        "query": "$0",
                        "topk": 5
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
    auto status = CreatePlan(collection, dsl_string, &plan);
    assert(status.error_code == Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(plan, blob.data(), blob.length(), &placeholderGroup);
    assert(status.error_code == Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    Timestamp time = 10000000;

    CQueryResult c_search_result_on_smallIndex;
    auto res_before_load_index =
        Search(segment, plan, placeholderGroups.data(), &time, 1, &c_search_result_on_smallIndex);
    assert(res_before_load_index.error_code == Success);

    // load index to segment
    auto conf = milvus::knowhere::Config{{milvus::knowhere::meta::DIM, DIM},
                                         {milvus::knowhere::meta::TOPK, K},
                                         {milvus::knowhere::IndexParams::nlist, 100},
                                         {milvus::knowhere::IndexParams::nprobe, 10},
                                         {milvus::knowhere::IndexParams::m, 4},
                                         {milvus::knowhere::IndexParams::nbits, 8},
                                         {milvus::knowhere::Metric::TYPE, milvus::knowhere::Metric::L2},
                                         {milvus::knowhere::meta::DEVICEID, 0}};

    auto indexing = generate_index(vec_col.data(), conf, DIM, K, N, IndexEnum::INDEX_FAISS_IVFPQ);

    // gen query dataset
    auto query_dataset = milvus::knowhere::GenDataset(num_queries, DIM, query_ptr);
    auto result_on_index = indexing->Query(query_dataset, conf, nullptr);
    auto ids = result_on_index->Get<int64_t*>(milvus::knowhere::meta::IDS);
    auto dis = result_on_index->Get<float*>(milvus::knowhere::meta::DISTANCE);
    std::vector<int64_t> vec_ids(ids, ids + K * num_queries);
    std::vector<float> vec_dis;
    for (int j = 0; j < K * num_queries; ++j) {
        vec_dis.push_back(dis[j] * -1);
    }

    auto search_result_on_raw_index = (QueryResult*)c_search_result_on_smallIndex;
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
    AppendFieldInfo(c_load_index_info, "fakevec", 0);
    AppendIndex(c_load_index_info, (CBinarySet)&binary_set);

    status = UpdateSegmentIndex(segment, c_load_index_info);
    assert(status.error_code == Success);

    CQueryResult c_search_result_on_bigIndex;
    auto res_after_load_index = Search(segment, plan, placeholderGroups.data(), &time, 1, &c_search_result_on_bigIndex);
    assert(res_after_load_index.error_code == Success);

    auto search_result_on_bigIndex = (*(QueryResult*)c_search_result_on_bigIndex);
    for (int i = 0; i < num_queries; ++i) {
        auto offset = i * K;
        ASSERT_EQ(search_result_on_bigIndex.internal_seg_offsets_[offset], 420000 + i);
        ASSERT_EQ(search_result_on_bigIndex.result_distances_[offset],
                  search_result_on_raw_index->result_distances_[offset]);
    }

    DeleteLoadIndexInfo(c_load_index_info);
    DeletePlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteQueryResult(c_search_result_on_smallIndex);
    DeleteQueryResult(c_search_result_on_bigIndex);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, UpdateSegmentIndex_With_float_Predicate_Term) {
    // insert data to segment
    constexpr auto DIM = 16;
    constexpr auto K = 5;

    std::string schema_string = generate_collection_shema("L2", "16", false);
    auto collection = NewCollection(schema_string.c_str());
    auto schema = ((segcore::Collection*)collection)->get_schema();
    auto segment = NewSegment(collection, 0);

    auto N = 1000 * 1000;
    auto dataset = DataGen(schema, N);
    auto vec_col = dataset.get_col<float>(0);
    auto query_ptr = vec_col.data() + 420000 * DIM;

    PreInsert(segment, N);
    auto ins_res = Insert(segment, 0, N, dataset.row_ids_.data(), dataset.timestamps_.data(), dataset.raw_.raw_data,
                          dataset.raw_.sizeof_per_row, dataset.raw_.count);
    assert(ins_res.error_code == Success);

    const char* dsl_string = R"({
        "bool": {
            "must": [
            {
                "term": {
                    "counter": {
                        "values": [420000, 420001, 420002, 420003, 420004]
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
                        "topk": 5
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
    auto status = CreatePlan(collection, dsl_string, &plan);
    assert(status.error_code == Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(plan, blob.data(), blob.length(), &placeholderGroup);
    assert(status.error_code == Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    Timestamp time = 10000000;

    CQueryResult c_search_result_on_smallIndex;
    auto res_before_load_index =
        Search(segment, plan, placeholderGroups.data(), &time, 1, &c_search_result_on_smallIndex);
    assert(res_before_load_index.error_code == Success);

    // load index to segment
    auto conf = milvus::knowhere::Config{{milvus::knowhere::meta::DIM, DIM},
                                         {milvus::knowhere::meta::TOPK, K},
                                         {milvus::knowhere::IndexParams::nlist, 100},
                                         {milvus::knowhere::IndexParams::nprobe, 10},
                                         {milvus::knowhere::IndexParams::m, 4},
                                         {milvus::knowhere::IndexParams::nbits, 8},
                                         {milvus::knowhere::Metric::TYPE, milvus::knowhere::Metric::L2},
                                         {milvus::knowhere::meta::DEVICEID, 0}};

    auto indexing = generate_index(vec_col.data(), conf, DIM, K, N, IndexEnum::INDEX_FAISS_IVFPQ);

    // gen query dataset
    auto query_dataset = milvus::knowhere::GenDataset(num_queries, DIM, query_ptr);
    auto result_on_index = indexing->Query(query_dataset, conf, nullptr);
    auto ids = result_on_index->Get<int64_t*>(milvus::knowhere::meta::IDS);
    auto dis = result_on_index->Get<float*>(milvus::knowhere::meta::DISTANCE);
    std::vector<int64_t> vec_ids(ids, ids + K * num_queries);
    std::vector<float> vec_dis;
    for (int j = 0; j < K * num_queries; ++j) {
        vec_dis.push_back(dis[j] * -1);
    }

    auto search_result_on_raw_index = (QueryResult*)c_search_result_on_smallIndex;
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
    AppendFieldInfo(c_load_index_info, "fakevec", 0);
    AppendIndex(c_load_index_info, (CBinarySet)&binary_set);

    status = UpdateSegmentIndex(segment, c_load_index_info);
    assert(status.error_code == Success);

    CQueryResult c_search_result_on_bigIndex;
    auto res_after_load_index = Search(segment, plan, placeholderGroups.data(), &time, 1, &c_search_result_on_bigIndex);
    assert(res_after_load_index.error_code == Success);

    auto search_result_on_bigIndex = (*(QueryResult*)c_search_result_on_bigIndex);
    for (int i = 0; i < num_queries; ++i) {
        auto offset = i * K;
        ASSERT_EQ(search_result_on_bigIndex.internal_seg_offsets_[offset], 420000 + i);
        ASSERT_EQ(search_result_on_bigIndex.result_distances_[offset],
                  search_result_on_raw_index->result_distances_[offset]);
    }

    DeleteLoadIndexInfo(c_load_index_info);
    DeletePlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteQueryResult(c_search_result_on_smallIndex);
    DeleteQueryResult(c_search_result_on_bigIndex);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, UpdateSegmentIndex_With_binary_Predicate_Range) {
    // insert data to segment
    constexpr auto DIM = 16;
    constexpr auto K = 5;

    std::string schema_string = generate_collection_shema("JACCARD", "16", true);
    auto collection = NewCollection(schema_string.c_str());
    auto schema = ((segcore::Collection*)collection)->get_schema();
    auto segment = NewSegment(collection, 0);

    auto N = 1000 * 1000;
    auto dataset = DataGen(schema, N);
    auto vec_col = dataset.get_col<uint8_t>(0);
    auto query_ptr = vec_col.data() + 420000 * DIM / 8;

    PreInsert(segment, N);
    auto ins_res = Insert(segment, 0, N, dataset.row_ids_.data(), dataset.timestamps_.data(), dataset.raw_.raw_data,
                          dataset.raw_.sizeof_per_row, dataset.raw_.count);
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
                        "topk": 5
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
    auto status = CreatePlan(collection, dsl_string, &plan);
    assert(status.error_code == Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(plan, blob.data(), blob.length(), &placeholderGroup);
    assert(status.error_code == Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    Timestamp time = 10000000;

    CQueryResult c_search_result_on_smallIndex;
    auto res_before_load_index =
        Search(segment, plan, placeholderGroups.data(), &time, 1, &c_search_result_on_smallIndex);
    assert(res_before_load_index.error_code == Success);

    // load index to segment
    auto conf = milvus::knowhere::Config{
        {milvus::knowhere::meta::DIM, DIM},
        {milvus::knowhere::meta::TOPK, K},
        {milvus::knowhere::IndexParams::nprobe, 10},
        {milvus::knowhere::IndexParams::nlist, 100},
        {milvus::knowhere::IndexParams::m, 4},
        {milvus::knowhere::IndexParams::nbits, 8},
        {milvus::knowhere::Metric::TYPE, milvus::knowhere::Metric::JACCARD},
    };

    auto indexing = generate_index(vec_col.data(), conf, DIM, K, N, IndexEnum::INDEX_FAISS_BIN_IVFFLAT);

    // gen query dataset
    auto query_dataset = milvus::knowhere::GenDataset(num_queries, DIM, query_ptr);
    auto result_on_index = indexing->Query(query_dataset, conf, nullptr);
    auto ids = result_on_index->Get<int64_t*>(milvus::knowhere::meta::IDS);
    auto dis = result_on_index->Get<float*>(milvus::knowhere::meta::DISTANCE);
    std::vector<int64_t> vec_ids(ids, ids + K * num_queries);
    std::vector<float> vec_dis;
    for (int j = 0; j < K * num_queries; ++j) {
        vec_dis.push_back(dis[j] * -1);
    }

    auto search_result_on_raw_index = (QueryResult*)c_search_result_on_smallIndex;
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
    AppendFieldInfo(c_load_index_info, "fakevec", 0);
    AppendIndex(c_load_index_info, (CBinarySet)&binary_set);

    status = UpdateSegmentIndex(segment, c_load_index_info);
    assert(status.error_code == Success);

    CQueryResult c_search_result_on_bigIndex;
    auto res_after_load_index = Search(segment, plan, placeholderGroups.data(), &time, 1, &c_search_result_on_bigIndex);
    assert(res_after_load_index.error_code == Success);

    auto search_result_on_bigIndex = (*(QueryResult*)c_search_result_on_bigIndex);
    for (int i = 0; i < num_queries; ++i) {
        auto offset = i * K;
        ASSERT_EQ(search_result_on_bigIndex.internal_seg_offsets_[offset], 420000 + i);
        ASSERT_EQ(search_result_on_bigIndex.result_distances_[offset],
                  search_result_on_raw_index->result_distances_[offset]);
    }

    DeleteLoadIndexInfo(c_load_index_info);
    DeletePlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteQueryResult(c_search_result_on_smallIndex);
    DeleteQueryResult(c_search_result_on_bigIndex);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, UpdateSegmentIndex_With_binary_Predicate_Term) {
    // insert data to segment
    constexpr auto DIM = 16;
    constexpr auto K = 5;

    std::string schema_string = generate_collection_shema("JACCARD", "16", true);
    auto collection = NewCollection(schema_string.c_str());
    auto schema = ((segcore::Collection*)collection)->get_schema();
    auto segment = NewSegment(collection, 0);

    auto N = 1000 * 1000;
    auto dataset = DataGen(schema, N);
    auto vec_col = dataset.get_col<uint8_t>(0);
    auto query_ptr = vec_col.data() + 420000 * DIM / 8;

    PreInsert(segment, N);
    auto ins_res = Insert(segment, 0, N, dataset.row_ids_.data(), dataset.timestamps_.data(), dataset.raw_.raw_data,
                          dataset.raw_.sizeof_per_row, dataset.raw_.count);
    assert(ins_res.error_code == Success);

    const char* dsl_string = R"({
        "bool": {
            "must": [
            {
                "term": {
                    "counter": {
                        "values": [420000, 420001, 420002, 420003, 420004]
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
                        "topk": 5
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
    auto status = CreatePlan(collection, dsl_string, &plan);
    assert(status.error_code == Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(plan, blob.data(), blob.length(), &placeholderGroup);
    assert(status.error_code == Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    Timestamp time = 10000000;

    CQueryResult c_search_result_on_smallIndex;
    auto res_before_load_index =
        Search(segment, plan, placeholderGroups.data(), &time, 1, &c_search_result_on_smallIndex);
    assert(res_before_load_index.error_code == Success);

    // load index to segment
    auto conf = milvus::knowhere::Config{
        {milvus::knowhere::meta::DIM, DIM},
        {milvus::knowhere::meta::TOPK, K},
        {milvus::knowhere::IndexParams::nprobe, 10},
        {milvus::knowhere::IndexParams::nlist, 100},
        {milvus::knowhere::IndexParams::m, 4},
        {milvus::knowhere::IndexParams::nbits, 8},
        {milvus::knowhere::Metric::TYPE, milvus::knowhere::Metric::JACCARD},
    };

    auto indexing = generate_index(vec_col.data(), conf, DIM, K, N, IndexEnum::INDEX_FAISS_BIN_IVFFLAT);

    // gen query dataset
    auto query_dataset = milvus::knowhere::GenDataset(num_queries, DIM, query_ptr);
    auto result_on_index = indexing->Query(query_dataset, conf, nullptr);
    auto ids = result_on_index->Get<int64_t*>(milvus::knowhere::meta::IDS);
    auto dis = result_on_index->Get<float*>(milvus::knowhere::meta::DISTANCE);
    std::vector<int64_t> vec_ids(ids, ids + K * num_queries);
    std::vector<float> vec_dis;
    for (int j = 0; j < K * num_queries; ++j) {
        vec_dis.push_back(dis[j] * -1);
    }

    auto search_result_on_raw_index = (QueryResult*)c_search_result_on_smallIndex;
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
    AppendFieldInfo(c_load_index_info, "fakevec", 0);
    AppendIndex(c_load_index_info, (CBinarySet)&binary_set);

    status = UpdateSegmentIndex(segment, c_load_index_info);
    assert(status.error_code == Success);

    CQueryResult c_search_result_on_bigIndex;
    auto res_after_load_index = Search(segment, plan, placeholderGroups.data(), &time, 1, &c_search_result_on_bigIndex);
    assert(res_after_load_index.error_code == Success);

    std::vector<CQueryResult> results;
    results.push_back(c_search_result_on_bigIndex);
    bool is_selected[1] = {false};
    status = ReduceQueryResults(results.data(), 1, is_selected);
    assert(status.error_code == Success);
    FillTargetEntry(segment, plan, c_search_result_on_bigIndex);

    auto search_result_on_bigIndex = (*(QueryResult*)c_search_result_on_bigIndex);
    for (int i = 0; i < num_queries; ++i) {
        auto offset = i * K;
        ASSERT_EQ(search_result_on_bigIndex.internal_seg_offsets_[offset], 420000 + i);
        ASSERT_EQ(search_result_on_bigIndex.result_distances_[offset],
                  search_result_on_raw_index->result_distances_[offset]);
    }

    DeleteLoadIndexInfo(c_load_index_info);
    DeletePlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteQueryResult(c_search_result_on_smallIndex);
    DeleteQueryResult(c_search_result_on_bigIndex);
    DeleteCollection(collection);
    DeleteSegment(segment);
}