// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include "examples/hybrid/src/ClientTest.h"
#include "examples/utils/TimeRecorder.h"
#include "examples/utils/Utils.h"
#include "include/BooleanQuery.h"
#include "include/MilvusApi.h"

#include <unistd.h>
#include <iostream>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

namespace {

const char* COLLECTION_NAME = milvus_sdk::Utils::GenCollectionName().c_str();

constexpr int64_t COLLECTION_DIMENSION = 128;
constexpr int64_t COLLECTION_INDEX_FILE_SIZE = 1024;
constexpr milvus::MetricType COLLECTION_METRIC_TYPE = milvus::MetricType::L2;
constexpr int64_t BATCH_ENTITY_COUNT = 100000;
constexpr int64_t NQ = 5;
constexpr int64_t TOP_K = 10;
constexpr int64_t NPROBE = 32;
constexpr int64_t SEARCH_TARGET = BATCH_ENTITY_COUNT / 2;  // change this value, result is different
constexpr int64_t ADD_ENTITY_LOOP = 5;
constexpr milvus::IndexType INDEX_TYPE = milvus::IndexType::IVFSQ8;
constexpr int32_t NLIST = 16384;
constexpr uint64_t FIELD_NUM = 3;

}  // namespace

ClientTest::ClientTest(const std::string& address, const std::string& port) {
    milvus::ConnectParam param = {address, port};
    conn_ = milvus::Connection::Create();
    milvus::Status stat = conn_->Connect(param);
    std::cout << "Connect function call status: " << stat.message() << std::endl;
}

ClientTest::~ClientTest() {
    milvus::Status stat = milvus::Connection::Destroy(conn_);
    std::cout << "Destroy connection function call status: " << stat.message() << std::endl;
}

void
ClientTest::CreateCollection(const std::string& collection_name) {
    milvus::FieldPtr field_ptr1 = std::make_shared<milvus::Field>();
    milvus::FieldPtr field_ptr2 = std::make_shared<milvus::Field>();
    milvus::FieldPtr field_ptr3 = std::make_shared<milvus::Field>();
    field_ptr1->field_name = "field_1";
    field_ptr1->field_type = milvus::DataType::INT64;
    JSON index_param_1;
    index_param_1["name"] = "index_1";
    field_ptr1->index_params = index_param_1.dump();

    field_ptr2->field_name = "field_2";
    field_ptr2->field_type = milvus::DataType::FLOAT;
    JSON index_param_2;
    index_param_2["name"] = "index_2";
    field_ptr2->index_params = index_param_2.dump();

    field_ptr3->field_name = "field_3";
    field_ptr3->field_type = milvus::DataType::FLOAT_VECTOR;
    JSON index_param_3;
    index_param_3["name"] = "index_3";
    index_param_3["index_type"] = "IVFFLAT";
    field_ptr3->index_params = index_param_3;
    JSON extra_params;
    extra_params["dimension"] = COLLECTION_DIMENSION;
    field_ptr3->extram_params = extra_params.dump();

    milvus::Mapping mapping = {collection_name, {field_ptr1, field_ptr2, field_ptr3}};

    milvus::Status stat = conn_->CreateCollection(mapping);
    std::cout << "CreateCollection function call status: " << stat.message() << std::endl;
}

void
ClientTest::Flush(const std::string& collection_name) {
    milvus_sdk::TimeRecorder rc("Flush");
    std::vector<std::string> collections = {collection_name};
    milvus::Status stat = conn_->Flush(collections);
    std::cout << "Flush function call status: " << stat.message() << std::endl;
}

void
ClientTest::Insert(std::string& collection_name, int64_t row_num) {
    milvus::FieldValue field_value;

    std::vector<int64_t> value1;
    std::vector<float> value2;
    value1.resize(row_num);
    value2.resize(row_num);
    for (uint64_t i = 0; i < row_num; ++i) {
        value1[i] = i;
        value2[i] = (float)(i + row_num);
    }
    field_value.int64_value.insert(std::make_pair("field_1", value1));
    field_value.float_value.insert(std::make_pair("field_2", value2));

    std::unordered_map<std::string, std::vector<milvus::VectorData>> vector_value;
    std::vector<milvus::VectorData> entity_array;
    std::vector<int64_t> record_ids;
    {  // generate vectors
        milvus_sdk::Utils::BuildEntities(0, row_num, entity_array, record_ids, 128);
    }

    field_value.vector_value.insert(std::make_pair("field_3", entity_array));
    milvus::Status status = conn_->Insert(collection_name, "", field_value, record_ids);
    std::cout << "InsertHybridEntities function call status: " << status.message() << std::endl;
}

void
ClientTest::SearchPB(std::string& collection_name) {
    std::vector<std::string> partition_tags;
    milvus::TopKQueryResult topk_query_result;

    auto leaf_queries = milvus_sdk::Utils::GenLeafQuery();

    // must
    auto must_clause = std::make_shared<milvus::BooleanQuery>(milvus::Occur::MUST);
    must_clause->AddLeafQuery(leaf_queries[0]);
    must_clause->AddLeafQuery(leaf_queries[1]);
    must_clause->AddLeafQuery(leaf_queries[2]);

    auto query_clause = std::make_shared<milvus::BooleanQuery>();
    query_clause->AddBooleanQuery(must_clause);

    std::string extra_params;
    milvus::Status status =
        conn_->SearchPB(collection_name, partition_tags, query_clause, extra_params, topk_query_result);

    milvus_sdk::Utils::PrintTopKHybridQueryResult(topk_query_result);
    std::cout << "HybridSearch function call status: " << status.message() << std::endl;
}

void
ClientTest::Search(std::string& collection_name) {
    nlohmann::json dsl_json, vector_param_json;
    milvus_sdk::Utils::GenDSLJson(dsl_json, vector_param_json);

    std::vector<milvus::VectorData> entity_array;
    std::vector<int64_t> record_ids;
    {  // generate vectors
        milvus_sdk::Utils::ConstructVector(NQ, COLLECTION_DIMENSION, entity_array);
    }

    milvus::VectorParam vector_param = {vector_param_json.dump(), entity_array};

    std::vector<std::string> partition_tags;
    milvus::TopKQueryResult topk_query_result;
    auto status = conn_->Search(collection_name, partition_tags, dsl_json.dump(), vector_param, topk_query_result);

    milvus_sdk::Utils::PrintTopKHybridQueryResult(topk_query_result);
    std::cout << "HybridSearch function call status: " << status.message() << std::endl;
}

void
ClientTest::GetEntityByID(const std::string& collection_name, const std::vector<int64_t>& id_array) {
    std::string result;
    {
        milvus_sdk::TimeRecorder rc("GetHybridEntityByID");
        milvus::Status stat = conn_->GetEntityByID(collection_name, id_array, result);
        std::cout << "GetEntitiesByID function call status: " << stat.message() << std::endl;
    }

    std::cout << "GetEntityByID function result: " << result;
}

void
ClientTest::TestHybrid() {
    std::string collection_name = "HYBRID_TEST";
    CreateCollection(collection_name);
    Insert(collection_name, 10000);
    Flush(collection_name);
    //    sleep(2);
    //    HybridSearchPB(collection_name);
    Search(collection_name);
}
