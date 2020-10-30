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

#include "examples/simple/src/ClientTest.h"
#include "examples/utils/TimeRecorder.h"
#include "examples/utils/Utils.h"
#include "include/BooleanQuery.h"
#include "include/MilvusApi.h"

#include <iostream>
#include <memory>
#include <utility>
#include <vector>

namespace {

const char* COLLECTION_NAME = milvus_sdk::Utils::GenCollectionName().c_str();
const char* PARTITION_TAG = "American";

constexpr int64_t COLLECTION_DIMENSION = 8;
constexpr milvus::MetricType COLLECTION_METRIC_TYPE = milvus::MetricType::L2;
constexpr int64_t BATCH_ENTITY_COUNT = 3;
constexpr int64_t NQ = 1;
constexpr int64_t TOP_K = 3;
constexpr int64_t NPROBE = 16;
constexpr int64_t SEARCH_TARGET = BATCH_ENTITY_COUNT / 2;  // change this value, result is different
constexpr int64_t ADD_ENTITY_LOOP = 1;
constexpr int32_t NLIST = 1024;
const char* DIMENSION = "dim";
const char* METRICTYPE = "metric_type";
const char* INDEXTYPE = "index_type";

void
PrintEntity(const std::string& tag, const milvus::VectorData& entity) {
    std::cout << tag << "\t[";
    for (size_t i = 0; i < entity.float_data.size(); i++) {
        if (i != 0) {
            std::cout << ", ";
        }
        std::cout << entity.float_data[i];
    }
    std::cout << "]" << std::endl;
}

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
ClientTest::ListCollections(std::vector<std::string>& collection_array) {
    milvus::Status stat = conn_->ListCollections(collection_array);
    std::cout << "ListCollections function call status: " << stat.message() << std::endl;
    std::cout << "Collection list: " << std::endl;
    for (auto& collection : collection_array) {
        int64_t entity_count = 0;
        stat = conn_->CountEntities(collection, entity_count);
        std::cout << "\t" << collection << " (" << entity_count << " entities)" << std::endl;
    }
}

void
ClientTest::CreateCollection() {
    milvus::FieldPtr field1 = std::make_shared<milvus::Field>("release_year", milvus::DataType::INT32, "");
    milvus::FieldPtr field2 = std::make_shared<milvus::Field>("duration", milvus::DataType::INT32, "");
    nlohmann::json vector_param = {{"dim", COLLECTION_DIMENSION}};
    milvus::FieldPtr field3 =
        std::make_shared<milvus::Field>("embedding", milvus::DataType::VECTOR_FLOAT, vector_param.dump());

    nlohmann::json json_param;
    json_param = {{"auto_id", false}, {"segment_row_limit", 4096}};
    milvus::Mapping mapping = {COLLECTION_NAME, {field1, field2, field3}, json_param.dump()};

    milvus::Status status = conn_->CreateCollection(mapping);
    std::cout << "CreateCollection function call status: " << status.message() << std::endl;
}

void
ClientTest::CreatePartition() {
    milvus::PartitionParam param = milvus::PartitionParam{COLLECTION_NAME, PARTITION_TAG};
    auto status = conn_->CreatePartition(param);
    std::cout << "CreatePartition function call status: " << status.message() << std::endl;
}

void
ClientTest::GetCollectionInfo() {
    milvus::Mapping mapping;
    milvus::Status status = conn_->GetCollectionInfo(COLLECTION_NAME, mapping);
    milvus_sdk::Utils::PrintMapping(mapping);
    std::cout << "GetCollectionInfo function call status: " << status.message() << std::endl;
}

void
ClientTest::ListPartitions() {
    milvus::PartitionTagList partition_list;
    auto status = conn_->ListPartitions(COLLECTION_NAME, partition_list);
    std::cout << "Partitions: ";
    for (const auto& part : partition_list) {
        std::cout << part << std::endl;
    }
    std::cout << "ListPartitions function call status: " << status.message() << std::endl;
}

void
ClientTest::InsertEntities() {
    std::vector<int32_t> duration{208, 226, 252};
    std::vector<int32_t> release_year{2001, 2002, 2003};
    std::vector<milvus::VectorData> embedding;
    milvus_sdk::Utils::BuildVectors(COLLECTION_DIMENSION, 3, embedding);

    milvus::FieldValue field_value;
    std::unordered_map<std::string, std::vector<int32_t>> int32_value = {{"duration", duration},
                                                                         {"release_year", release_year}};

    std::unordered_map<std::string, std::vector<milvus::VectorData>> vector_value = {{"embedding", embedding}};
    field_value.int32_value = int32_value;
    field_value.vector_value = vector_value;

    std::vector<int64_t> id_array = {1, 2, 3};
    auto status = conn_->Insert(COLLECTION_NAME, PARTITION_TAG, field_value, id_array);
    std::cout << "InsertEntities function call status: " << status.message() << std::endl;
}

void
ClientTest::CountEntities(int64_t& entity_count) {
    auto status = conn_->CountEntities(COLLECTION_NAME, entity_count);
    std::cout << "Collection " << COLLECTION_NAME << " entity count: " << entity_count << std::endl;
}

void
ClientTest::Flush() {
    milvus_sdk::TimeRecorder rc("Flush");
    std::vector<std::string> collections = {COLLECTION_NAME};
    milvus::Status stat = conn_->Flush(collections);
    std::cout << "Flush function call status: " << stat.message() << std::endl;
}

void
ClientTest::GetCollectionStats() {
    std::string collection_stats;
    milvus::Status stat = conn_->GetCollectionStats(COLLECTION_NAME, collection_stats);
    std::cout << "Collection stats: " << collection_stats << std::endl;
    std::cout << "GetCollectionStats function call status: " << stat.message() << std::endl;
}

void
ClientTest::BuildVectors(int64_t nq, int64_t dimension) {
    search_entity_array_.clear();
    search_id_array_.clear();

    for (int64_t i = 0; i < nq; i++) {
        std::vector<milvus::VectorData> entity_array;
        std::vector<int64_t> record_ids;
        int64_t index = i * BATCH_ENTITY_COUNT + SEARCH_TARGET;
        milvus_sdk::Utils::ConstructVectors(index, index + 1, entity_array, record_ids, dimension);
        search_entity_array_.push_back(std::make_pair(record_ids[0], entity_array[0]));
        search_id_array_.push_back(record_ids[0]);
    }
}

void
ClientTest::GetEntityByID(const std::vector<int64_t>& id_array) {
    milvus::Entities entities;
    {
        milvus_sdk::TimeRecorder rc("GetEntityByID");
        milvus::Status stat = conn_->GetEntityByID(COLLECTION_NAME, id_array, entities);
        std::cout << "GetEntityByID function call status: " << stat.message() << std::endl;
    };

    std::cout << "GetEntityByID function result: " << std::endl;
    for (const auto& entity : entities) {
        std::cout << "Entity id: " << entity.entity_id << std::endl;
        for (const auto& data : entity.scalar_data) {
            if (data.first == "duration" || data.first == "release_year") {
                std::cout << data.first << ": " << std::any_cast<int32_t>(data.second) << std::endl;
            }
        }
        for (const auto& data : entity.vector_data) {
            auto embedding = data.second.float_data;
            std::cout << data.first << ":";
            for (const auto& v : embedding) {
                std::cout << v << " ";
            }
            std::cout << std::endl;
        }
    }
}

void
ClientTest::SearchEntities() {
    nlohmann::json dsl_json, vector_param_json;
    milvus_sdk::Utils::GenDSLJson(dsl_json, vector_param_json, TOP_K, "L2");

    std::vector<milvus::VectorData> query_embedding;
    milvus_sdk::Utils::BuildVectors(COLLECTION_DIMENSION, 1, query_embedding);

    milvus::VectorParam vector_param = {vector_param_json.dump(), query_embedding};

    std::vector<std::string> get_fields{"duration", "release_year", "embedding"};
    nlohmann::json json_params = {{"fields", get_fields}};

    std::vector<std::string> partition_tags;
    milvus::TopKQueryResult topk_query_result;
    auto status = conn_->Search(COLLECTION_NAME, partition_tags, dsl_json.dump(), vector_param, json_params.dump(),
                                topk_query_result);

    std::cout << " Search function call result: " << std::endl;
    milvus_sdk::Utils::PrintTopKQueryResult(topk_query_result);
}

void
ClientTest::CreateIndex(const std::string& collection_name, int64_t nlist) {
    milvus_sdk::TimeRecorder rc("Create index");
    std::cout << "Wait until create all index done" << std::endl;
    JSON json_params = {{"index_type", "IVF_FLAT"}, {"metric_type", "L2"}, {"params", {{"nlist", nlist}}}};
    // JSON json_params = {{"index_type", "IVF_PQ"}, {"metric_type", "L2"}, {"params", {{"nlist", nlist}, {"m", 16}}}};
    milvus::IndexParam index1 = {collection_name, "field_vec", json_params.dump()};
    milvus_sdk::Utils::PrintIndexParam(index1);
    milvus::Status stat = conn_->CreateIndex(index1);
    std::cout << "CreateIndex function call status: " << stat.message() << std::endl;
    milvus_sdk::Utils::PrintIndexParam(index1);
}

void
ClientTest::LoadCollection(const std::string& collection_name) {
    milvus_sdk::TimeRecorder rc("Preload");
    milvus::Status stat = conn_->LoadCollection(collection_name);
    std::cout << "PreloadCollection function call status: " << stat.message() << std::endl;
}

void
ClientTest::CompactCollection(const std::string& collection_name) {
    milvus_sdk::TimeRecorder rc("Compact");
    milvus::Status stat = conn_->Compact(collection_name, 0.1);
    std::cout << "CompactCollection function call status: " << stat.message() << std::endl;
}

void
ClientTest::DeleteByIds(const std::vector<int64_t>& id_array) {
    auto status = conn_->DeleteEntityByID(COLLECTION_NAME, id_array);
    std::cout << "DeleteByID function call status: " << status.message() << std::endl;
}

void
ClientTest::DropPartition() {
    milvus::PartitionParam param = {COLLECTION_NAME, PARTITION_TAG};
    auto status = conn_->DropPartition(param);
    std::cout << "DropPartition function call status: " << status.message() << std::endl;
}

void
ClientTest::DropIndex(const std::string& collection_name, const std::string& field_name,
                      const std::string& index_name) {
    milvus::Status stat = conn_->DropIndex(collection_name, field_name, index_name);
    std::cout << "DropIndex function call status: " << stat.message() << std::endl;

    int64_t row_count = 0;
    stat = conn_->CountEntities(collection_name, row_count);
    std::cout << collection_name << "(" << row_count << " rows)" << std::endl;
}

void
ClientTest::DropCollection(const std::string& collection_name) {
    milvus::Status stat = conn_->DropCollection(collection_name);
    std::cout << "DropCollection function call status: " << stat.message() << std::endl;
}

void
ClientTest::Test() {
    std::vector<std::string> collection_array;
    ListCollections(collection_array);
    for (const auto& collection : collection_array) {
        DropCollection(collection);
    }

    CreateCollection();
    CreatePartition();

    std::cout << "--------get collection info--------" << std::endl;
    GetCollectionInfo();
    std::cout << "\n----------list partitions----------" << std::endl;
    ListPartitions();

    std::cout << "\n----------insert----------" << std::endl;
    InsertEntities();

    int64_t before_flush_counts = 0;
    int64_t after_flush_counts = 0;
    CountEntities(before_flush_counts);
    Flush();
    CountEntities(after_flush_counts);
    std::cout << "\n----------flush----------" << std::endl;
    std::cout << "There are " << before_flush_counts << " films in collection " << COLLECTION_NAME << " before flush"
              << std::endl;
    std::cout << "There are " << after_flush_counts << " films in collection " << COLLECTION_NAME << " after flush"
              << std::endl;

    std::cout << "\n----------get collection stats----------\n";
    GetCollectionStats();

    std::cout << "\n----------get entity by id = 1, id = 200----------\n";
    std::vector<int64_t> id_array = {1, 200};
    GetEntityByID(id_array);

    std::cout << "\n----------search----------\n";
    SearchEntities();

    std::vector<int64_t> delete_id_array = {1, 2};
    std::cout << "\n----------delete id = 1, id = 2----------\n";
    DeleteByIds(delete_id_array);
    Flush();
    GetEntityByID(delete_id_array);

    int64_t counts_in_collection;
    CountEntities(counts_in_collection);
    std::cout << "There are " << counts_in_collection << " entities after delete films with 1, 2\n";

    DropCollection(COLLECTION_NAME);
}
