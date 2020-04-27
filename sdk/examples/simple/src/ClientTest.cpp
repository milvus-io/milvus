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

#include "include/MilvusApi.h"
#include "include/BooleanQuery.h"
#include "examples/utils/TimeRecorder.h"
#include "examples/utils/Utils.h"
#include "examples/simple/src/ClientTest.h"

#include <iostream>
#include <memory>
#include <utility>
#include <vector>

namespace {

const char* COLLECTION_NAME = milvus_sdk::Utils::GenCollectionName().c_str();

constexpr int64_t COLLECTION_DIMENSION = 512;
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

void PrintEntity(const std::string& tag, const milvus::Entity& entity) {
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
ClientTest::ShowServerVersion() {
    std::string version = conn_->ServerVersion();
    std::cout << "Server version: " << version << std::endl;
}

void
ClientTest::ShowSdkVersion() {
    std::string version = conn_->ClientVersion();
    std::cout << "SDK version: " << version << std::endl;
}

void
ClientTest::ShowCollections(std::vector<std::string>& collection_array) {
    milvus::Status stat = conn_->ShowCollections(collection_array);
    std::cout << "ShowCollections function call status: " << stat.message() << std::endl;
    std::cout << "All collections: " << std::endl;
    for (auto& collection : collection_array) {
        int64_t entity_count = 0;
        stat = conn_->CountCollection(collection, entity_count);
        std::cout << "\t" << collection << "(" << entity_count << " entities)" << std::endl;
    }
}

void
ClientTest::CreateCollection(const std::string& collection_name, int64_t dim, milvus::MetricType type) {
    milvus::CollectionParam collection_param = {collection_name, dim, COLLECTION_INDEX_FILE_SIZE, type};
    milvus::Status stat = conn_->CreateCollection(collection_param);
    std::cout << "CreateCollection function call status: " << stat.message() << std::endl;
    milvus_sdk::Utils::PrintCollectionParam(collection_param);

    bool has_collection = conn_->HasCollection(collection_param.collection_name);
    if (has_collection) {
        std::cout << "Collection is created" << std::endl;
    }
}

void
ClientTest::DescribeCollection(const std::string& collection_name) {
    milvus::CollectionParam collection_param;
    milvus::Status stat = conn_->DescribeCollection(collection_name, collection_param);
    std::cout << "DescribeCollection function call status: " << stat.message() << std::endl;
    milvus_sdk::Utils::PrintCollectionParam(collection_param);
}

void
ClientTest::InsertEntities(const std::string& collection_name, int64_t dim) {
    for (int i = 0; i < ADD_ENTITY_LOOP; i++) {
        std::vector<milvus::Entity> entity_array;
        std::vector<int64_t> record_ids;
        int64_t begin_index = i * BATCH_ENTITY_COUNT;
        {  // generate vectors
            milvus_sdk::TimeRecorder rc("Build entities No." + std::to_string(i));
            milvus_sdk::Utils::BuildEntities(begin_index,
                                             begin_index + BATCH_ENTITY_COUNT,
                                             entity_array,
                                             record_ids,
                                             dim);
        }

        std::string title = "Insert " + std::to_string(entity_array.size()) + " entities No." + std::to_string(i);
        milvus_sdk::TimeRecorder rc(title);
        milvus::Status stat = conn_->Insert(collection_name, "", entity_array, record_ids);
        std::cout << "InsertEntities function call status: " << stat.message() << std::endl;
        std::cout << "Returned id array count: " << record_ids.size() << std::endl;
    }
}

void
ClientTest::BuildSearchEntities(int64_t nq, int64_t dim) {
    search_entity_array_.clear();
    search_id_array_.clear();
    for (int64_t i = 0; i < nq; i++) {
        std::vector<milvus::Entity> entity_array;
        std::vector<int64_t> record_ids;
        int64_t index = i * BATCH_ENTITY_COUNT + SEARCH_TARGET;
        milvus_sdk::Utils::BuildEntities(index, index + 1, entity_array, record_ids, dim);
        search_entity_array_.push_back(std::make_pair(record_ids[0], entity_array[0]));
        search_id_array_.push_back(record_ids[0]);
    }
}

void
ClientTest::Flush(const std::string& collection_name) {
    milvus_sdk::TimeRecorder rc("Flush");
    milvus::Status stat = conn_->FlushCollection(collection_name);
    std::cout << "FlushCollection function call status: " << stat.message() << std::endl;
}

void
ClientTest::ShowCollectionInfo(const std::string& collection_name) {
    std::string collection_info;
    milvus::Status stat = conn_->ShowCollectionInfo(collection_name, collection_info);
    std::cout << "Collection info: " << collection_info << std::endl;
    std::cout << "ShowCollectionInfo function call status: " << stat.message() << std::endl;
}

void
ClientTest::GetEntitiesByID(const std::string& collection_name,  const std::vector<int64_t>& id_array) {
    std::vector<milvus::Entity> entities;
    {
        milvus_sdk::TimeRecorder rc("GetEntitiesByID");
        milvus::Status stat = conn_->GetEntitiesByID(collection_name, id_array, entities);
        std::cout << "GetEntitiesByID function call status: " << stat.message() << std::endl;
    }

    for (size_t i = 0; i < entities.size(); i++) {
        std::string prefix = "No." + std::to_string(i) + " id:" + std::to_string(id_array[i]);
        PrintEntity(prefix, entities[i]);
    }
}

void
ClientTest::SearchEntities(const std::string& collection_name, int64_t topk, int64_t nprobe) {
    std::vector<std::string> partition_tags;
    milvus::TopKQueryResult topk_query_result;
    milvus_sdk::Utils::DoSearch(conn_, collection_name, partition_tags, topk, nprobe, search_entity_array_,
                                topk_query_result);
}

void
ClientTest::SearchEntitiesByID(const std::string& collection_name, int64_t topk, int64_t nprobe) {
    std::vector<std::string> partition_tags;
    milvus::TopKQueryResult topk_query_result;

    topk_query_result.clear();

    std::vector<int64_t> id_array;
    for (auto& pair : search_entity_array_) {
        id_array.push_back(pair.first);
    }

    JSON json_params = {{"nprobe", nprobe}};
    milvus_sdk::TimeRecorder rc("SearchByID");
    milvus::Status stat = conn_->SearchByID(collection_name,
                                            partition_tags,
                                            id_array,
                                            topk,
                                            json_params.dump(),
                                            topk_query_result);
    std::cout << "SearchByID function call status: " << stat.message() << std::endl;

    if (topk_query_result.size() != id_array.size()) {
        std::cout << "ERROR! wrong result for query by id" << std::endl;
        return;
    }

    for (size_t i = 0; i < id_array.size(); i++) {
        std::cout << "Entity " << id_array[i] << " top " << topk << " search result:" << std::endl;
        const milvus::QueryResult& one_result = topk_query_result[i];
        for (size_t j = 0; j < one_result.ids.size(); j++) {
            std::cout << "\t" << one_result.ids[j] << "\t" << one_result.distances[j] << std::endl;
        }
    }
}

void
ClientTest::CreateIndex(const std::string& collection_name, milvus::IndexType type, int64_t nlist) {
    milvus_sdk::TimeRecorder rc("Create index");
    std::cout << "Wait until create all index done" << std::endl;
    JSON json_params = {{"nlist", nlist}};
    milvus::IndexParam index1 = {collection_name, type, json_params.dump()};
    milvus_sdk::Utils::PrintIndexParam(index1);
    milvus::Status stat = conn_->CreateIndex(index1);
    std::cout << "CreateIndex function call status: " << stat.message() << std::endl;

    milvus::IndexParam index2;
    stat = conn_->DescribeIndex(collection_name, index2);
    std::cout << "DescribeIndex function call status: " << stat.message() << std::endl;
    milvus_sdk::Utils::PrintIndexParam(index2);
}

void
ClientTest::PreloadCollection(const std::string& collection_name) {
    milvus_sdk::TimeRecorder rc("Preload");
    milvus::Status stat = conn_->PreloadCollection(collection_name);
    std::cout << "PreloadCollection function call status: " << stat.message() << std::endl;
}

void
ClientTest::CompactCollection(const std::string& collection_name) {
    milvus_sdk::TimeRecorder rc("Compact");
    milvus::Status stat = conn_->CompactCollection(collection_name);
    std::cout << "CompactCollection function call status: " << stat.message() << std::endl;
}

void
ClientTest::DeleteByIds(const std::string& collection_name, const std::vector<int64_t>& id_array) {
    std::cout << "Delete entity: ";
    for (auto id : id_array) {
        std::cout << "\t" << id;
    }
    std::cout << std::endl;

    milvus::Status stat = conn_->DeleteByID(collection_name, id_array);
    std::cout << "DeleteByID function call status: " << stat.message() << std::endl;

    {
        milvus_sdk::TimeRecorder rc("Flush");
        stat = conn_->FlushCollection(collection_name);
        std::cout << "FlushCollection function call status: " << stat.message() << std::endl;
    }
}

void
ClientTest::DropIndex(const std::string& collection_name) {
    milvus::Status stat = conn_->DropIndex(collection_name);
    std::cout << "DropIndex function call status: " << stat.message() << std::endl;

    int64_t row_count = 0;
    stat = conn_->CountCollection(collection_name, row_count);
    std::cout << collection_name << "(" << row_count << " rows)" << std::endl;
}

void
ClientTest::DropCollection(const std::string& collection_name) {
    milvus::Status stat = conn_->DropCollection(collection_name);
    std::cout << "DropCollection function call status: " << stat.message() << std::endl;
}

void
ClientTest::Test() {
    std::string collection_name = COLLECTION_NAME;
    int64_t dim = COLLECTION_DIMENSION;
    milvus::MetricType metric_type = COLLECTION_METRIC_TYPE;

    ShowServerVersion();
    ShowSdkVersion();

    std::vector<std::string> table_array;
    ShowCollections(table_array);

    CreateCollection(collection_name, dim, metric_type);
    DescribeCollection(collection_name);

    InsertEntities(collection_name, dim);
    Flush(collection_name);
    ShowCollectionInfo(collection_name);

    BuildSearchEntities(NQ, dim);
    GetEntitiesByID(collection_name, search_id_array_);
//    SearchEntities(collection_name, TOP_K, NPROBE);
    SearchEntitiesByID(collection_name, TOP_K, NPROBE);

    CreateIndex(collection_name, INDEX_TYPE, NLIST);
    ShowCollectionInfo(collection_name);

    std::vector<int64_t> delete_ids = {search_id_array_[0], search_id_array_[1]};
    DeleteByIds(collection_name, delete_ids);
    CompactCollection(collection_name);

    PreloadCollection(collection_name);
    SearchEntities(collection_name, TOP_K, NPROBE); // this line get two search error since we delete two entities

    DropIndex(collection_name);
    DropCollection(collection_name);
}
