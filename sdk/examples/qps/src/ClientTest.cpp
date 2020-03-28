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

#include "examples/utils/TimeRecorder.h"
#include "examples/utils/Utils.h"
#include "examples/qps/src/ClientTest.h"

#include <iostream>
#include <memory>
#include <utility>
#include <vector>

namespace {

const char* COLLECTION_NAME = milvus_sdk::Utils::GenCollectionName().c_str();

constexpr int64_t COLLECTION_DIMENSION = 128;
constexpr int64_t COLLECTION_INDEX_FILE_SIZE = 512;
constexpr milvus::MetricType COLLECTION_METRIC_TYPE = milvus::MetricType::L2;
constexpr int64_t BATCH_ENTITY_COUNT = 100000;
constexpr int64_t NQ = 5;
constexpr int64_t TOP_K = 10;
constexpr int64_t NPROBE = 16;
constexpr int64_t ADD_ENTITY_LOOP = 10;
constexpr milvus::IndexType INDEX_TYPE = milvus::IndexType::IVFSQ8;
constexpr int32_t NLIST = 16384;

// parallel query setting
constexpr int32_t QUERY_THREAD_COUNT = 20;
constexpr int32_t TOTAL_QUERY_COUNT = 1000;
bool PRINT_RESULT = false;

bool
InsertEntities(std::shared_ptr<milvus::Connection>& conn) {
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
                                             COLLECTION_DIMENSION);
        }

        std::string title = "Insert " + std::to_string(entity_array.size()) + " entities No." + std::to_string(i);
        milvus_sdk::TimeRecorder rc(title);
        milvus::Status stat = conn->Insert(COLLECTION_NAME, "", entity_array, record_ids);
        std::cout << "InsertEntities function call status: " << stat.message() << std::endl;
        std::cout << "Returned id array count: " << record_ids.size() << std::endl;

        stat = conn->FlushCollection(COLLECTION_NAME);
    }

    return true;
}

void
PrintSearchResult(int64_t batch_num, const milvus::TopKQueryResult& result) {
    if (!PRINT_RESULT) {
        return;
    }

    std::cout << "No." << batch_num << " query result:" << std::endl;
    for (size_t i = 0; i < result.size(); i++) {
        std::cout << "\tNQ_" << i;
        const milvus::QueryResult& one_result = result[i];
        size_t topk = one_result.ids.size();
        for (size_t j = 0; j < topk; j++) {
            std::cout << "\t[" << one_result.ids[j] << ", " << one_result.distances[j] << "]";
        }
        std::cout << std::endl;
    }
}

}  // namespace

ClientTest::ClientTest(const std::string& address, const std::string& port)
    : server_ip_(address), server_port_(port), query_thread_pool_(QUERY_THREAD_COUNT, QUERY_THREAD_COUNT * 2) {
}

ClientTest::~ClientTest() {
}

std::shared_ptr<milvus::Connection>
ClientTest::Connect() {
    std::shared_ptr<milvus::Connection> conn;
    milvus::ConnectParam param = {server_ip_, server_port_};
    conn = milvus::Connection::Create();
    milvus::Status stat = conn->Connect(param);
    if (!stat.ok()) {
        std::string msg = "Connect function call status: " + stat.message();
        std::cout << "Connect function call status: " << stat.message() << std::endl;
    }
    return conn;
}

bool
ClientTest::BuildCollection() {
    std::shared_ptr<milvus::Connection> conn = Connect();
    if (conn == nullptr) {
        return false;
    }

    milvus::CollectionParam
        collection_param = {COLLECTION_NAME, COLLECTION_DIMENSION, COLLECTION_INDEX_FILE_SIZE, COLLECTION_METRIC_TYPE};
    auto stat = conn->CreateCollection(collection_param);
    std::cout << "CreateCollection function call status: " << stat.message() << std::endl;
    if (!stat.ok()) {
        return false;
    }

    InsertEntities(conn);
    milvus::Connection::Destroy(conn);
    return true;
}

void
ClientTest::CreateIndex() {
    std::shared_ptr<milvus::Connection> conn = Connect();
    if (conn == nullptr) {
        return;
    }

    std::cout << "Wait create index ..." << std::endl;
    JSON json_params = {{"nlist", NLIST}};
    milvus::IndexParam index = {COLLECTION_NAME, INDEX_TYPE, json_params.dump()};
    milvus_sdk::Utils::PrintIndexParam(index);
    milvus::Status stat = conn->CreateIndex(index);
    std::cout << "CreateIndex function call status: " << stat.message() << std::endl;
}

void
ClientTest::DropCollection() {
    std::shared_ptr<milvus::Connection> conn = Connect();
    if (conn == nullptr) {
        return;
    }

    milvus::Status stat = conn->DropCollection(COLLECTION_NAME);
    std::cout << "DropCollection function call status: " << stat.message() << std::endl;
}

void
ClientTest::BuildSearchEntities(std::vector<EntityList>& entity_array) {
    entity_array.clear();
    for (int64_t i = 0; i < TOTAL_QUERY_COUNT; i++) {
        std::vector<milvus::Entity> entities;
        std::vector<int64_t> record_ids;

        int64_t batch_index = i % ADD_ENTITY_LOOP;
        int64_t offset = batch_index * BATCH_ENTITY_COUNT;
        milvus_sdk::Utils::BuildEntities(offset, offset + NQ, entities, record_ids, COLLECTION_DIMENSION);
        entity_array.emplace_back(entities);
    }

//    std::cout << "Build search entities finish" << std::endl;
}

void
ClientTest::Search() {
    std::vector<EntityList> search_entities;
    BuildSearchEntities(search_entities);

    query_thread_results_.clear();

    auto start = std::chrono::system_clock::now();
    // multi-threads query
    for (int32_t i = 0; i < TOTAL_QUERY_COUNT; i++) {
        query_thread_results_.push_back(query_thread_pool_.enqueue(&ClientTest::SearchWorker,
                                                                   this,
                                                                   search_entities[i]));
    }

    // wait all query return
    for (auto& iter : query_thread_results_) {
        iter.wait();
    }

    std::chrono::system_clock::time_point end = std::chrono::system_clock::now();
    int64_t span = (std::chrono::duration_cast<std::chrono::milliseconds>(end - start)).count();
    double sec = (double)span / 1000.0;
    std::cout << "data information: dimension = " << COLLECTION_DIMENSION << " row_count = "
              << BATCH_ENTITY_COUNT * ADD_ENTITY_LOOP << std::endl;
    std::cout << "search parameters: nq = " << NQ << " topk = " << TOP_K << " nprobe = " << NPROBE << std::endl;
    std::cout << "search threads = " << QUERY_THREAD_COUNT << " total_query_count = " << TOTAL_QUERY_COUNT << std::endl;
    std::cout << "search " << TOTAL_QUERY_COUNT << " times totally cost: " << span << " ms" << std::endl;
    std::cout << "search qps = " << TOTAL_QUERY_COUNT / sec << std::endl;

    // print result
    int64_t index = 0;
    for (auto& iter : query_thread_results_) {
        PrintSearchResult(index++, iter.get());
    }
}

milvus::TopKQueryResult
ClientTest::SearchWorker(EntityList& entities) {
    milvus::TopKQueryResult res;

    std::shared_ptr<milvus::Connection> conn;
    milvus::ConnectParam param = {server_ip_, server_port_};
    conn = milvus::Connection::Create();
    milvus::Status stat = conn->Connect(param);
    if (!stat.ok()) {
        milvus::Connection::Destroy(conn);
        std::string msg = "Connect function call status: " + stat.message();
        std::cout << msg << std::endl;
        return res;
    }

    JSON json_params = {{"nprobe", NPROBE}};
    std::vector<std::string> partition_tags;
    stat = conn->Search(COLLECTION_NAME,
                        partition_tags,
                        entities,
                        TOP_K,
                        json_params.dump(),
                        res);
    if (!stat.ok()) {
        milvus::Connection::Destroy(conn);
        std::string msg = "Search function call status: " + stat.message();
        std::cout << msg << std::endl;
        return res;
    }

    milvus::Connection::Destroy(conn);
    return res;
}

void
ClientTest::Test() {
    if (!BuildCollection()) {
        return;
    }

    // search without index
    std::cout << "Search without index" << std::endl;
    Search();

    CreateIndex();

    // search with index
    std::cout << "Search with index" << std::endl;
    Search();

    DropCollection();
}
