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

constexpr int64_t COLLECTION_DIMENSION = 512;
constexpr int64_t COLLECTION_INDEX_FILE_SIZE = 1024;
constexpr milvus::MetricType COLLECTION_METRIC_TYPE = milvus::MetricType::L2;
constexpr int64_t BATCH_ENTITY_COUNT = 4000;
constexpr int64_t NQ = 5;
constexpr int64_t TOP_K = 10;
constexpr int64_t NPROBE = 32;
constexpr int64_t SEARCH_TARGET = BATCH_ENTITY_COUNT / 2;  // change this value, result is different
constexpr int64_t ADD_ENTITY_LOOP = 1;
constexpr milvus::IndexType INDEX_TYPE = milvus::IndexType::IVFFLAT;
constexpr int32_t NLIST = 16384;
const char* PARTITION_TAG = "part";
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
ClientTest::CreateCollection(const std::string& collection_name) {
    milvus::FieldPtr field_ptr1 = std::make_shared<milvus::Field>();
    milvus::FieldPtr field_ptr2 = std::make_shared<milvus::Field>();
    milvus::FieldPtr field_ptr3 = std::make_shared<milvus::Field>();
    milvus::FieldPtr field_ptr4 = std::make_shared<milvus::Field>();

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
    field_ptr3->field_type = milvus::DataType::INT32;
    JSON index_param_3;
    index_param_3["name"] = "index_3";
    field_ptr3->index_params = index_param_3.dump();

    field_ptr4->field_name = "field_vec";
    field_ptr4->field_type = milvus::DataType::VECTOR_FLOAT;
    JSON index_param_4;
    index_param_4["name"] = "index_vec";
    field_ptr4->index_params = index_param_4.dump();
    JSON extra_params_4;
    extra_params_4[DIMENSION] = COLLECTION_DIMENSION;
    field_ptr4->extra_params = extra_params_4.dump();

    JSON extra_params;
    extra_params["segment_row_count"] = 1024;
    milvus::Mapping mapping = {collection_name, {field_ptr1, field_ptr2, field_ptr3, field_ptr4}};

    milvus::Status stat = conn_->CreateCollection(mapping, extra_params.dump());
    std::cout << "CreateCollection function call status: " << stat.message() << std::endl;
}

void
ClientTest::GetCollectionInfo(const std::string& collection_name) {
    milvus::Mapping mapping;
    milvus::Status stat = conn_->GetCollectionInfo(collection_name, mapping);
    milvus_sdk::Utils::PrintMapping(mapping);
    std::cout << "GetCollectionInfo function call status: " << stat.message() << std::endl;
}

void
ClientTest::InsertEntities(const std::string& collection_name) {
    for (int64_t i = 0; i < ADD_ENTITY_LOOP; i++) {
        milvus::FieldValue field_value;
        std::vector<int64_t> entity_ids;

        int64_t begin_index = i * BATCH_ENTITY_COUNT;
        {
            milvus_sdk::TimeRecorder rc("Build entities No." + std::to_string(i));
            milvus_sdk::Utils::BuildEntities(begin_index, begin_index + BATCH_ENTITY_COUNT, field_value, entity_ids,
                                             COLLECTION_DIMENSION);
        }
        entity_ids.clear();
        milvus::Status status = conn_->Insert(collection_name, "", field_value, entity_ids);
        search_id_array_.emplace_back(entity_ids[10]);
        std::cout << "InsertEntities function call status: " << status.message() << std::endl;
        std::cout << "Returned id array count: " << entity_ids.size() << std::endl;
    }
}

void
ClientTest::CountEntities(const std::string& collection_name) {
    int64_t entity_count = 0;
    auto status = conn_->CountEntities(collection_name, entity_count);
    std::cout << "Collection " << collection_name << " entity count: " << entity_count << std::endl;
}

void
ClientTest::Flush(const std::string& collection_name) {
    milvus_sdk::TimeRecorder rc("Flush");
    std::vector<std::string> collections = {collection_name};
    milvus::Status stat = conn_->Flush(collections);
    std::cout << "Flush function call status: " << stat.message() << std::endl;
}

void
ClientTest::GetCollectionStats(const std::string& collection_name) {
    std::string collection_stats;
    milvus::Status stat = conn_->GetCollectionStats(collection_name, collection_stats);
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
ClientTest::GetEntityByID(const std::string& collection_name, const std::vector<int64_t>& id_array) {
    std::string result;
    {
        milvus_sdk::TimeRecorder rc("GetEntityByID");
        milvus::Status stat = conn_->GetEntityByID(collection_name, id_array, result);
        std::cout << "GetEntityByID function call status: " << stat.message() << std::endl;
    }
    std::cout << "GetEntityByID function result: " << std::endl;
    JSON result_json = JSON::parse(result);
    for (const auto& one_result : result_json) {
        std::cout << one_result << std::endl;
    }
}

void
ClientTest::SearchEntities(const std::string& collection_name, int64_t topk, int64_t nprobe, const std::string metric_type) {
    nlohmann::json dsl_json, vector_param_json;
    milvus_sdk::Utils::GenDSLJson(dsl_json, vector_param_json, metric_type);

    std::vector<int64_t> record_ids;
    std::vector<milvus::VectorData> temp_entity_array;
    for (auto& pair : search_entity_array_) {
        temp_entity_array.push_back(pair.second);
    }

    milvus::VectorParam vector_param = {vector_param_json.dump(), temp_entity_array};

    std::vector<std::string> partition_tags;
    milvus::TopKQueryResult topk_query_result;
    auto status = conn_->Search(collection_name, partition_tags, dsl_json.dump(), vector_param, topk_query_result);

    std::cout << metric_type << " Search function call result: " << std::endl;
    milvus_sdk::Utils::PrintTopKQueryResult(topk_query_result);
    std::cout << metric_type << " Search function call status: " << status.message() << std::endl;
}

void
ClientTest::SearchEntitiesByID(const std::string& collection_name, int64_t topk, int64_t nprobe) {
    //    std::vector<std::string> partition_tags;
    //    milvus::TopKQueryResult topk_query_result;
    //
    //    topk_query_result.clear();
    //
    //    std::vector<int64_t> id_array;
    //    for (auto& pair : search_entity_array_) {
    //        id_array.push_back(pair.first);
    //    }
    //
    //    std::vector<milvus::Entity> entities;
    //    milvus::Status stat = conn_->GetEntityByID(collection_name, id_array, entities);
    //    std::cout << "GetEntityByID function call status: " << stat.message() << std::endl;
    //
    //    JSON json_params = {{"nprobe", nprobe}};
    //    milvus_sdk::TimeRecorder rc("Search");
    //    stat = conn_->Search(collection_name, partition_tags, entities, topk, json_params.dump(), topk_query_result);
    //    std::cout << "Search function call status: " << stat.message() << std::endl;
    //
    //    if (topk_query_result.size() != id_array.size()) {
    //        std::cout << "ERROR! wrong result for query by id" << std::endl;
    //        return;
    //    }
    //
    //    for (size_t i = 0; i < id_array.size(); i++) {
    //        std::cout << "Entity " << id_array[i] << " top " << topk << " search result:" << std::endl;
    //        const milvus::QueryResult& one_result = topk_query_result[i];
    //        for (size_t j = 0; j < one_result.ids.size(); j++) {
    //            std::cout << "\t" << one_result.ids[j] << "\t" << one_result.distances[j] << std::endl;
    //        }
    //    }
}

void
ClientTest::CreateIndex(const std::string& collection_name, int64_t nlist) {
    milvus_sdk::TimeRecorder rc("Create index");
    std::cout << "Wait until create all index done" << std::endl;
    JSON json_params = {{"nlist", nlist}, {"index_type", "IVF_FLAT"}};
    milvus::IndexParam index1 = {collection_name, "field_vec", "index_3", json_params.dump()};
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
    milvus::Status stat = conn_->Compact(collection_name);
    std::cout << "CompactCollection function call status: " << stat.message() << std::endl;
}

void
ClientTest::DeleteByIds(const std::string& collection_name, const std::vector<int64_t>& id_array) {
    std::cout << "Delete entity: ";
    for (auto id : id_array) {
        std::cout << "\t" << id;
    }
    std::cout << std::endl;

    milvus::Status stat = conn_->DeleteEntityByID(collection_name, id_array);
    std::cout << "DeleteByID function call status: " << stat.message() << std::endl;

    Flush(collection_name);
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
    std::string collection_name = COLLECTION_NAME;
    int64_t dim = COLLECTION_DIMENSION;
    milvus::MetricType metric_type = COLLECTION_METRIC_TYPE;

    std::vector<std::string> table_array;
    ListCollections(table_array);

    CreateCollection(collection_name);
    GetCollectionInfo(collection_name);
    GetCollectionStats(collection_name);

    ListCollections(table_array);
    CountEntities(collection_name);

    InsertEntities(collection_name);
    Flush(collection_name);
    CountEntities(collection_name);
    //    GetCollectionStats(collection_name);
    //
    BuildVectors(NQ, COLLECTION_DIMENSION);
    //    GetEntityByID(collection_name, search_id_array_);
    SearchEntities(collection_name, TOP_K, NPROBE, "L2");
    SearchEntities(collection_name, TOP_K, NPROBE, "IP");
    //    GetCollectionStats(collection_name);
    //
    //    std::vector<int64_t> delete_ids = {search_id_array_[0], search_id_array_[1]};
    //    DeleteByIds(collection_name, delete_ids);
    //    GetEntityByID(collection_name, search_id_array_);
    //    CompactCollection(collection_name);
    //
    //    LoadCollection(collection_name);
    //    SearchEntities(collection_name, TOP_K, NPROBE);  // this line get two search error since we delete two
    //    entities
    //
    //    DropIndex(collection_name, "field_vec", "index_3");
    //    DropCollection(collection_name);
}
