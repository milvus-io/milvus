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

#include "examples/binary_vector/src/ClientTest.h"
#include "examples/utils/TimeRecorder.h"
#include "examples/utils/Utils.h"
#include "include/MilvusApi.h"

#include <iostream>
#include <memory>
#include <random>
#include <utility>
#include <vector>

namespace {

constexpr int64_t BATCH_ENTITY_COUNT = 10000;
constexpr int64_t NQ = 5;
constexpr int64_t TOP_K = 10;
constexpr int64_t NPROBE = 32;
constexpr int64_t SEARCH_TARGET = 5000;  // change this value, result is different, ensure less than BATCH_ENTITY_COUNT
constexpr int64_t ADD_ENTITY_LOOP = 10;
constexpr int64_t DIMENSION = 128;

void
BuildBinaryVectors(int64_t from, int64_t to, std::vector<milvus::VectorData>& entity_array,
                   std::vector<int64_t>& entity_ids, int64_t dimension) {
    if (to <= from) {
        return;
    }

    entity_array.clear();
    entity_ids.clear();

    int64_t dim_byte = ceil(dimension / 8);
    if ((dimension % 8) > 0) {
        dim_byte++;
    }
    for (int64_t k = from; k < to; k++) {
        milvus::VectorData entity;
        entity.binary_data.resize(dim_byte);
        for (int64_t i = 0; i < dim_byte; i++) {
            entity.binary_data[i] = (uint8_t)lrand48();
        }

        entity_array.emplace_back(entity);
        entity_ids.push_back(k);
    }
}

void
TestProcess(std::shared_ptr<milvus::Connection> connection, const milvus::Mapping& mapping,
            const milvus::IndexParam& index_param) {
    milvus::Status stat;

    {  // create collection
        JSON extra_params;
        extra_params["segment_row_limit"] = 1000000;
        extra_params["auto_id"] = false;
        stat = connection->CreateCollection(mapping, extra_params.dump());
        std::cout << "CreateCollection function call status: " << stat.message() << std::endl;
        milvus_sdk::Utils::PrintCollectionParam(mapping);
    }

    {
        milvus::Mapping get_mapping;
        stat = connection->GetCollectionInfo(mapping.collection_name, get_mapping);
        std::cout << "GetCollectionInfo function call status: " << stat.message() << std::endl;
    }

    std::vector<std::pair<int64_t, milvus::VectorData>> search_entity_array;
    {  // insert vectors
        for (int i = 0; i < ADD_ENTITY_LOOP; i++) {
            milvus::FieldValue field_value;

            std::vector<milvus::VectorData> entity_array;
            std::vector<int64_t> entity_ids;
            int64_t begin_index = i * BATCH_ENTITY_COUNT;
            {  // generate vectors
                milvus_sdk::TimeRecorder rc("Build entities No." + std::to_string(i));
                BuildBinaryVectors(begin_index, begin_index + BATCH_ENTITY_COUNT, entity_array, entity_ids, DIMENSION);
            }

            if (search_entity_array.size() < NQ) {
                search_entity_array.push_back(std::make_pair(entity_ids[0], entity_array[0]));
            }

            std::vector<int64_t> int64_data(BATCH_ENTITY_COUNT);
            for (int j = begin_index; j < begin_index + BATCH_ENTITY_COUNT; j++) {
                int64_data[j - begin_index] = j - begin_index;
            }
            field_value.int64_value.insert(std::make_pair("field_1", int64_data));
            field_value.vector_value.insert(std::make_pair("field_vec", entity_array));

            std::string title = "Insert " + std::to_string(entity_array.size()) + " entities No." + std::to_string(i);
            milvus_sdk::TimeRecorder rc(title);
            stat = connection->Insert(mapping.collection_name, "", field_value, entity_ids);
            std::cout << "Insert function call status: " << stat.message() << std::endl;
            std::cout << "Returned id array count: " << entity_ids.size() << std::endl;
        }
    }

    {  // flush buffer
        std::vector<std::string> collections = {mapping.collection_name};
        stat = connection->Flush(collections);
        std::cout << "Flush function call status: " << stat.message() << std::endl;
    }

    {  // search vectors
//        std::string metric_type = "HAMMING";
        std::string metric_type = "JACCARD";
//        std::string metric_type = "TANIMOTO";

        nlohmann::json dsl_json, vector_param_json;
        milvus_sdk::Utils::GenPureVecDSLJson(dsl_json, vector_param_json, metric_type);

        std::vector<milvus::VectorData> temp_entity_array;
        for (auto& pair : search_entity_array) {
            temp_entity_array.push_back(pair.second);
        }

        milvus::VectorParam vector_param = {vector_param_json.dump(), temp_entity_array};

        std::vector<std::string> partition_tags;
        milvus::TopKQueryResult topk_query_result;
        auto status = connection->Search(mapping.collection_name, partition_tags, dsl_json.dump(), vector_param, topk_query_result);

        std::cout << metric_type << " Search function call result: " << std::endl;
        milvus_sdk::Utils::PrintTopKQueryResult(topk_query_result);
        std::cout << metric_type << " Search function call status: " << status.message() << std::endl;
    }
/*
    {  // wait unit build index finish
        milvus_sdk::TimeRecorder rc("Create index");
        std::cout << "Wait until create all index done" << std::endl;
        milvus_sdk::Utils::PrintIndexParam(index_param);
        stat = connection->CreateIndex(index_param);
        std::cout << "CreateIndex function call status: " << stat.message() << std::endl;
    }

    {  // search vectors
        std::vector<std::string> partition_tags;
        milvus::TopKQueryResult topk_query_result;
        milvus_sdk::Utils::DoSearch(connection, mapping.collection_name, partition_tags, TOP_K, NPROBE,
                                    search_entity_array, topk_query_result);
    }
*/
    {  // drop collection
        stat = connection->DropCollection(mapping.collection_name);
        std::cout << "DropCollection function call status: " << stat.message() << std::endl;
    }
}

}  // namespace

void
ClientTest::Test(const std::string& address, const std::string& port) {
    std::shared_ptr<milvus::Connection> connection = milvus::Connection::Create();
    {  // connect server
        milvus::ConnectParam param = {address, port};
        auto stat = connection->Connect(param);
        std::cout << "Connect function call status: " << stat.message() << std::endl;
        if (!stat.ok()) {
            return;
        }
    }

    {
        milvus::FieldPtr field_ptr1 = std::make_shared<milvus::Field>();
        milvus::FieldPtr field_ptr2 = std::make_shared<milvus::Field>();

        field_ptr1->field_name = "field_1";
        field_ptr1->field_type = milvus::DataType::INT64;
        JSON index_param_1;
        index_param_1["name"] = "index_1";
        field_ptr1->index_params = index_param_1.dump();

        field_ptr2->field_name = "field_vec";
        field_ptr2->field_type = milvus::DataType::VECTOR_BINARY;
        JSON index_param_2;
        index_param_2["name"] = "index_vec";
        field_ptr2->index_params = index_param_2.dump();
        JSON extra_params;
        extra_params["dim"] = DIMENSION;
        field_ptr2->extra_params = extra_params.dump();

        milvus::Mapping mapping = {"collection_1", {field_ptr1, field_ptr2}};

        JSON json_params = {{"index_type", "BIN_IVF_FLAT"}, {"nlist", 1024}};
        milvus::IndexParam index_param = {mapping.collection_name, "field_vec", json_params.dump()};

        TestProcess(connection, mapping, index_param);
    }

    //    {
    //        milvus::Mapping collection_param = {"collection_2",
    //                                            512,  // dimension
    //                                            512,  // index file size
    //                                            milvus::MetricType::SUBSTRUCTURE};
    //
    //        JSON json_params = {};
    //        milvus::IndexParam index_param = {collection_param.collection_name, milvus::IndexType::FLAT,
    //                                          json_params.dump()};
    //
    //        TestProcess(connection, collection_param, index_param);
    //    }
    //
    //    {
    //        milvus::Mapping mapping = {"collection_3",
    //                                   128,   // dimension
    //                                   1024,  // index file size
    //                                   milvus::MetricType::SUPERSTRUCTURE};
    //
    //        JSON json_params = {};
    //        milvus::IndexParam index_param = {collection_param.collection_name, milvus::IndexType::FLAT,
    //                                          json_params.dump()};
    //
    //        TestProcess(connection, collection_param, index_param);
    //    }

    milvus::Connection::Destroy(connection);
}
