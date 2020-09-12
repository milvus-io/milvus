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
#include "examples/binary_vector/src/ClientTest.h"
#include "examples/utils/TimeRecorder.h"
#include "examples/utils/Utils.h"

#include <iostream>
#include <memory>
#include <utility>
#include <vector>
#include <random>

namespace {

constexpr int64_t BATCH_ENTITY_COUNT = 100000;
constexpr int64_t NQ = 5;
constexpr int64_t TOP_K = 10;
constexpr int64_t NPROBE = 32;
constexpr int64_t SEARCH_TARGET = 5000;  // change this value, result is different, ensure less than BATCH_ENTITY_COUNT
constexpr int64_t ADD_ENTITY_LOOP = 10;

void
BuildBinaryVectors(int64_t from, int64_t to, std::vector<milvus::Entity>& entity_array,
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
        milvus::Entity entity;
        entity.binary_data.resize(dim_byte);
        for (int64_t i = 0; i < dim_byte; i++) {
            entity.binary_data[i] = (uint8_t)lrand48();
        }

        entity_array.emplace_back(entity);
        entity_ids.push_back(k);
    }
}

void
TestProcess(std::shared_ptr<milvus::Connection> connection,
            const milvus::CollectionParam& collection_param,
            const milvus::IndexParam& index_param) {
    milvus::Status stat;

    {  // create collection
        stat = connection->CreateCollection(collection_param);
        std::cout << "CreateCollection function call status: " << stat.message() << std::endl;
        milvus_sdk::Utils::PrintCollectionParam(collection_param);
    }

    std::vector<std::pair<int64_t, milvus::Entity>> search_entity_array;
    {  // insert vectors
        for (int i = 0; i < ADD_ENTITY_LOOP; i++) {
            std::vector<milvus::Entity> entity_array;
            std::vector<int64_t> entity_ids;
            int64_t begin_index = i * BATCH_ENTITY_COUNT;
            {  // generate vectors
                milvus_sdk::TimeRecorder rc("Build entities No." + std::to_string(i));
                BuildBinaryVectors(begin_index,
                                   begin_index + BATCH_ENTITY_COUNT,
                                   entity_array,
                                   entity_ids,
                                   collection_param.dimension);
            }

            if (search_entity_array.size() < NQ) {
                search_entity_array.push_back(std::make_pair(entity_ids[SEARCH_TARGET], entity_array[SEARCH_TARGET]));
            }

            std::string title = "Insert " + std::to_string(entity_array.size()) + " entities No." + std::to_string(i);
            milvus_sdk::TimeRecorder rc(title);
            stat = connection->Insert(collection_param.collection_name, "", entity_array, entity_ids);
            std::cout << "Insert function call status: " << stat.message() << std::endl;
            std::cout << "Returned id array count: " << entity_ids.size() << std::endl;
        }
    }

    {  // flush buffer
        std::vector<std::string> collections = {collection_param.collection_name};
        stat = connection->Flush(collections);
        std::cout << "Flush function call status: " << stat.message() << std::endl;
    }

    {  // search vectors
        std::vector<std::string> partition_tags;
        milvus::TopKQueryResult topk_query_result;
        milvus_sdk::Utils::DoSearch(connection,
                                    collection_param.collection_name,
                                    partition_tags,
                                    TOP_K,
                                    NPROBE,
                                    search_entity_array,
                                    topk_query_result);
    }

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
        milvus_sdk::Utils::DoSearch(connection,
                                    collection_param.collection_name,
                                    partition_tags,
                                    TOP_K,
                                    NPROBE,
                                    search_entity_array,
                                    topk_query_result);
    }

    {  // drop collection
        stat = connection->DropCollection(collection_param.collection_name);
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
        milvus::CollectionParam collection_param = {
            "collection_1",
            512, // dimension
            256, // index file size
            milvus::MetricType::TANIMOTO
        };

        JSON json_params = {{"nlist", 1024}};
        milvus::IndexParam index_param = {
            collection_param.collection_name,
            milvus::IndexType::IVFFLAT,
            json_params.dump()
        };

        TestProcess(connection, collection_param, index_param);
    }

    {
        milvus::CollectionParam collection_param = {
            "collection_2",
            512, // dimension
            512, // index file size
            milvus::MetricType::SUBSTRUCTURE
        };

        JSON json_params = {};
        milvus::IndexParam index_param = {
            collection_param.collection_name,
            milvus::IndexType::FLAT,
            json_params.dump()
        };

        TestProcess(connection, collection_param, index_param);
    }

    {
        milvus::CollectionParam collection_param = {
            "collection_3",
            128, // dimension
            1024, // index file size
            milvus::MetricType::SUPERSTRUCTURE
        };

        JSON json_params = {};
        milvus::IndexParam index_param = {
            collection_param.collection_name,
            milvus::IndexType::FLAT,
            json_params.dump()
        };

        TestProcess(connection, collection_param, index_param);
    }

    milvus::Connection::Destroy(connection);
}
