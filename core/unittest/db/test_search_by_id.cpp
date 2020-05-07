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

#include <boost/filesystem.hpp>
#include <chrono>
#include <cmath>
#include <fstream>
#include <iostream>
#include <limits>
#include <random>
#include <thread>

#include "db/Constants.h"
#include "db/Utils.h"
#include "db/engine/EngineFactory.h"
#include "db/insert/MemTable.h"
#include "db/insert/MemTableFile.h"
#include "db/insert/VectorSource.h"
#include "db/meta/MetaConsts.h"
#include "db/utils.h"
#include "gtest/gtest.h"
#include "metrics/Metrics.h"

namespace {

static constexpr int64_t COLLECTION_DIM = 256;

std::string
GetCollectionName() {
    auto now = std::chrono::system_clock::now();
    auto micros = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
    std::string collection_name = std::to_string(micros);
    return collection_name;
}

milvus::engine::meta::CollectionSchema
BuildCollectionSchema() {
    milvus::engine::meta::CollectionSchema collection_info;
    collection_info.dimension_ = COLLECTION_DIM;
    collection_info.collection_id_ = GetCollectionName();
    collection_info.metric_type_ = (int32_t)milvus::engine::MetricType::L2;
    collection_info.engine_type_ = (int)milvus::engine::EngineType::FAISS_IVFFLAT;
    return collection_info;
}

void
BuildVectors(uint64_t n, milvus::engine::VectorsData& vectors) {
    vectors.vector_count_ = n;
    vectors.float_data_.clear();
    vectors.float_data_.resize(n * COLLECTION_DIM);
    float* data = vectors.float_data_.data();
    for (int i = 0; i < n; i++) {
        for (int j = 0; j < COLLECTION_DIM; j++) data[COLLECTION_DIM * i + j] = drand48();
    }
}

void
CheckQueryResult(const std::vector<int64_t>& target_ids, int64_t topk, milvus::engine::ResultIds result_ids,
                 milvus::engine::ResultDistances result_distances) {
    ASSERT_EQ(result_ids.size(), target_ids.size() * topk);
    ASSERT_EQ(result_distances.size(), target_ids.size() * topk);

    for (size_t i = 0; i < target_ids.size(); i++) {
        ASSERT_EQ(result_ids[topk * i], target_ids[i]);
        ASSERT_LT(result_distances[topk * i], 1e-3);
    }
}

}  // namespace

TEST_F(SearchByIdTest, BASIC_TEST) {
    milvus::engine::meta::CollectionSchema collection_info = BuildCollectionSchema();
    auto stat = db_->CreateCollection(collection_info);

    milvus::engine::meta::CollectionSchema collection_info_get;
    collection_info_get.collection_id_ = collection_info.collection_id_;
    stat = db_->DescribeCollection(collection_info_get);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(collection_info_get.dimension_, COLLECTION_DIM);

    int64_t nb = 100000;
    milvus::engine::VectorsData xb;
    BuildVectors(nb, xb);

    for (int64_t i = 0; i < nb; i++) {
        xb.id_array_.push_back(i);
    }

    stat = db_->InsertVectors(collection_info.collection_id_, "", xb);
    ASSERT_TRUE(stat.ok());

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int64_t> dis(0, nb - 1);

    int64_t num_query = 10;
    std::vector<int64_t> ids_to_search;
    for (int64_t i = 0; i < num_query; ++i) {
        int64_t index = dis(gen);
        ids_to_search.emplace_back(index);
    }

    stat = db_->Flush();
    ASSERT_TRUE(stat.ok());

    const int64_t topk = 10, nprobe = 10;
    milvus::json json_params = {{"nprobe", nprobe}};

    std::vector<std::string> tags;
    milvus::engine::ResultIds result_ids;
    milvus::engine::ResultDistances result_distances;

    stat = db_->QueryByIDs(dummy_context_,
                           collection_info.collection_id_,
                           tags,
                           topk,
                           json_params,
                           ids_to_search,
                           result_ids,
                           result_distances);

    CheckQueryResult(ids_to_search, topk, result_ids, result_distances);

    // invalid id search
    ids_to_search.clear();
    for (int64_t i = 0; i < num_query; ++i) {
        int64_t index = (i % 2 == 0) ? -1 : dis(gen);
        ids_to_search.emplace_back(index);
    }
    stat = db_->QueryByIDs(dummy_context_,
                           collection_info.collection_id_,
                           tags,
                           topk,
                           json_params,
                           ids_to_search,
                           result_ids,
                           result_distances);
    ASSERT_EQ(result_ids.size(), ids_to_search.size() * topk);
    ASSERT_EQ(result_distances.size(), ids_to_search.size() * topk);

    for (size_t i = 0; i < ids_to_search.size(); i++) {
        if (i % 2 == 0) {
            ASSERT_EQ(result_ids[topk * i], -1);
            ASSERT_FLOAT_EQ(result_distances[topk * i], std::numeric_limits<float>::max());
        } else {
            ASSERT_EQ(result_ids[topk * i], ids_to_search[i]);
            ASSERT_LT(result_distances[topk * i], 1e-3);
        }
    }

    // duplicate id search
    ids_to_search.clear();
    ids_to_search.push_back(1);
    ids_to_search.push_back(1);

    stat = db_->QueryByIDs(dummy_context_,
                           collection_info.collection_id_,
                           tags,
                           topk,
                           json_params,
                           ids_to_search,
                           result_ids,
                           result_distances);
    ASSERT_EQ(result_ids.size(), ids_to_search.size() * topk);
    ASSERT_EQ(result_distances.size(), ids_to_search.size() * topk);

    CheckQueryResult(ids_to_search, topk, result_ids, result_distances);
}

TEST_F(SearchByIdTest, WITH_INDEX_TEST) {
    milvus::engine::meta::CollectionSchema collection_info = BuildCollectionSchema();
    auto stat = db_->CreateCollection(collection_info);

    milvus::engine::meta::CollectionSchema collection_info_get;
    collection_info_get.collection_id_ = collection_info.collection_id_;
    stat = db_->DescribeCollection(collection_info_get);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(collection_info_get.dimension_, COLLECTION_DIM);

    int64_t nb = 10000;
    milvus::engine::VectorsData xb;
    BuildVectors(nb, xb);

    for (int64_t i = 0; i < nb; i++) {
        xb.id_array_.push_back(i);
    }

    stat = db_->InsertVectors(collection_info.collection_id_, "", xb);
    ASSERT_TRUE(stat.ok());

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int64_t> dis(0, nb - 1);

    int64_t num_query = 10;
    std::vector<int64_t> ids_to_search;
    for (int64_t i = 0; i < num_query; ++i) {
        int64_t index = dis(gen);
        ids_to_search.emplace_back(index);
    }

    //    std::this_thread::sleep_for(std::chrono::seconds(3));  // ensure raw data write to disk
    stat = db_->Flush();
    ASSERT_TRUE(stat.ok());

    milvus::engine::CollectionIndex index;
    index.engine_type_ = (int)milvus::engine::EngineType::FAISS_IVFSQ8;
    index.extra_params_ = {{"nlist", 10}};
    stat = db_->CreateIndex(dummy_context_, collection_info.collection_id_, index);
    ASSERT_TRUE(stat.ok());

    const int64_t topk = 10, nprobe = 10;
    milvus::json json_params = {{"nprobe", nprobe}};

    std::vector<std::string> tags;
    milvus::engine::ResultIds result_ids;
    milvus::engine::ResultDistances result_distances;

    stat = db_->QueryByIDs(dummy_context_,
                           collection_info.collection_id_,
                           tags,
                           topk,
                           json_params,
                           ids_to_search,
                           result_ids,
                           result_distances);

    CheckQueryResult(ids_to_search, topk, result_ids, result_distances);
}

TEST_F(SearchByIdTest, WITH_DELETE_TEST) {
    milvus::engine::meta::CollectionSchema collection_info = BuildCollectionSchema();
    auto stat = db_->CreateCollection(collection_info);

    milvus::engine::meta::CollectionSchema collection_info_get;
    collection_info_get.collection_id_ = collection_info.collection_id_;
    stat = db_->DescribeCollection(collection_info_get);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(collection_info_get.dimension_, COLLECTION_DIM);

    int64_t nb = 100000;
    milvus::engine::VectorsData xb;
    BuildVectors(nb, xb);

    for (int64_t i = 0; i < nb; i++) {
        xb.id_array_.push_back(i);
    }

    stat = db_->InsertVectors(collection_info.collection_id_, "", xb);
    ASSERT_TRUE(stat.ok());

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int64_t> dis(0, nb - 1);

    int64_t num_query = 10;
    std::vector<int64_t> ids_to_search;
    for (int64_t i = 0; i < num_query; ++i) {
        int64_t index = dis(gen);
        ids_to_search.emplace_back(index);
    }

    //    std::this_thread::sleep_for(std::chrono::seconds(3));  // ensure raw data write to disk
    stat = db_->Flush();
    ASSERT_TRUE(stat.ok());

    milvus::engine::IDNumbers ids_to_delete;
    for (auto& id : ids_to_search) {
        ids_to_delete.emplace_back(id);
    }
    stat = db_->DeleteVectors(collection_info.collection_id_, ids_to_delete);

    stat = db_->Flush();
    ASSERT_TRUE(stat.ok());

    const int64_t topk = 10, nprobe = 10;
    milvus::json json_params = {{"nprobe", nprobe}};

    std::vector<std::string> tags;
    milvus::engine::ResultIds result_ids;
    milvus::engine::ResultDistances result_distances;

    stat = db_->QueryByIDs(dummy_context_,
                           collection_info.collection_id_,
                           tags,
                           topk,
                           json_params,
                           ids_to_search,
                           result_ids,
                           result_distances);

    ASSERT_EQ(result_ids.size(), ids_to_search.size() * topk);
    ASSERT_EQ(result_distances.size(), ids_to_search.size() * topk);

    for (size_t i = 0; i < result_ids.size(); i++) {
        ASSERT_EQ(result_ids[i], -1);
    }
}

TEST_F(GetVectorByIdTest, BASIC_TEST) {
    milvus::engine::meta::CollectionSchema collection_info = BuildCollectionSchema();
    auto stat = db_->CreateCollection(collection_info);

    milvus::engine::meta::CollectionSchema collection_info_get;
    collection_info_get.collection_id_ = collection_info.collection_id_;
    stat = db_->DescribeCollection(collection_info_get);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(collection_info_get.dimension_, COLLECTION_DIM);

    int64_t nb = 100000;
    milvus::engine::VectorsData xb;
    BuildVectors(nb, xb);

    for (int64_t i = 0; i < nb; i++) {
        xb.id_array_.push_back(i);
    }

    stat = db_->InsertVectors(collection_info.collection_id_, "", xb);
    ASSERT_TRUE(stat.ok());

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int64_t> dis(0, nb - 1);

    int64_t num_query = 10;
    std::vector<int64_t> ids_to_search;
    for (int64_t i = 0; i < num_query; ++i) {
        int64_t index = dis(gen);
        ids_to_search.emplace_back(index);
    }

    //    std::this_thread::sleep_for(std::chrono::seconds(3));  // ensure raw data write to disk
    stat = db_->Flush();
    ASSERT_TRUE(stat.ok());

    const int64_t topk = 10, nprobe = 10;
    milvus::json json_params = {{"nprobe", nprobe}};

    std::vector<std::string> tags;
    milvus::engine::ResultIds result_ids;
    milvus::engine::ResultDistances result_distances;

    std::vector<milvus::engine::VectorsData> vectors;
    stat = db_->GetVectorsByID(collection_info.collection_id_, ids_to_search, vectors);
    ASSERT_TRUE(stat.ok());

    stat = db_->Query(dummy_context_, collection_info.collection_id_, tags, topk, json_params, vectors[0], result_ids,
                      result_distances);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(result_ids[0], ids_to_search[0]);
    ASSERT_LT(result_distances[0], 1e-4);
}

TEST_F(GetVectorByIdTest, WITH_INDEX_TEST) {
    milvus::engine::meta::CollectionSchema collection_info = BuildCollectionSchema();
    auto stat = db_->CreateCollection(collection_info);

    milvus::engine::meta::CollectionSchema collection_info_get;
    collection_info_get.collection_id_ = collection_info.collection_id_;
    stat = db_->DescribeCollection(collection_info_get);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(collection_info_get.dimension_, COLLECTION_DIM);

    int64_t nb = 10000;
    milvus::engine::VectorsData xb;
    BuildVectors(nb, xb);

    for (int64_t i = 0; i < nb; i++) {
        xb.id_array_.push_back(i);
    }

    stat = db_->InsertVectors(collection_info.collection_id_, "", xb);
    ASSERT_TRUE(stat.ok());

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int64_t> dis(0, nb - 1);

    int64_t num_query = 10;
    std::vector<int64_t> ids_to_search;
    for (int64_t i = 0; i < num_query; ++i) {
        int64_t index = dis(gen);
        ids_to_search.emplace_back(index);
    }

    //    std::this_thread::sleep_for(std::chrono::seconds(3));  // ensure raw data write to disk
    stat = db_->Flush();
    ASSERT_TRUE(stat.ok());

    milvus::engine::CollectionIndex index;
    index.extra_params_ = {{"nlist", 10}};
    index.engine_type_ = (int)milvus::engine::EngineType::FAISS_IVFSQ8;
    stat = db_->CreateIndex(dummy_context_, collection_info.collection_id_, index);
    ASSERT_TRUE(stat.ok());

    const int64_t topk = 10, nprobe = 10;
    milvus::json json_params = {{"nprobe", nprobe}};

    std::vector<std::string> tags;
    milvus::engine::ResultIds result_ids;
    milvus::engine::ResultDistances result_distances;

    std::vector<milvus::engine::VectorsData> vectors;
    stat = db_->GetVectorsByID(collection_info.collection_id_, ids_to_search, vectors);
    ASSERT_TRUE(stat.ok());

    stat = db_->Query(dummy_context_, collection_info.collection_id_, tags, topk, json_params, vectors[0], result_ids,
                      result_distances);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(result_ids[0], ids_to_search[0]);
    ASSERT_LT(result_distances[0], 1e-3);
}

TEST_F(GetVectorByIdTest, WITH_DELETE_TEST) {
    milvus::engine::meta::CollectionSchema collection_info = BuildCollectionSchema();
    auto stat = db_->CreateCollection(collection_info);

    milvus::engine::meta::CollectionSchema collection_info_get;
    collection_info_get.collection_id_ = collection_info.collection_id_;
    stat = db_->DescribeCollection(collection_info_get);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(collection_info_get.dimension_, COLLECTION_DIM);

    int64_t nb = 100000;
    milvus::engine::VectorsData xb;
    BuildVectors(nb, xb);

    for (int64_t i = 0; i < nb; i++) {
        xb.id_array_.push_back(i);
    }

    stat = db_->InsertVectors(collection_info.collection_id_, "", xb);
    ASSERT_TRUE(stat.ok());

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int64_t> dis(0, nb - 1);

    int64_t num_query = 10;
    std::vector<int64_t> ids_to_search;
    for (int64_t i = 0; i < num_query; ++i) {
        int64_t index = dis(gen);
        ids_to_search.emplace_back(index);
    }

    //    std::this_thread::sleep_for(std::chrono::seconds(3));  // ensure raw data write to disk
    stat = db_->Flush();
    ASSERT_TRUE(stat.ok());

    milvus::engine::IDNumbers ids_to_delete;
    for (auto& id : ids_to_search) {
        ids_to_delete.emplace_back(id);
    }
    stat = db_->DeleteVectors(collection_info.collection_id_, ids_to_delete);

    stat = db_->Flush();
    ASSERT_TRUE(stat.ok());

    std::vector<std::string> tags;
    milvus::engine::ResultIds result_ids;
    milvus::engine::ResultDistances result_distances;

    std::vector<milvus::engine::VectorsData> vectors;
    stat = db_->GetVectorsByID(collection_info.collection_id_, ids_to_search, vectors);
    ASSERT_TRUE(stat.ok());
    for (auto& vector : vectors) {
        ASSERT_EQ(vector.vector_count_, 0);
    }
}

TEST_F(SearchByIdTest, BINARY_TEST) {
    milvus::engine::meta::CollectionSchema collection_info = BuildCollectionSchema();
    collection_info.engine_type_ = (int)milvus::engine::EngineType::FAISS_BIN_IDMAP;
    collection_info.metric_type_ = (int32_t)milvus::engine::MetricType::JACCARD;
    auto stat = db_->CreateCollection(collection_info);
    ASSERT_TRUE(stat.ok());

    milvus::engine::meta::CollectionSchema collection_info_get;
    collection_info_get.collection_id_ = collection_info.collection_id_;
    stat = db_->DescribeCollection(collection_info_get);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(collection_info_get.dimension_, COLLECTION_DIM);

    int insert_loop = 100;
    int64_t nb = 1000;

    for (int k = 0; k < insert_loop; ++k) {
        milvus::engine::VectorsData vectors;
        vectors.vector_count_ = nb;
        vectors.binary_data_.clear();
        vectors.binary_data_.resize(nb * COLLECTION_DIM);
        uint8_t* data = vectors.binary_data_.data();
        vectors.id_array_.clear();

        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> distribution{0, std::numeric_limits<uint8_t>::max()};
        for (int i = 0; i < nb; i++) {
            for (int j = 0; j < COLLECTION_DIM; j++) {
                data[COLLECTION_DIM * i + j] = distribution(gen);
            }
            vectors.id_array_.emplace_back(k * nb + i);
        }

        stat = db_->InsertVectors(collection_info.collection_id_, "", vectors);
        ASSERT_TRUE(stat.ok());
    }

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int64_t> dis(0, nb * insert_loop - 1);

    int64_t num_query = 10;
    std::vector<int64_t> ids_to_search;
    for (int64_t i = 0; i < num_query; ++i) {
        int64_t index = dis(gen);
        ids_to_search.emplace_back(index);
    }

    stat = db_->Flush();
    ASSERT_TRUE(stat.ok());

    uint64_t row_count;
    stat = db_->GetCollectionRowCount(collection_info.collection_id_, row_count);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(row_count, nb * insert_loop);

    const int64_t topk = 10, nprobe = 10;
    milvus::json json_params = {{"nprobe", nprobe}};

    std::vector<std::string> tags;
    milvus::engine::ResultIds result_ids;
    milvus::engine::ResultDistances result_distances;

    std::vector<milvus::engine::VectorsData> vectors;
    stat = db_->GetVectorsByID(collection_info.collection_id_, ids_to_search, vectors);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(vectors.size(), ids_to_search.size());

    stat = db_->Query(dummy_context_, collection_info.collection_id_, tags, topk, json_params, vectors[0], result_ids,
                      result_distances);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(result_ids[0], ids_to_search[0]);
    ASSERT_LT(result_distances[0], 1e-4);

    tags.clear();
    result_ids.clear();
    result_distances.clear();

    stat = db_->QueryByIDs(dummy_context_,
                           collection_info.collection_id_,
                           tags,
                           topk,
                           json_params,
                           ids_to_search,
                           result_ids,
                           result_distances);
    ASSERT_TRUE(stat.ok());

    CheckQueryResult(ids_to_search, topk, result_ids, result_distances);

    // invalid id search
    ids_to_search.clear();
    for (int64_t i = 0; i < num_query; ++i) {
        int64_t index = (i % 2 == 0) ? -1 : dis(gen);
        ids_to_search.emplace_back(index);
    }
    stat = db_->QueryByIDs(dummy_context_,
                           collection_info.collection_id_,
                           tags,
                           topk,
                           json_params,
                           ids_to_search,
                           result_ids,
                           result_distances);
    ASSERT_EQ(result_ids.size(), ids_to_search.size() * topk);
    ASSERT_EQ(result_distances.size(), ids_to_search.size() * topk);

    for (size_t i = 0; i < ids_to_search.size(); i++) {
        if (i % 2 == 0) {
            ASSERT_EQ(result_ids[topk * i], -1);
            ASSERT_FLOAT_EQ(result_distances[topk * i], std::numeric_limits<float>::max());
        } else {
            ASSERT_EQ(result_ids[topk * i], ids_to_search[i]);
            ASSERT_LT(result_distances[topk * i], 1e-3);
        }
    }

    // duplicate id search
    ids_to_search.clear();
    ids_to_search.push_back(1);
    ids_to_search.push_back(1);

    stat = db_->QueryByIDs(dummy_context_,
                           collection_info.collection_id_,
                           tags,
                           topk,
                           json_params,
                           ids_to_search,
                           result_ids,
                           result_distances);
    ASSERT_EQ(result_ids.size(), ids_to_search.size() * topk);
    ASSERT_EQ(result_distances.size(), ids_to_search.size() * topk);

    CheckQueryResult(ids_to_search, topk, result_ids, result_distances);
}
