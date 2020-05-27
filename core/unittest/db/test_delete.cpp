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
    static std::string collection_name = std::to_string(micros);
    return collection_name;
}

milvus::engine::meta::CollectionSchema
BuildCollectionSchema() {
    milvus::engine::meta::CollectionSchema collection_info;
    collection_info.dimension_ = COLLECTION_DIM;
    collection_info.collection_id_ = GetCollectionName();
    collection_info.metric_type_ = (int32_t)milvus::engine::MetricType::L2;
    collection_info.engine_type_ = (int)milvus::engine::EngineType::FAISS_IDMAP;
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
}  // namespace

TEST_F(DeleteTest, delete_in_mem) {
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
    std::map<int64_t, milvus::engine::VectorsData> search_vectors;
    for (int64_t i = 0; i < num_query; ++i) {
        int64_t index = dis(gen);
        milvus::engine::VectorsData search;
        search.vector_count_ = 1;
        for (int64_t j = 0; j < COLLECTION_DIM; j++) {
            search.float_data_.push_back(xb.float_data_[index * COLLECTION_DIM + j]);
        }
        search_vectors.insert(std::make_pair(xb.id_array_[index], search));
    }

    milvus::engine::IDNumbers ids_to_delete;
    for (auto& kv : search_vectors) {
        ids_to_delete.emplace_back(kv.first);
    }

    stat = db_->DeleteVectors(collection_info.collection_id_, ids_to_delete);
    ASSERT_TRUE(stat.ok());

    //    std::this_thread::sleep_for(std::chrono::seconds(3));  // ensure raw data write to disk
    stat = db_->Flush();
    ASSERT_TRUE(stat.ok());

    uint64_t row_count;
    stat = db_->GetCollectionRowCount(collection_info.collection_id_, row_count);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(row_count, nb - search_vectors.size());

    int topk = 10, nprobe = 10;
    for (auto& pair : search_vectors) {
        auto& search = pair.second;

        std::vector<std::string> tags;
        milvus::engine::ResultIds result_ids;
        milvus::engine::ResultDistances result_distances;
        stat = db_->Query(dummy_context_, collection_info.collection_id_, tags, topk,
                          {{"nprobe", nprobe}}, search, result_ids, result_distances);
        ASSERT_NE(result_ids[0], pair.first);
        //        ASSERT_LT(result_distances[0], 1e-4);
        ASSERT_GT(result_distances[0], 1);
    }
}

TEST_F(DeleteTest, delete_on_disk) {
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
    std::map<int64_t, milvus::engine::VectorsData> search_vectors;
    for (int64_t i = 0; i < num_query; ++i) {
        int64_t index = dis(gen);
        milvus::engine::VectorsData search;
        search.vector_count_ = 1;
        for (int64_t j = 0; j < COLLECTION_DIM; j++) {
            search.float_data_.push_back(xb.float_data_[index * COLLECTION_DIM + j]);
        }
        search_vectors.insert(std::make_pair(xb.id_array_[index], search));
    }

    //    std::this_thread::sleep_for(std::chrono::seconds(3));  // ensure raw data write to disk
    stat = db_->Flush();
    ASSERT_TRUE(stat.ok());

    for (auto& kv : search_vectors) {
        stat = db_->DeleteVector(collection_info.collection_id_, kv.first);
        ASSERT_TRUE(stat.ok());
    }

    stat = db_->Flush();
    ASSERT_TRUE(stat.ok());

    uint64_t row_count;
    stat = db_->GetCollectionRowCount(collection_info.collection_id_, row_count);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(row_count, nb - search_vectors.size());

    int topk = 10, nprobe = 10;
    for (auto& pair : search_vectors) {
        auto& search = pair.second;

        std::vector<std::string> tags;
        milvus::engine::ResultIds result_ids;
        milvus::engine::ResultDistances result_distances;
        stat = db_->Query(dummy_context_,
                          collection_info.collection_id_,
                          tags,
                          topk,
                          {{"nprobe", nprobe}},
                          search,
                          result_ids,
                          result_distances);
        ASSERT_NE(result_ids[0], pair.first);
        //        ASSERT_LT(result_distances[0], 1e-4);
        ASSERT_GT(result_distances[0], 1);
    }
}

TEST_F(DeleteTest, delete_multiple_times) {
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
    std::map<int64_t, milvus::engine::VectorsData> search_vectors;
    for (int64_t i = 0; i < num_query; ++i) {
        int64_t index = dis(gen);
        milvus::engine::VectorsData search;
        search.vector_count_ = 1;
        for (int64_t j = 0; j < COLLECTION_DIM; j++) {
            search.float_data_.push_back(xb.float_data_[index * COLLECTION_DIM + j]);
        }
        search_vectors.insert(std::make_pair(xb.id_array_[index], search));
    }

    //    std::this_thread::sleep_for(std::chrono::seconds(3));  // ensure raw data write to disk
    stat = db_->Flush();
    ASSERT_TRUE(stat.ok());

    int topk = 10, nprobe = 10;
    for (auto& pair : search_vectors) {
        std::vector<int64_t> to_delete{pair.first};
        stat = db_->DeleteVectors(collection_info.collection_id_, to_delete);
        ASSERT_TRUE(stat.ok());

        stat = db_->Flush();
        ASSERT_TRUE(stat.ok());

        auto& search = pair.second;

        std::vector<std::string> tags;
        milvus::engine::ResultIds result_ids;
        milvus::engine::ResultDistances result_distances;
        stat = db_->Query(dummy_context_,
                          collection_info.collection_id_,
                          tags,
                          topk,
                          {{"nprobe", nprobe}},
                          search,
                          result_ids,
                          result_distances);
        ASSERT_NE(result_ids[0], pair.first);
        //        ASSERT_LT(result_distances[0], 1e-4);
        ASSERT_GT(result_distances[0], 1);
    }
}

TEST_F(DeleteTest, delete_before_create_index) {
    milvus::engine::meta::CollectionSchema collection_info = BuildCollectionSchema();
    collection_info.engine_type_ = (int32_t)milvus::engine::EngineType::FAISS_IVFFLAT;
    auto stat = db_->CreateCollection(collection_info);

    milvus::engine::meta::CollectionSchema collection_info_get;
    collection_info_get.collection_id_ = collection_info.collection_id_;
    stat = db_->DescribeCollection(collection_info_get);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(collection_info_get.dimension_, COLLECTION_DIM);

    int64_t nb = 5000;
    milvus::engine::VectorsData xb;
    BuildVectors(nb, xb);

    for (int64_t i = 0; i < nb; i++) {
        xb.id_array_.push_back(i);
    }

    stat = db_->InsertVectors(collection_info.collection_id_, "", xb);
    ASSERT_TRUE(stat.ok());

    stat = db_->Flush();
    ASSERT_TRUE(stat.ok());

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int64_t> dis(0, nb - 1);

    int64_t num_query = 10;
    std::map<int64_t, milvus::engine::VectorsData> search_vectors;
    for (int64_t i = 0; i < num_query; ++i) {
        int64_t index = dis(gen);
        milvus::engine::VectorsData search;
        search.vector_count_ = 1;
        for (int64_t j = 0; j < COLLECTION_DIM; j++) {
            search.float_data_.push_back(xb.float_data_[index * COLLECTION_DIM + j]);
        }
        search_vectors.insert(std::make_pair(xb.id_array_[index], search));
    }

    milvus::engine::IDNumbers ids_to_delete;
    for (auto& kv : search_vectors) {
        ids_to_delete.emplace_back(kv.first);
    }
    stat = db_->DeleteVectors(collection_info.collection_id_, ids_to_delete);

    stat = db_->Flush();
    ASSERT_TRUE(stat.ok());

    milvus::engine::CollectionIndex index;
    index.engine_type_ = (int)milvus::engine::EngineType::FAISS_IVFSQ8;
    index.extra_params_ = {{"nlist", 100}};
    stat = db_->CreateIndex(dummy_context_, collection_info.collection_id_, index);
    ASSERT_TRUE(stat.ok());

    uint64_t row_count;
    stat = db_->GetCollectionRowCount(collection_info.collection_id_, row_count);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(row_count, nb - ids_to_delete.size());

    int topk = 10, nprobe = 10;
    for (auto& pair : search_vectors) {
        auto& search = pair.second;

        std::vector<std::string> tags;
        milvus::engine::ResultIds result_ids;
        milvus::engine::ResultDistances result_distances;
        stat = db_->Query(dummy_context_,
                          collection_info.collection_id_,
                          tags,
                          topk,
                          {{"nprobe", nprobe}},
                          search,
                          result_ids,
                          result_distances);
        ASSERT_NE(result_ids[0], pair.first);
        //        ASSERT_LT(result_distances[0], 1e-4);
        ASSERT_GT(result_distances[0], 1);
    }
}

TEST_F(DeleteTest, delete_with_index) {
    milvus::engine::meta::CollectionSchema collection_info = BuildCollectionSchema();
    collection_info.engine_type_ = (int32_t)milvus::engine::EngineType::FAISS_IVFFLAT;
    auto stat = db_->CreateCollection(collection_info);

    milvus::engine::meta::CollectionSchema collection_info_get;
    collection_info_get.collection_id_ = collection_info.collection_id_;
    stat = db_->DescribeCollection(collection_info_get);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(collection_info_get.dimension_, COLLECTION_DIM);

    int64_t nb = 5000;
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
    std::map<int64_t, milvus::engine::VectorsData> search_vectors;
    for (int64_t i = 0; i < num_query; ++i) {
        int64_t index = dis(gen);
        milvus::engine::VectorsData search;
        search.vector_count_ = 1;
        for (int64_t j = 0; j < COLLECTION_DIM; j++) {
            search.float_data_.push_back(xb.float_data_[index * COLLECTION_DIM + j]);
        }
        search_vectors.insert(std::make_pair(xb.id_array_[index], search));
    }

    milvus::engine::CollectionIndex index;
    index.engine_type_ = (int)milvus::engine::EngineType::FAISS_IVFSQ8;
    index.extra_params_ = {{"nlist", 100}};
    stat = db_->CreateIndex(dummy_context_, collection_info.collection_id_, index);
    ASSERT_TRUE(stat.ok());

    //    std::this_thread::sleep_for(std::chrono::seconds(3));  // ensure raw data write to disk
    stat = db_->Flush();
    ASSERT_TRUE(stat.ok());

    milvus::engine::IDNumbers ids_to_delete;
    for (auto& kv : search_vectors) {
        ids_to_delete.emplace_back(kv.first);
    }
    stat = db_->DeleteVectors(collection_info.collection_id_, ids_to_delete);

    stat = db_->Flush();
    ASSERT_TRUE(stat.ok());

    uint64_t row_count;
    stat = db_->GetCollectionRowCount(collection_info.collection_id_, row_count);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(row_count, nb - ids_to_delete.size());

    int topk = 10, nprobe = 10;
    for (auto& pair : search_vectors) {
        auto& search = pair.second;

        std::vector<std::string> tags;
        milvus::engine::ResultIds result_ids;
        milvus::engine::ResultDistances result_distances;
        stat = db_->Query(dummy_context_,
                          collection_info.collection_id_,
                          tags,
                          topk,
                          {{"nprobe", nprobe}},
                          search,
                          result_ids,
                          result_distances);
        ASSERT_NE(result_ids[0], pair.first);
        //        ASSERT_LT(result_distances[0], 1e-4);
        ASSERT_GT(result_distances[0], 1);
    }
}

TEST_F(DeleteTest, delete_multiple_times_with_index) {
    milvus::engine::meta::CollectionSchema collection_info = BuildCollectionSchema();
    auto stat = db_->CreateCollection(collection_info);

    milvus::engine::meta::CollectionSchema collection_info_get;
    collection_info_get.collection_id_ = collection_info.collection_id_;
    stat = db_->DescribeCollection(collection_info_get);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(collection_info_get.dimension_, COLLECTION_DIM);

    int64_t nb = 5000;
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
    std::map<int64_t, milvus::engine::VectorsData> search_vectors;
    for (int64_t i = 0; i < num_query; ++i) {
        int64_t index = dis(gen);
        milvus::engine::VectorsData search;
        search.vector_count_ = 1;
        for (int64_t j = 0; j < COLLECTION_DIM; j++) {
            search.float_data_.push_back(xb.float_data_[index * COLLECTION_DIM + j]);
        }
        search_vectors.insert(std::make_pair(xb.id_array_[index], search));
    }

    //    std::this_thread::sleep_for(std::chrono::seconds(3));  // ensure raw data write to disk
    stat = db_->Flush();
    ASSERT_TRUE(stat.ok());

    milvus::engine::CollectionIndex index;
    index.engine_type_ = (int)milvus::engine::EngineType::FAISS_IVFFLAT;
    index.extra_params_ = {{"nlist", 1}};
    stat = db_->CreateIndex(dummy_context_, collection_info.collection_id_, index);
    ASSERT_TRUE(stat.ok());

    int topk = 10, nprobe = 10;
    int deleted = 0;
    for (auto& pair : search_vectors) {
        std::vector<int64_t> to_delete{pair.first};
        stat = db_->DeleteVectors(collection_info.collection_id_, to_delete);
        ASSERT_TRUE(stat.ok());

        stat = db_->Flush();
        ASSERT_TRUE(stat.ok());

        ++deleted;

        uint64_t row_count;
        stat = db_->GetCollectionRowCount(collection_info.collection_id_, row_count);
        ASSERT_TRUE(stat.ok());
        ASSERT_EQ(row_count, nb - deleted);

        auto& search = pair.second;

        std::vector<std::string> tags;
        milvus::engine::ResultIds result_ids;
        milvus::engine::ResultDistances result_distances;
        stat = db_->Query(dummy_context_,
                          collection_info.collection_id_,
                          tags,
                          topk,
                          {{"nprobe", nprobe}},
                          search,
                          result_ids,
                          result_distances);
        ASSERT_TRUE(stat.ok());
        ASSERT_NE(result_ids[0], pair.first);
        //        ASSERT_LT(result_distances[0], 1e-4);
        ASSERT_GT(result_distances[0], 1);
    }
}

TEST_F(DeleteTest, delete_single_vector) {
    milvus::engine::meta::CollectionSchema collection_info = BuildCollectionSchema();
    auto stat = db_->CreateCollection(collection_info);

    milvus::engine::meta::CollectionSchema collection_info_get;
    collection_info_get.collection_id_ = collection_info.collection_id_;
    stat = db_->DescribeCollection(collection_info_get);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(collection_info_get.dimension_, COLLECTION_DIM);

    int64_t nb = 1;
    milvus::engine::VectorsData xb;
    BuildVectors(nb, xb);

    stat = db_->InsertVectors(collection_info.collection_id_, "", xb);
    ASSERT_TRUE(stat.ok());

    //    std::this_thread::sleep_for(std::chrono::seconds(3));  // ensure raw data write to disk
    stat = db_->Flush();
    ASSERT_TRUE(stat.ok());

    stat = db_->DeleteVectors(collection_info.collection_id_, xb.id_array_);
    ASSERT_TRUE(stat.ok());

    stat = db_->Flush();
    ASSERT_TRUE(stat.ok());

    uint64_t row_count;
    stat = db_->GetCollectionRowCount(collection_info.collection_id_, row_count);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(row_count, 0);

    const int topk = 1, nprobe = 1;
    milvus::json json_params = {{"nprobe", nprobe}};

    std::vector<std::string> tags;
    milvus::engine::ResultIds result_ids;
    milvus::engine::ResultDistances result_distances;
    stat = db_->Query(dummy_context_,
                      collection_info.collection_id_, tags, topk, json_params, xb, result_ids, result_distances);
    ASSERT_TRUE(result_ids.empty());
    ASSERT_TRUE(result_distances.empty());
    // ASSERT_EQ(result_ids[0], -1);
    //        ASSERT_LT(result_distances[0], 1e-4);
    // ASSERT_EQ(result_distances[0], std::numeric_limits<float>::max());
}

TEST_F(DeleteTest, delete_add_create_index) {
    milvus::engine::meta::CollectionSchema collection_info = BuildCollectionSchema();
    auto stat = db_->CreateCollection(collection_info);

    milvus::engine::meta::CollectionSchema collection_info_get;
    collection_info_get.collection_id_ = collection_info.collection_id_;
    stat = db_->DescribeCollection(collection_info_get);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(collection_info_get.dimension_, COLLECTION_DIM);

    int64_t nb = 3000;
    milvus::engine::VectorsData xb;
    BuildVectors(nb, xb);

    stat = db_->InsertVectors(collection_info.collection_id_, "", xb);
    ASSERT_TRUE(stat.ok());

    // stat = db_->Flush();
    // ASSERT_TRUE(stat.ok());
    milvus::engine::CollectionIndex index;
    index.engine_type_ = (int)milvus::engine::EngineType::FAISS_IVFFLAT;
    index.extra_params_ = {{"nlist", 100}};
    stat = db_->CreateIndex(dummy_context_, collection_info.collection_id_, index);
    ASSERT_TRUE(stat.ok());

    std::vector<milvus::engine::IDNumber> ids_to_delete;
    ids_to_delete.emplace_back(xb.id_array_.front());
    stat = db_->DeleteVectors(collection_info.collection_id_, ids_to_delete);
    ASSERT_TRUE(stat.ok());

    milvus::engine::VectorsData xb2 = xb;
    xb2.id_array_.clear();  // same vector, different id

    stat = db_->InsertVectors(collection_info.collection_id_, "", xb2);
    ASSERT_TRUE(stat.ok());

    // stat = db_->Flush();
    // ASSERT_TRUE(stat.ok());
    stat = db_->CreateIndex(dummy_context_, collection_info.collection_id_, index);
    ASSERT_TRUE(stat.ok());

    uint64_t row_count;
    stat = db_->GetCollectionRowCount(collection_info.collection_id_, row_count);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(row_count, nb * 2 - 1);

    const int topk = 10, nprobe = 10;
    milvus::json json_params = {{"nprobe", nprobe}};

    std::vector<std::string> tags;
    milvus::engine::ResultIds result_ids;
    milvus::engine::ResultDistances result_distances;
    milvus::engine::VectorsData qb = xb;
    qb.float_data_.resize(COLLECTION_DIM);
    qb.vector_count_ = 1;
    qb.id_array_.clear();
    stat = db_->Query(dummy_context_,
                      collection_info.collection_id_, tags, topk, json_params, qb, result_ids, result_distances);

    ASSERT_EQ(result_ids[0], xb2.id_array_.front());
    ASSERT_LT(result_distances[0], 1e-4);

    result_ids.clear();
    result_distances.clear();
    stat = db_->QueryByIDs(dummy_context_, collection_info.collection_id_, tags, topk,
                           json_params, ids_to_delete, result_ids, result_distances);
    ASSERT_EQ(result_ids[0], -1);
    ASSERT_EQ(result_distances[0], std::numeric_limits<float>::max());
}

TEST_F(DeleteTest, delete_add_auto_flush) {
    milvus::engine::meta::CollectionSchema collection_info = BuildCollectionSchema();
    auto stat = db_->CreateCollection(collection_info);

    milvus::engine::meta::CollectionSchema collection_info_get;
    collection_info_get.collection_id_ = collection_info.collection_id_;
    stat = db_->DescribeCollection(collection_info_get);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(collection_info_get.dimension_, COLLECTION_DIM);

    int64_t nb = 3000;
    milvus::engine::VectorsData xb;
    BuildVectors(nb, xb);

    stat = db_->InsertVectors(collection_info.collection_id_, "", xb);
    ASSERT_TRUE(stat.ok());

    std::this_thread::sleep_for(std::chrono::seconds(2));

    // stat = db_->Flush();
    // ASSERT_TRUE(stat.ok());
    // milvus::engine::CollectionIndex index;
    // index.engine_type_ = (int)milvus::engine::EngineType::FAISS_IVFSQ8;
    // stat = db_->CreateIndex(dummy_context_, collection_info.collection_id_, index);
    // ASSERT_TRUE(stat.ok());

    std::vector<milvus::engine::IDNumber> ids_to_delete;
    ids_to_delete.emplace_back(xb.id_array_.front());
    stat = db_->DeleteVectors(collection_info.collection_id_, ids_to_delete);
    ASSERT_TRUE(stat.ok());

    milvus::engine::VectorsData xb2 = xb;
    xb2.id_array_.clear();  // same vector, different id

    stat = db_->InsertVectors(collection_info.collection_id_, "", xb2);
    ASSERT_TRUE(stat.ok());

    std::this_thread::sleep_for(std::chrono::seconds(2));
    // stat = db_->Flush();
    // ASSERT_TRUE(stat.ok());
    // stat = db_->CreateIndex(dummy_context_, collection_info.collection_id_, index);
    // ASSERT_TRUE(stat.ok());

    uint64_t row_count;
    stat = db_->GetCollectionRowCount(collection_info.collection_id_, row_count);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(row_count, nb * 2 - 1);

    const int topk = 10, nprobe = 10;
    milvus::json json_params = {{"nprobe", nprobe}};

    std::vector<std::string> tags;
    milvus::engine::ResultIds result_ids;
    milvus::engine::ResultDistances result_distances;
    milvus::engine::VectorsData qb = xb;
    qb.float_data_.resize(COLLECTION_DIM);
    qb.vector_count_ = 1;
    qb.id_array_.clear();
    stat = db_->Query(dummy_context_,
                      collection_info.collection_id_, tags, topk, json_params, qb, result_ids, result_distances);

    ASSERT_EQ(result_ids[0], xb2.id_array_.front());
    ASSERT_LT(result_distances[0], 1e-4);

    result_ids.clear();
    result_distances.clear();
    stat = db_->QueryByIDs(dummy_context_,
                           collection_info.collection_id_, tags, topk, {{"nprobe", nprobe}},
                           ids_to_delete, result_ids, result_distances);
    ASSERT_EQ(result_ids[0], -1);
    ASSERT_EQ(result_distances[0], std::numeric_limits<float>::max());
}

TEST_F(CompactTest, compact_basic) {
    milvus::engine::meta::CollectionSchema collection_info = BuildCollectionSchema();
    auto stat = db_->CreateCollection(collection_info);

    milvus::engine::meta::CollectionSchema collection_info_get;
    collection_info_get.collection_id_ = collection_info.collection_id_;
    stat = db_->DescribeCollection(collection_info_get);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(collection_info_get.dimension_, COLLECTION_DIM);

    int64_t nb = 100;
    milvus::engine::VectorsData xb;
    BuildVectors(nb, xb);

    stat = db_->InsertVectors(collection_info.collection_id_, "", xb);
    ASSERT_TRUE(stat.ok());

    stat = db_->Flush();
    ASSERT_TRUE(stat.ok());

    std::vector<milvus::engine::IDNumber> ids_to_delete;
    ids_to_delete.emplace_back(xb.id_array_.front());
    ids_to_delete.emplace_back(xb.id_array_.back());
    stat = db_->DeleteVectors(collection_info.collection_id_, ids_to_delete);
    ASSERT_TRUE(stat.ok());

    stat = db_->Flush();
    ASSERT_TRUE(stat.ok());

    uint64_t row_count;
    stat = db_->GetCollectionRowCount(collection_info.collection_id_, row_count);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(row_count, nb - 2);

    stat = db_->Compact(dummy_context_, collection_info.collection_id_);
    ASSERT_TRUE(stat.ok());

    const int topk = 1, nprobe = 1;
    milvus::json json_params = {{"nprobe", nprobe}};

    std::vector<std::string> tags;
    milvus::engine::ResultIds result_ids;
    milvus::engine::ResultDistances result_distances;
    milvus::engine::VectorsData qb = xb;

    stat = db_->QueryByIDs(dummy_context_,
                           collection_info.collection_id_,
                           tags,
                           topk,
                           json_params,
                           ids_to_delete,
                           result_ids,
                           result_distances);
    ASSERT_EQ(result_ids[0], -1);
    ASSERT_EQ(result_distances[0], std::numeric_limits<float>::max());
}

TEST_F(CompactTest, compact_with_index) {
    milvus::engine::meta::CollectionSchema collection_info = BuildCollectionSchema();
    collection_info.index_file_size_ = milvus::engine::KB;
    collection_info.engine_type_ = (int32_t)milvus::engine::EngineType::FAISS_IVFSQ8;
    auto stat = db_->CreateCollection(collection_info);

    milvus::engine::meta::CollectionSchema collection_info_get;
    collection_info_get.collection_id_ = collection_info.collection_id_;
    stat = db_->DescribeCollection(collection_info_get);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(collection_info_get.dimension_, COLLECTION_DIM);

    int64_t nb = 3000;
    milvus::engine::VectorsData xb;
    BuildVectors(nb, xb);

    xb.id_array_.clear();
    for (int64_t i = 0; i < nb; i++) {
        xb.id_array_.emplace_back(i);
    }

    stat = db_->InsertVectors(collection_info.collection_id_, "", xb);
    ASSERT_TRUE(stat.ok());

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int64_t> dis(0, nb - 1);

    int64_t num_query = 10;
    std::map<int64_t, milvus::engine::VectorsData> search_vectors;
    for (int64_t i = 0; i < num_query; ++i) {
        int64_t index = dis(gen);
        milvus::engine::VectorsData search;
        search.vector_count_ = 1;
        for (int64_t j = 0; j < COLLECTION_DIM; j++) {
            search.float_data_.push_back(xb.float_data_[index * COLLECTION_DIM + j]);
        }
        search_vectors.insert(std::make_pair(xb.id_array_[index], search));
    }

    milvus::engine::CollectionIndex index;
    index.engine_type_ = (int)milvus::engine::EngineType::FAISS_IVFSQ8;
    stat = db_->CreateIndex(dummy_context_, collection_info.collection_id_, index);
    ASSERT_TRUE(stat.ok());

    stat = db_->Flush();
    ASSERT_TRUE(stat.ok());

    milvus::engine::IDNumbers ids_to_delete;
    for (auto& kv : search_vectors) {
        ids_to_delete.emplace_back(kv.first);
    }
    stat = db_->DeleteVectors(collection_info.collection_id_, ids_to_delete);

    stat = db_->Flush();
    ASSERT_TRUE(stat.ok());

    uint64_t row_count;
    stat = db_->GetCollectionRowCount(collection_info.collection_id_, row_count);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(row_count, nb - ids_to_delete.size());

    stat = db_->Compact(dummy_context_, collection_info.collection_id_);
    ASSERT_TRUE(stat.ok());

    stat = db_->GetCollectionRowCount(collection_info.collection_id_, row_count);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(row_count, nb - ids_to_delete.size());

    milvus::engine::CollectionIndex table_index;
    stat = db_->DescribeIndex(collection_info.collection_id_, table_index);
    ASSERT_TRUE(stat.ok());
    ASSERT_FLOAT_EQ(table_index.engine_type_, index.engine_type_);

    const int topk = 10, nprobe = 10;
    milvus::json json_params = {{"nprobe", nprobe}};

    for (auto& pair : search_vectors) {
        auto& search = pair.second;

        std::vector<std::string> tags;
        milvus::engine::ResultIds result_ids;
        milvus::engine::ResultDistances result_distances;
        stat = db_->Query(dummy_context_, collection_info.collection_id_, tags, topk, json_params, search, result_ids,
                          result_distances);
        ASSERT_NE(result_ids[0], pair.first);
        //        ASSERT_LT(result_distances[0], 1e-4);
        ASSERT_GT(result_distances[0], 1);
    }
}

TEST_F(CompactTest, compact_non_existing_table) {
    auto status = db_->Compact(dummy_context_, "non_existing_table");
    ASSERT_FALSE(status.ok());
}
