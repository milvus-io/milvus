// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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

static constexpr int64_t TABLE_DIM = 256;

std::string
GetTableName() {
    auto now = std::chrono::system_clock::now();
    auto micros = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
    static std::string table_name = std::to_string(micros);
    return table_name;
}

milvus::engine::meta::TableSchema
BuildTableSchema() {
    milvus::engine::meta::TableSchema table_info;
    table_info.dimension_ = TABLE_DIM;
    table_info.table_id_ = GetTableName();
    table_info.metric_type_ = (int32_t)milvus::engine::MetricType::L2;
    table_info.engine_type_ = (int)milvus::engine::EngineType::FAISS_IVFFLAT;
    return table_info;
}

void
BuildVectors(uint64_t n, milvus::engine::VectorsData& vectors) {
    vectors.vector_count_ = n;
    vectors.float_data_.clear();
    vectors.float_data_.resize(n * TABLE_DIM);
    float* data = vectors.float_data_.data();
    for (int i = 0; i < n; i++) {
        for (int j = 0; j < TABLE_DIM; j++) data[TABLE_DIM * i + j] = drand48();
    }
}
}  // namespace

TEST_F(SearchByIdTest, basic) {
    milvus::engine::meta::TableSchema table_info = BuildTableSchema();
    auto stat = db_->CreateTable(table_info);

    milvus::engine::meta::TableSchema table_info_get;
    table_info_get.table_id_ = GetTableName();
    stat = db_->DescribeTable(table_info_get);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(table_info_get.dimension_, TABLE_DIM);

    int64_t nb = 100000;
    milvus::engine::VectorsData xb;
    BuildVectors(nb, xb);

    for (int64_t i = 0; i < nb; i++) {
        xb.id_array_.push_back(i);
    }

    stat = db_->InsertVectors(GetTableName(), "", xb);
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

    int topk = 10, nprobe = 10;

    for (auto i : ids_to_search) {
        //        std::cout << "xxxxxxxxxxxxxxxxxxxx " << i << std::endl;
        std::vector<std::string> tags;
        milvus::engine::ResultIds result_ids;
        milvus::engine::ResultDistances result_distances;

        stat = db_->QueryByID(dummy_context_, GetTableName(), tags, topk, nprobe, i, result_ids, result_distances);
        ASSERT_EQ(result_ids[0], i);
        ASSERT_LT(result_distances[0], 1e-4);
    }
}

TEST_F(SearchByIdTest, with_index) {
    milvus::engine::meta::TableSchema table_info = BuildTableSchema();
    auto stat = db_->CreateTable(table_info);

    milvus::engine::meta::TableSchema table_info_get;
    table_info_get.table_id_ = GetTableName();
    stat = db_->DescribeTable(table_info_get);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(table_info_get.dimension_, TABLE_DIM);

    int64_t nb = 10000;
    milvus::engine::VectorsData xb;
    BuildVectors(nb, xb);

    for (int64_t i = 0; i < nb; i++) {
        xb.id_array_.push_back(i);
    }

    stat = db_->InsertVectors(GetTableName(), "", xb);
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

    milvus::engine::TableIndex index;
    index.engine_type_ = (int)milvus::engine::EngineType::FAISS_IVFSQ8;
    stat = db_->CreateIndex(GetTableName(), index);
    ASSERT_TRUE(stat.ok());

    int topk = 10, nprobe = 10;

    for (auto i : ids_to_search) {
        //        std::cout << "xxxxxxxxxxxxxxxxxxxx " << i << std::endl;
        std::vector<std::string> tags;
        milvus::engine::ResultIds result_ids;
        milvus::engine::ResultDistances result_distances;

        stat = db_->QueryByID(dummy_context_, GetTableName(), tags, topk, nprobe, i, result_ids, result_distances);
        ASSERT_EQ(result_ids[0], i);
        ASSERT_LT(result_distances[0], 1e-3);
    }
}

TEST_F(SearchByIdTest, with_delete) {
    milvus::engine::meta::TableSchema table_info = BuildTableSchema();
    auto stat = db_->CreateTable(table_info);

    milvus::engine::meta::TableSchema table_info_get;
    table_info_get.table_id_ = GetTableName();
    stat = db_->DescribeTable(table_info_get);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(table_info_get.dimension_, TABLE_DIM);

    int64_t nb = 100000;
    milvus::engine::VectorsData xb;
    BuildVectors(nb, xb);

    for (int64_t i = 0; i < nb; i++) {
        xb.id_array_.push_back(i);
    }

    stat = db_->InsertVectors(GetTableName(), "", xb);
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
    stat = db_->DeleteVectors(GetTableName(), ids_to_delete);

    stat = db_->Flush();
    ASSERT_TRUE(stat.ok());

    int topk = 10, nprobe = 10;

    for (auto i : ids_to_search) {
        //        std::cout << "xxxxxxxxxxxxxxxxxxxx " << i << std::endl;
        std::vector<std::string> tags;
        milvus::engine::ResultIds result_ids;
        milvus::engine::ResultDistances result_distances;

        stat = db_->QueryByID(dummy_context_, GetTableName(), tags, topk, nprobe, i, result_ids, result_distances);
        ASSERT_EQ(result_ids[0], -1);
        ASSERT_EQ(result_distances[0], std::numeric_limits<float>::max());
    }
}

TEST_F(GetVectorByIdTest, basic) {
    milvus::engine::meta::TableSchema table_info = BuildTableSchema();
    auto stat = db_->CreateTable(table_info);

    milvus::engine::meta::TableSchema table_info_get;
    table_info_get.table_id_ = GetTableName();
    stat = db_->DescribeTable(table_info_get);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(table_info_get.dimension_, TABLE_DIM);

    int64_t nb = 100000;
    milvus::engine::VectorsData xb;
    BuildVectors(nb, xb);

    for (int64_t i = 0; i < nb; i++) {
        xb.id_array_.push_back(i);
    }

    stat = db_->InsertVectors(GetTableName(), "", xb);
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

    int topk = 10, nprobe = 10;

    for (auto id : ids_to_search) {
        //        std::cout << "xxxxxxxxxxxxxxxxxxxx " << i << std::endl;
        std::vector<std::string> tags;
        milvus::engine::ResultIds result_ids;
        milvus::engine::ResultDistances result_distances;

        milvus::engine::VectorsData vector;
        stat = db_->GetVectorByID(GetTableName(), id, vector);
        ASSERT_TRUE(stat.ok());

        stat = db_->Query(dummy_context_, GetTableName(), tags, topk, nprobe, vector, result_ids, result_distances);
        ASSERT_TRUE(stat.ok());
        ASSERT_EQ(result_ids[0], id);
        ASSERT_LT(result_distances[0], 1e-4);
    }
}

TEST_F(GetVectorByIdTest, with_index) {
    milvus::engine::meta::TableSchema table_info = BuildTableSchema();
    auto stat = db_->CreateTable(table_info);

    milvus::engine::meta::TableSchema table_info_get;
    table_info_get.table_id_ = GetTableName();
    stat = db_->DescribeTable(table_info_get);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(table_info_get.dimension_, TABLE_DIM);

    int64_t nb = 10000;
    milvus::engine::VectorsData xb;
    BuildVectors(nb, xb);

    for (int64_t i = 0; i < nb; i++) {
        xb.id_array_.push_back(i);
    }

    stat = db_->InsertVectors(GetTableName(), "", xb);
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

    milvus::engine::TableIndex index;
    index.engine_type_ = (int)milvus::engine::EngineType::FAISS_IVFSQ8;
    stat = db_->CreateIndex(GetTableName(), index);
    ASSERT_TRUE(stat.ok());

    int topk = 10, nprobe = 10;

    for (auto id : ids_to_search) {
        //        std::cout << "xxxxxxxxxxxxxxxxxxxx " << i << std::endl;
        std::vector<std::string> tags;
        milvus::engine::ResultIds result_ids;
        milvus::engine::ResultDistances result_distances;

        milvus::engine::VectorsData vector;
        stat = db_->GetVectorByID(GetTableName(), id, vector);
        ASSERT_TRUE(stat.ok());

        stat = db_->Query(dummy_context_, GetTableName(), tags, topk, nprobe, vector, result_ids, result_distances);
        ASSERT_EQ(result_ids[0], id);
        ASSERT_LT(result_distances[0], 1e-3);
    }
}

TEST_F(GetVectorByIdTest, with_delete) {
    milvus::engine::meta::TableSchema table_info = BuildTableSchema();
    auto stat = db_->CreateTable(table_info);

    milvus::engine::meta::TableSchema table_info_get;
    table_info_get.table_id_ = GetTableName();
    stat = db_->DescribeTable(table_info_get);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(table_info_get.dimension_, TABLE_DIM);

    int64_t nb = 100000;
    milvus::engine::VectorsData xb;
    BuildVectors(nb, xb);

    for (int64_t i = 0; i < nb; i++) {
        xb.id_array_.push_back(i);
    }

    stat = db_->InsertVectors(GetTableName(), "", xb);
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
    stat = db_->DeleteVectors(GetTableName(), ids_to_delete);

    stat = db_->Flush();
    ASSERT_TRUE(stat.ok());

    for (auto id : ids_to_search) {
        //        std::cout << "xxxxxxxxxxxxxxxxxxxx " << i << std::endl;
        std::vector<std::string> tags;
        milvus::engine::ResultIds result_ids;
        milvus::engine::ResultDistances result_distances;

        milvus::engine::VectorsData vector;
        stat = db_->GetVectorByID(GetTableName(), id, vector);
        ASSERT_TRUE(stat.ok());
        ASSERT_TRUE(vector.float_data_.empty());
        ASSERT_EQ(vector.vector_count_, 0);
    }
}

TEST_F(SearchByIdTest, BINARY) {
    milvus::engine::meta::TableSchema table_info;
    table_info.dimension_ = TABLE_DIM;
    table_info.table_id_ = GetTableName();
    table_info.engine_type_ = (int)milvus::engine::EngineType::FAISS_BIN_IDMAP;
    table_info.metric_type_ = (int32_t)milvus::engine::MetricType::JACCARD;
    auto stat = db_->CreateTable(table_info);
    ASSERT_TRUE(stat.ok());

    milvus::engine::meta::TableSchema table_info_get;
    table_info_get.table_id_ = GetTableName();
    stat = db_->DescribeTable(table_info_get);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(table_info_get.dimension_, TABLE_DIM);

    int insert_loop = 10;
    int64_t nb = 1000;

    for (int k = 0; k < insert_loop; ++k) {
        milvus::engine::VectorsData vectors;
        vectors.vector_count_ = nb;
        vectors.binary_data_.clear();
        vectors.binary_data_.resize(nb * TABLE_DIM);
        uint8_t* data = vectors.binary_data_.data();
        vectors.id_array_.clear();

        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> distribution{0, std::numeric_limits<uint8_t>::max()};
        for (int i = 0; i < nb; i++) {
            for (int j = 0; j < TABLE_DIM; j++) {
                data[TABLE_DIM * i + j] = distribution(gen);
            }
            vectors.id_array_.emplace_back(k * nb + i);
        }

        stat = db_->InsertVectors(GetTableName(), "", vectors);
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

    //    std::this_thread::sleep_for(std::chrono::seconds(3));  // ensure raw data write to disk
    stat = db_->Flush();
    ASSERT_TRUE(stat.ok());

    uint64_t row_count;
    stat = db_->GetTableRowCount(GetTableName(), row_count);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(row_count, nb * insert_loop);

    int topk = 10, nprobe = 10;

    for (auto id : ids_to_search) {
        //        std::cout << "xxxxxxxxxxxxxxxxxxxx " << i << std::endl;
        std::vector<std::string> tags;
        milvus::engine::ResultIds result_ids;
        milvus::engine::ResultDistances result_distances;

        milvus::engine::VectorsData vector;
        stat = db_->GetVectorByID(GetTableName(), id, vector);
        ASSERT_TRUE(stat.ok());
        ASSERT_EQ(vector.vector_count_, 1);

        stat = db_->Query(dummy_context_, GetTableName(), tags, topk, nprobe, vector, result_ids, result_distances);
        ASSERT_TRUE(stat.ok());
        ASSERT_EQ(result_ids[0], id);
        ASSERT_LT(result_distances[0], 1e-4);

        tags.clear();
        result_ids.clear();
        result_distances.clear();

        stat = db_->QueryByID(dummy_context_, GetTableName(), tags, topk, nprobe, id, result_ids, result_distances);
        ASSERT_TRUE(stat.ok());
        ASSERT_EQ(result_ids[0], id);
        ASSERT_LT(result_distances[0], 1e-4);
    }
}
