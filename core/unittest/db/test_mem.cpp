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
#include <random>
#include <thread>

#include "db/Constants.h"
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
    table_info.engine_type_ = (int)milvus::engine::EngineType::FAISS_IDMAP;
    return table_info;
}

void
BuildVectors(int64_t n, std::vector<float>& vectors) {
    vectors.clear();
    vectors.resize(n * TABLE_DIM);
    float* data = vectors.data();
    for (int i = 0; i < n; i++) {
        for (int j = 0; j < TABLE_DIM; j++) data[TABLE_DIM * i + j] = drand48();
    }
}
}  // namespace

TEST_F(MemManagerTest, VECTOR_SOURCE_TEST) {
    milvus::engine::meta::TableSchema table_schema = BuildTableSchema();
    auto status = impl_->CreateTable(table_schema);
    ASSERT_TRUE(status.ok());

    milvus::engine::meta::TableFileSchema table_file_schema;
    table_file_schema.table_id_ = GetTableName();
    status = impl_->CreateTableFile(table_file_schema);
    ASSERT_TRUE(status.ok());

    int64_t n = 100;
    std::vector<float> vectors;
    BuildVectors(n, vectors);

    milvus::engine::VectorSource source(n, vectors.data());

    size_t num_vectors_added;
    milvus::engine::ExecutionEnginePtr execution_engine_ = milvus::engine::EngineFactory::Build(
        table_file_schema.dimension_, table_file_schema.location_,
        (milvus::engine::EngineType)table_file_schema.engine_type_,
        (milvus::engine::MetricType)table_file_schema.metric_type_, table_schema.nlist_);

    milvus::engine::IDNumbers vector_ids;
    status = source.Add(execution_engine_, table_file_schema, 50, num_vectors_added, vector_ids);
    ASSERT_TRUE(status.ok());
    vector_ids = source.GetVectorIds();
    ASSERT_EQ(vector_ids.size(), 50);
    ASSERT_EQ(num_vectors_added, 50);

    vector_ids.clear();
    status = source.Add(execution_engine_, table_file_schema, 60, num_vectors_added, vector_ids);
    ASSERT_TRUE(status.ok());

    ASSERT_EQ(num_vectors_added, 50);

    vector_ids = source.GetVectorIds();
    ASSERT_EQ(vector_ids.size(), 100);
}

TEST_F(MemManagerTest, MEM_TABLE_FILE_TEST) {
    auto options = GetOptions();

    milvus::engine::meta::TableSchema table_schema = BuildTableSchema();
    auto status = impl_->CreateTable(table_schema);
    ASSERT_TRUE(status.ok());

    milvus::engine::MemTableFile mem_table_file(GetTableName(), impl_, options);

    int64_t n_100 = 100;
    std::vector<float> vectors_100;
    BuildVectors(n_100, vectors_100);

    milvus::engine::VectorSourcePtr source = std::make_shared<milvus::engine::VectorSource>(n_100, vectors_100.data());

    milvus::engine::IDNumbers vector_ids;
    status = mem_table_file.Add(source, vector_ids);
    ASSERT_TRUE(status.ok());

    //    std::cout << mem_table_file.GetCurrentMem() << " " << mem_table_file.GetMemLeft() << std::endl;

    vector_ids = source->GetVectorIds();
    ASSERT_EQ(vector_ids.size(), 100);

    size_t singleVectorMem = sizeof(float) * TABLE_DIM;
    ASSERT_EQ(mem_table_file.GetCurrentMem(), n_100 * singleVectorMem);

    int64_t n_max = milvus::engine::MAX_TABLE_FILE_MEM / singleVectorMem;
    std::vector<float> vectors_128M;
    BuildVectors(n_max, vectors_128M);

    milvus::engine::VectorSourcePtr source_128M =
        std::make_shared<milvus::engine::VectorSource>(n_max, vectors_128M.data());
    vector_ids.clear();
    status = mem_table_file.Add(source_128M, vector_ids);

    vector_ids = source_128M->GetVectorIds();
    ASSERT_EQ(vector_ids.size(), n_max - n_100);

    ASSERT_TRUE(mem_table_file.IsFull());
}

TEST_F(MemManagerTest, MEM_TABLE_TEST) {
    auto options = GetOptions();

    milvus::engine::meta::TableSchema table_schema = BuildTableSchema();
    auto status = impl_->CreateTable(table_schema);
    ASSERT_TRUE(status.ok());

    int64_t n_100 = 100;
    std::vector<float> vectors_100;
    BuildVectors(n_100, vectors_100);

    milvus::engine::VectorSourcePtr source_100 =
        std::make_shared<milvus::engine::VectorSource>(n_100, vectors_100.data());

    milvus::engine::MemTable mem_table(GetTableName(), impl_, options);

    milvus::engine::IDNumbers vector_ids;
    status = mem_table.Add(source_100, vector_ids);
    ASSERT_TRUE(status.ok());
    vector_ids = source_100->GetVectorIds();
    ASSERT_EQ(vector_ids.size(), 100);

    milvus::engine::MemTableFilePtr mem_table_file;
    mem_table.GetCurrentMemTableFile(mem_table_file);
    size_t singleVectorMem = sizeof(float) * TABLE_DIM;
    ASSERT_EQ(mem_table_file->GetCurrentMem(), n_100 * singleVectorMem);

    int64_t n_max = milvus::engine::MAX_TABLE_FILE_MEM / singleVectorMem;
    std::vector<float> vectors_128M;
    BuildVectors(n_max, vectors_128M);

    vector_ids.clear();
    milvus::engine::VectorSourcePtr source_128M =
        std::make_shared<milvus::engine::VectorSource>(n_max, vectors_128M.data());
    status = mem_table.Add(source_128M, vector_ids);
    ASSERT_TRUE(status.ok());

    vector_ids = source_128M->GetVectorIds();
    ASSERT_EQ(vector_ids.size(), n_max);

    mem_table.GetCurrentMemTableFile(mem_table_file);
    ASSERT_EQ(mem_table_file->GetCurrentMem(), n_100 * singleVectorMem);

    ASSERT_EQ(mem_table.GetTableFileCount(), 2);

    int64_t n_1G = 1024000;
    std::vector<float> vectors_1G;
    BuildVectors(n_1G, vectors_1G);

    milvus::engine::VectorSourcePtr source_1G = std::make_shared<milvus::engine::VectorSource>(n_1G, vectors_1G.data());

    vector_ids.clear();
    status = mem_table.Add(source_1G, vector_ids);
    ASSERT_TRUE(status.ok());

    vector_ids = source_1G->GetVectorIds();
    ASSERT_EQ(vector_ids.size(), n_1G);

    int expectedTableFileCount = 2 + std::ceil((n_1G - n_100) * singleVectorMem / milvus::engine::MAX_TABLE_FILE_MEM);
    ASSERT_EQ(mem_table.GetTableFileCount(), expectedTableFileCount);

    status = mem_table.Serialize();
    ASSERT_TRUE(status.ok());
}

TEST_F(MemManagerTest2, SERIAL_INSERT_SEARCH_TEST) {
    milvus::engine::meta::TableSchema table_info = BuildTableSchema();
    auto stat = db_->CreateTable(table_info);

    milvus::engine::meta::TableSchema table_info_get;
    table_info_get.table_id_ = GetTableName();
    stat = db_->DescribeTable(table_info_get);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(table_info_get.dimension_, TABLE_DIM);

    int64_t nb = 100000;
    std::vector<float> xb;
    BuildVectors(nb, xb);

    milvus::engine::IDNumbers vector_ids;
    for (int64_t i = 0; i < nb; i++) {
        vector_ids.push_back(i);
    }

    stat = db_->InsertVectors(GetTableName(), "", nb, xb.data(), vector_ids);
    ASSERT_TRUE(stat.ok());

    std::this_thread::sleep_for(std::chrono::seconds(3));  // ensure raw data write to disk

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int64_t> dis(0, nb - 1);

    int64_t num_query = 10;
    std::map<int64_t, std::vector<float>> search_vectors;
    for (int64_t i = 0; i < num_query; ++i) {
        int64_t index = dis(gen);
        std::vector<float> search;
        for (int64_t j = 0; j < TABLE_DIM; j++) {
            search.push_back(xb[index * TABLE_DIM + j]);
        }
        search_vectors.insert(std::make_pair(vector_ids[index], search));
    }

    int topk = 10, nprobe = 10;
    for (auto& pair : search_vectors) {
        auto& search = pair.second;

        std::vector<std::string> tags;
        milvus::engine::ResultIds result_ids;
        milvus::engine::ResultDistances result_distances;
        stat = db_->Query(dummy_context_, GetTableName(), tags, topk, 1, nprobe, search.data(), result_ids,
                          result_distances);
        ASSERT_EQ(result_ids[0], pair.first);
        ASSERT_LT(result_distances[0], 1e-4);
    }
}

TEST_F(MemManagerTest2, INSERT_TEST) {
    milvus::engine::meta::TableSchema table_info = BuildTableSchema();
    auto stat = db_->CreateTable(table_info);

    milvus::engine::meta::TableSchema table_info_get;
    table_info_get.table_id_ = GetTableName();
    stat = db_->DescribeTable(table_info_get);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(table_info_get.dimension_, TABLE_DIM);

    auto start_time = METRICS_NOW_TIME;

    int insert_loop = 20;
    for (int i = 0; i < insert_loop; ++i) {
        int64_t nb = 40960;
        std::vector<float> xb;
        BuildVectors(nb, xb);
        milvus::engine::IDNumbers vector_ids;
        stat = db_->InsertVectors(GetTableName(), "", nb, xb.data(), vector_ids);
        ASSERT_TRUE(stat.ok());
    }
    auto end_time = METRICS_NOW_TIME;
    auto total_time = METRICS_MICROSECONDS(start_time, end_time);
    LOG(DEBUG) << "total_time spent in INSERT_TEST (ms) : " << total_time;
}

TEST_F(MemManagerTest2, CONCURRENT_INSERT_SEARCH_TEST) {
    milvus::engine::meta::TableSchema table_info = BuildTableSchema();
    auto stat = db_->CreateTable(table_info);

    milvus::engine::meta::TableSchema table_info_get;
    table_info_get.table_id_ = GetTableName();
    stat = db_->DescribeTable(table_info_get);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(table_info_get.dimension_, TABLE_DIM);

    milvus::engine::IDNumbers vector_ids;
    milvus::engine::IDNumbers target_ids;

    int64_t nb = 40960;
    std::vector<float> xb;
    BuildVectors(nb, xb);

    int64_t qb = 5;
    std::vector<float> qxb;
    BuildVectors(qb, qxb);

    std::thread search([&]() {
        milvus::engine::ResultIds result_ids;
        milvus::engine::ResultDistances result_distances;
        int k = 10;
        std::this_thread::sleep_for(std::chrono::seconds(2));

        INIT_TIMER;
        std::stringstream ss;
        uint64_t count = 0;
        uint64_t prev_count = 0;

        for (auto j = 0; j < 10; ++j) {
            ss.str("");
            db_->Size(count);
            prev_count = count;

            START_TIMER;

            std::vector<std::string> tags;
            stat =
                db_->Query(dummy_context_, GetTableName(), tags, k, qb, 10, qxb.data(), result_ids, result_distances);
            ss << "Search " << j << " With Size " << count / milvus::engine::M << " M";
            STOP_TIMER(ss.str());

            ASSERT_TRUE(stat.ok());
            for (auto i = 0; i < qb; ++i) {
                ASSERT_EQ(result_ids[i * k], target_ids[i]);
                ss.str("");
                ss << "Result [" << i << "]:";
                for (auto t = 0; t < k; t++) {
                    ss << result_ids[i * k + t] << " ";
                }
                /* LOG(DEBUG) << ss.str(); */
            }
            ASSERT_TRUE(count >= prev_count);
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    });

    int loop = 20;

    for (auto i = 0; i < loop; ++i) {
        if (i == 0) {
            db_->InsertVectors(GetTableName(), "", qb, qxb.data(), target_ids);
            ASSERT_EQ(target_ids.size(), qb);
        } else {
            db_->InsertVectors(GetTableName(), "", nb, xb.data(), vector_ids);
        }
        std::this_thread::sleep_for(std::chrono::microseconds(1));
    }

    search.join();
}

TEST_F(MemManagerTest2, VECTOR_IDS_TEST) {
    milvus::engine::meta::TableSchema table_info = BuildTableSchema();
    auto stat = db_->CreateTable(table_info);

    milvus::engine::meta::TableSchema table_info_get;
    table_info_get.table_id_ = GetTableName();
    stat = db_->DescribeTable(table_info_get);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(table_info_get.dimension_, TABLE_DIM);

    milvus::engine::IDNumbers vector_ids;

    int64_t nb = 100000;
    std::vector<float> xb;
    BuildVectors(nb, xb);

    vector_ids.resize(nb);
    for (auto i = 0; i < nb; i++) {
        vector_ids[i] = i;
    }

    stat = db_->InsertVectors(GetTableName(), "", nb, xb.data(), vector_ids);
    ASSERT_EQ(vector_ids[0], 0);
    ASSERT_TRUE(stat.ok());

    nb = 25000;
    xb.clear();
    BuildVectors(nb, xb);
    vector_ids.clear();
    vector_ids.resize(nb);
    for (auto i = 0; i < nb; i++) {
        vector_ids[i] = i + nb;
    }
    stat = db_->InsertVectors(GetTableName(), "", nb, xb.data(), vector_ids);
    ASSERT_EQ(vector_ids[0], nb);
    ASSERT_TRUE(stat.ok());

    nb = 262144;  // 512M
    xb.clear();
    BuildVectors(nb, xb);
    vector_ids.clear();
    vector_ids.resize(nb);
    for (auto i = 0; i < nb; i++) {
        vector_ids[i] = i + nb / 2;
    }
    stat = db_->InsertVectors(GetTableName(), "", nb, xb.data(), vector_ids);
    ASSERT_EQ(vector_ids[0], nb / 2);
    ASSERT_TRUE(stat.ok());

    nb = 65536;  // 128M
    xb.clear();
    BuildVectors(nb, xb);
    vector_ids.clear();
    stat = db_->InsertVectors(GetTableName(), "", nb, xb.data(), vector_ids);
    ASSERT_TRUE(stat.ok());

    nb = 100;
    xb.clear();
    BuildVectors(nb, xb);
    vector_ids.clear();
    vector_ids.resize(nb);
    for (auto i = 0; i < nb; i++) {
        vector_ids[i] = i + nb;
    }
    stat = db_->InsertVectors(GetTableName(), "", nb, xb.data(), vector_ids);
    for (auto i = 0; i < nb; i++) {
        ASSERT_EQ(vector_ids[i], i + nb);
    }
}
