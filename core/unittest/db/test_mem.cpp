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
#include <random>
#include <thread>
#include <fiu-control.h>
#include <fiu-local.h>
#include <src/db/DBFactory.h>

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

TEST_F(MemManagerTest, VECTOR_SOURCE_TEST) {
    milvus::engine::meta::CollectionSchema collection_schema = BuildCollectionSchema();
    auto status = impl_->CreateCollection(collection_schema);
    ASSERT_TRUE(status.ok());

    milvus::engine::meta::SegmentSchema table_file_schema;
    table_file_schema.collection_id_ = GetCollectionName();
    status = impl_->CreateCollectionFile(table_file_schema);
    ASSERT_TRUE(status.ok());

    int64_t n = 100;
    milvus::engine::VectorsData vectors;
    BuildVectors(n, vectors);

    milvus::engine::VectorSource source(vectors);

    size_t num_vectors_added;

    //    milvus::engine::ExecutionEnginePtr execution_engine_ = milvus::engine::EngineFactory::Build(
    //        table_file_schema.dimension_, table_file_schema.location_,
    //        (milvus::engine::EngineType)table_file_schema.engine_type_,
    //        (milvus::engine::MetricType)table_file_schema.metric_type_, collection_schema.nlist_);
    std::string directory;
    milvus::engine::utils::GetParentPath(table_file_schema.location_, directory);
    auto segment_writer_ptr = std::make_shared<milvus::segment::SegmentWriter>(directory);

    status = source.Add(segment_writer_ptr, table_file_schema, 50, num_vectors_added);

    ASSERT_TRUE(status.ok());
    ASSERT_EQ(num_vectors_added, 50);
    ASSERT_EQ(source.GetVectorIds().size(), 50);

    vectors.id_array_.clear();
    status = source.Add(segment_writer_ptr, table_file_schema, 60, num_vectors_added);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(num_vectors_added, 50);
    ASSERT_EQ(source.GetVectorIds().size(), 100);

    auto current_num_vectors_added = source.GetNumVectorsAdded();
    ASSERT_EQ(current_num_vectors_added, 100);

    vectors.id_array_ = source.GetVectorIds();
    ASSERT_EQ(vectors.id_array_.size(), 100);
}

TEST_F(MemManagerTest, MEM_TABLE_FILE_TEST) {
    auto options = GetOptions();
    fiu_init(0);

    milvus::engine::meta::CollectionSchema collection_schema = BuildCollectionSchema();
    auto status = impl_->CreateCollection(collection_schema);
    ASSERT_TRUE(status.ok());

    milvus::engine::MemTableFile mem_table_file(GetCollectionName(), impl_, options);

    int64_t n_100 = 100;
    milvus::engine::VectorsData vectors_100;
    BuildVectors(n_100, vectors_100);

    milvus::engine::VectorSourcePtr source = std::make_shared<milvus::engine::VectorSource>(vectors_100);

    status = mem_table_file.Add(source);
    ASSERT_TRUE(status.ok());

    //    std::cout << mem_table_file.GetCurrentMem() << " " << mem_table_file.GetMemLeft() << std::endl;

    size_t singleVectorMem = sizeof(float) * COLLECTION_DIM;
    ASSERT_EQ(mem_table_file.GetCurrentMem(), n_100 * singleVectorMem);

    int64_t n_max = milvus::engine::MAX_TABLE_FILE_MEM / singleVectorMem;
    milvus::engine::VectorsData vectors_128M;
    BuildVectors(n_max, vectors_128M);

    milvus::engine::VectorSourcePtr source_128M =
        std::make_shared<milvus::engine::VectorSource>(vectors_128M);
    status = mem_table_file.Add(source_128M);

    ASSERT_EQ(source_128M->GetVectorIds().size(), n_max - n_100);

    ASSERT_TRUE(mem_table_file.IsFull());

    //mem_table_file has no memory left = 0
    status = mem_table_file.Add(source_128M);
    ASSERT_TRUE(status.ok());

    {
        //test fail create collection file
        FIU_ENABLE_FIU("SqliteMetaImpl.CreateCollectionFile.throw_exception");
        milvus::engine::MemTableFile mem_table_file_1(GetCollectionName(), impl_, options);
        fiu_disable("SqliteMetaImpl.CreateCollectionFile.throw_exception");

        status = mem_table_file_1.Add(source);
        ASSERT_FALSE(status.ok());
        ASSERT_EQ(status.code(), milvus::DB_ERROR);
    }

    {
        options.insert_cache_immediately_ = true;
        milvus::engine::meta::CollectionSchema collection_schema = BuildCollectionSchema();
        collection_schema.collection_id_ = "faiss_pq";
        collection_schema.engine_type_ = (int)milvus::engine::EngineType::FAISS_PQ;
        auto status = impl_->CreateCollection(collection_schema);
        ASSERT_TRUE(status.ok());

        milvus::engine::MemTableFile mem_table_file_1("faiss_pq", impl_, options);
        mem_table_file_1.Serialize(0);
    }
}

TEST_F(MemManagerTest, MEM_TABLE_TEST) {
    auto options = GetOptions();

    milvus::engine::meta::CollectionSchema collection_schema = BuildCollectionSchema();
    auto status = impl_->CreateCollection(collection_schema);
    ASSERT_TRUE(status.ok());

    int64_t n_100 = 100;
    milvus::engine::VectorsData vectors_100;
    BuildVectors(n_100, vectors_100);

    milvus::engine::VectorSourcePtr source_100 = std::make_shared<milvus::engine::VectorSource>(vectors_100);
    milvus::engine::MemTable mem_table(GetCollectionName(), impl_, options);

    status = mem_table.Add(source_100);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(source_100->GetVectorIds().size(), 100);

    milvus::engine::MemTableFilePtr mem_table_file;
    mem_table.GetCurrentMemTableFile(mem_table_file);
    size_t singleVectorMem = sizeof(float) * COLLECTION_DIM;
    ASSERT_EQ(mem_table_file->GetCurrentMem(), n_100 * singleVectorMem);

    int64_t n_max = milvus::engine::MAX_TABLE_FILE_MEM / singleVectorMem;
    milvus::engine::VectorsData vectors_128M;
    BuildVectors(n_max, vectors_128M);

    milvus::engine::VectorSourcePtr source_128M =
        std::make_shared<milvus::engine::VectorSource>(vectors_128M);
    status = mem_table.Add(source_128M);
    ASSERT_TRUE(status.ok());

    ASSERT_EQ(source_128M->GetVectorIds().size(), n_max);

    mem_table.GetCurrentMemTableFile(mem_table_file);
    ASSERT_EQ(mem_table_file->GetCurrentMem(), n_100 * singleVectorMem);

    ASSERT_EQ(mem_table.GetTableFileCount(), 2);

    int64_t n_1G = 1024000;
    milvus::engine::VectorsData vectors_1G;
    BuildVectors(n_1G, vectors_1G);

    milvus::engine::VectorSourcePtr source_1G = std::make_shared<milvus::engine::VectorSource>(vectors_1G);

    status = mem_table.Add(source_1G);
    ASSERT_TRUE(status.ok());

    ASSERT_EQ(source_1G->GetVectorIds().size(), n_1G);

    int expectedTableFileCount = 2 + std::ceil((n_1G - n_100) * singleVectorMem / milvus::engine::MAX_TABLE_FILE_MEM);
    ASSERT_EQ(mem_table.GetTableFileCount(), expectedTableFileCount);

    status = mem_table.Serialize(0);
    ASSERT_TRUE(status.ok());

    milvus::engine::VectorsData vectors_10;
    BuildVectors(10, vectors_10);
    milvus::engine::VectorSourcePtr source_10 = std::make_shared<milvus::engine::VectorSource>(vectors_10);

    fiu_init(0);

    status = mem_table.Add(source_10);
    ASSERT_TRUE(status.ok());

    FIU_ENABLE_FIU("SqliteMetaImpl.UpdateCollectionFile.throw_exception");
    status = mem_table.Serialize(0);
    ASSERT_FALSE(status.ok());
    fiu_disable("SqliteMetaImpl.UpdateCollectionFile.throw_exception");
}

TEST_F(MemManagerTest2, SERIAL_INSERT_SEARCH_TEST) {
    milvus::engine::meta::CollectionSchema collection_info = BuildCollectionSchema();
    auto stat = db_->CreateCollection(collection_info);

    milvus::engine::meta::CollectionSchema collection_info_get;
    collection_info_get.collection_id_ = GetCollectionName();
    stat = db_->DescribeCollection(collection_info_get);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(collection_info_get.dimension_, COLLECTION_DIM);

    int64_t nb = 100000;
    milvus::engine::VectorsData xb;
    BuildVectors(nb, xb);

    for (int64_t i = 0; i < nb; i++) {
        xb.id_array_.push_back(i);
    }

    stat = db_->InsertVectors(GetCollectionName(), "", xb);
    ASSERT_TRUE(stat.ok());

    //    std::this_thread::sleep_for(std::chrono::seconds(3));  // ensure raw data write to disk
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

    const int topk = 10, nprobe = 10;
    milvus::json json_params = {{"nprobe", nprobe}};
    for (auto& pair : search_vectors) {
        auto& search = pair.second;

        std::vector<std::string> tags;
        milvus::engine::ResultIds result_ids;
        milvus::engine::ResultDistances result_distances;

        stat =
            db_->Query(dummy_context_,
                       GetCollectionName(),
                       tags,
                       topk,
                       json_params,
                       search,
                       result_ids,
                       result_distances);
        ASSERT_EQ(result_ids[0], pair.first);
        ASSERT_LT(result_distances[0], 1e-4);
    }
}

TEST_F(MemManagerTest2, INSERT_TEST) {
    milvus::engine::meta::CollectionSchema collection_info = BuildCollectionSchema();
    auto stat = db_->CreateCollection(collection_info);

    milvus::engine::meta::CollectionSchema collection_info_get;
    collection_info_get.collection_id_ = GetCollectionName();
    stat = db_->DescribeCollection(collection_info_get);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(collection_info_get.dimension_, COLLECTION_DIM);

    auto start_time = METRICS_NOW_TIME;

    int insert_loop = 20;
    for (int i = 0; i < insert_loop; ++i) {
        int64_t nb = 40960;
        milvus::engine::VectorsData xb;
        BuildVectors(nb, xb);
        milvus::engine::IDNumbers vector_ids;
        stat = db_->InsertVectors(GetCollectionName(), "", xb);
        ASSERT_TRUE(stat.ok());
    }
    auto end_time = METRICS_NOW_TIME;
    auto total_time = METRICS_MICROSECONDS(start_time, end_time);
    LOG(DEBUG) << "total_time spent in INSERT_TEST (ms) : " << total_time;
}

TEST_F(MemManagerTest2, INSERT_BINARY_TEST) {
    milvus::engine::meta::CollectionSchema collection_info;
    collection_info.dimension_ = COLLECTION_DIM;
    collection_info.collection_id_ = GetCollectionName();
    collection_info.engine_type_ = (int)milvus::engine::EngineType::FAISS_BIN_IDMAP;
    collection_info.metric_type_ = (int32_t)milvus::engine::MetricType::JACCARD;
    auto stat = db_->CreateCollection(collection_info);
    ASSERT_TRUE(stat.ok());

    milvus::engine::meta::CollectionSchema collection_info_get;
    collection_info_get.collection_id_ = GetCollectionName();
    stat = db_->DescribeCollection(collection_info_get);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(collection_info_get.dimension_, COLLECTION_DIM);

    int insert_loop = 10;
    for (int k = 0; k < insert_loop; ++k) {
        milvus::engine::VectorsData vectors;
        int64_t nb = 10000;
        vectors.vector_count_ = nb;
        vectors.binary_data_.clear();
        vectors.binary_data_.resize(nb * COLLECTION_DIM);
        uint8_t* data = vectors.binary_data_.data();

        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> distribution{0, std::numeric_limits<uint8_t>::max()};
        for (int i = 0; i < nb; i++) {
            for (int j = 0; j < COLLECTION_DIM; j++) data[COLLECTION_DIM * i + j] = distribution(gen);
        }
        milvus::engine::IDNumbers vector_ids;
        stat = db_->InsertVectors(GetCollectionName(), "", vectors);
        ASSERT_TRUE(stat.ok());
    }
}

//TEST_F(MemManagerTest2, CONCURRENT_INSERT_SEARCH_TEST) {
//    milvus::engine::meta::CollectionSchema collection_info = BuildCollectionSchema();
//    auto stat = db_->CreateCollection(collection_info);
//
//    milvus::engine::meta::CollectionSchema collection_info_get;
//    collection_info_get.collection_id_ = GetCollectionName();
//    stat = db_->DescribeCollection(collection_info_get);
//    ASSERT_TRUE(stat.ok());
//    ASSERT_EQ(collection_info_get.dimension_, COLLECTION_DIM);
//
//    int64_t nb = 40960;
//    milvus::engine::VectorsData xb;
//    BuildVectors(nb, xb);
//
//    int64_t qb = 5;
//    milvus::engine::VectorsData qxb;
//    BuildVectors(qb, qxb);
//
//    std::thread search([&]() {
//        milvus::engine::ResultIds result_ids;
//        milvus::engine::ResultDistances result_distances;
//        int k = 10;
//        std::this_thread::sleep_for(std::chrono::seconds(2));
//
//        INIT_TIMER;
//        std::stringstream ss;
//        uint64_t count = 0;
//        uint64_t prev_count = 0;
//        milvus::json json_params = {{"nprobe", 10}};
//
//        for (auto j = 0; j < 10; ++j) {
//            ss.str("");
//            db_->Size(count);
//            prev_count = count;
//
//            START_TIMER;
//
//            std::vector<std::string> tags;
//            stat =
//                db_->Query(dummy_context_,
//                           GetCollectionName(),
//                           tags,
//                           k,
//                           json_params,
//                           qxb,
//                           result_ids,
//                           result_distances);
//            ss << "Search " << j << " With Size " << count / milvus::engine::M << " M";
//            STOP_TIMER(ss.str());
//
//            ASSERT_TRUE(stat.ok());
//            for (auto i = 0; i < qb; ++i) {
//                ss.str("");
//                ss << "Result [" << i << "]:";
//                for (auto t = 0; t < k; t++) {
//                    ss << result_ids[i * k + t] << " ";
//                }
//
//                LOG(DEBUG) << ss.str();
//            }
//            ASSERT_TRUE(count >= prev_count);
//            std::this_thread::sleep_for(std::chrono::seconds(1));
//        }
//    });
//
//    int loop = 20;
//
//    for (auto i = 0; i < loop; ++i) {
//        if (i == 0) {
//            qxb.id_array_.clear();
//            db_->InsertVectors(GetCollectionName(), "", qxb);
//            ASSERT_EQ(qxb.id_array_.size(), qb);
//        } else {
//            xb.id_array_.clear();
//            db_->InsertVectors(GetCollectionName(), "", xb);
//            ASSERT_EQ(xb.id_array_.size(), nb);
//        }
//        std::this_thread::sleep_for(std::chrono::microseconds(1));
//    }
//
//    search.join();
//}

TEST_F(MemManagerTest2, VECTOR_IDS_TEST) {
    milvus::engine::meta::CollectionSchema collection_info = BuildCollectionSchema();
    auto stat = db_->CreateCollection(collection_info);

    milvus::engine::meta::CollectionSchema collection_info_get;
    collection_info_get.collection_id_ = GetCollectionName();
    stat = db_->DescribeCollection(collection_info_get);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(collection_info_get.dimension_, COLLECTION_DIM);

    int64_t nb = 100000;
    milvus::engine::VectorsData xb;
    BuildVectors(nb, xb);

    xb.id_array_.resize(nb);
    for (auto i = 0; i < nb; i++) {
        xb.id_array_[i] = i;
    }

    stat = db_->InsertVectors(GetCollectionName(), "", xb);
    ASSERT_EQ(xb.id_array_[0], 0);
    ASSERT_TRUE(stat.ok());

    nb = 25000;
    BuildVectors(nb, xb);

    xb.id_array_.resize(nb);
    for (auto i = 0; i < nb; i++) {
        xb.id_array_[i] = i + nb;
    }
    stat = db_->InsertVectors(GetCollectionName(), "", xb);
    ASSERT_EQ(xb.id_array_[0], nb);
    ASSERT_TRUE(stat.ok());

    nb = 262144;  // 512M
    BuildVectors(nb, xb);

    xb.id_array_.resize(nb);
    for (auto i = 0; i < nb; i++) {
        xb.id_array_[i] = i + nb / 2;
    }
    stat = db_->InsertVectors(GetCollectionName(), "", xb);
    ASSERT_EQ(xb.id_array_[0], nb / 2);
    ASSERT_TRUE(stat.ok());

    nb = 65536;  // 128M
    BuildVectors(nb, xb);
    xb.id_array_.clear();
    stat = db_->InsertVectors(GetCollectionName(), "", xb);
    ASSERT_TRUE(stat.ok());

    nb = 100;
    BuildVectors(nb, xb);

    xb.id_array_.resize(nb);
    for (auto i = 0; i < nb; i++) {
        xb.id_array_[i] = i + nb;
    }
    stat = db_->InsertVectors(GetCollectionName(), "", xb);
    for (auto i = 0; i < nb; i++) {
        ASSERT_EQ(xb.id_array_[i], i + nb);
    }
}
