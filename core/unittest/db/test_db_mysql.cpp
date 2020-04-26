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

#include <gtest/gtest.h>
#include <fiu-control.h>
#include <fiu-local.h>
#include <boost/filesystem.hpp>
#include <random>
#include <thread>

#include "db/Constants.h"
#include "db/DB.h"
#include "db/DBImpl.h"
#include "db/meta/MetaConsts.h"
#include "db/utils.h"

namespace {

static const char* COLLECTION_NAME = "test_group";
static constexpr int64_t COLLECTION_DIM = 256;
static constexpr int64_t VECTOR_COUNT = 25000;
static constexpr int64_t INSERT_LOOP = 100;

milvus::engine::meta::CollectionSchema
BuildCollectionSchema() {
    milvus::engine::meta::CollectionSchema collection_info;
    collection_info.dimension_ = COLLECTION_DIM;
    collection_info.collection_id_ = COLLECTION_NAME;
    collection_info.engine_type_ = (int)milvus::engine::EngineType::FAISS_IDMAP;
    return collection_info;
}

void
BuildVectors(uint64_t n, uint64_t batch_index, milvus::engine::VectorsData& vectors) {
    vectors.vector_count_ = n;
    vectors.float_data_.clear();
    vectors.float_data_.resize(n * COLLECTION_DIM);
    float* data = vectors.float_data_.data();
    for (uint64_t i = 0; i < n; i++) {
        for (int64_t j = 0; j < COLLECTION_DIM; j++) data[COLLECTION_DIM * i + j] = drand48();
        data[COLLECTION_DIM * i] += i / 2000.;

        vectors.id_array_.push_back(n * batch_index + i);
    }
}

}  // namespace

TEST_F(MySqlDBTest, DB_TEST) {
    milvus::engine::meta::CollectionSchema collection_info = BuildCollectionSchema();
    auto stat = db_->CreateCollection(collection_info);

    milvus::engine::meta::CollectionSchema collection_info_get;
    collection_info_get.collection_id_ = COLLECTION_NAME;
    stat = db_->DescribeCollection(collection_info_get);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(collection_info_get.dimension_, COLLECTION_DIM);

    uint64_t qb = 5;
    milvus::engine::VectorsData qxb;
    BuildVectors(qb, 0, qxb);

    std::thread search([&]() {
        milvus::engine::ResultIds result_ids;
        milvus::engine::ResultDistances result_distances;
        int k = 10;
        std::this_thread::sleep_for(std::chrono::seconds(2));

        INIT_TIMER;
        std::stringstream ss;
        uint64_t count = 0;
        uint64_t prev_count = 0;
        milvus::json json_params = {{"nprobe", 10}};

        for (auto j = 0; j < 10; ++j) {
            ss.str("");
            db_->Size(count);
            prev_count = count;
            if (count == 0) {
                continue;
            }

            START_TIMER;

            std::vector<std::string> tags;
            stat = db_->Query(dummy_context_, COLLECTION_NAME, tags, k, json_params, qxb, result_ids, result_distances);
            ss << "Search " << j << " With Size " << count / milvus::engine::MB << " MB";
            STOP_TIMER(ss.str());

            ASSERT_TRUE(stat.ok());
            ASSERT_EQ(result_ids.size(), qb * k);
            for (auto i = 0; i < qb; ++i) {
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

    int loop = 100;

    for (auto i = 0; i < loop; ++i) {
        if (i == 40) {
            db_->InsertVectors(COLLECTION_NAME, "", qxb);
            ASSERT_EQ(qxb.id_array_.size(), qb);
        } else {
            uint64_t nb = 50;
            milvus::engine::VectorsData xb;
            BuildVectors(nb, i, xb);

            db_->InsertVectors(COLLECTION_NAME, "", xb);
            ASSERT_EQ(xb.id_array_.size(), nb);
        }

        stat = db_->Flush();
        ASSERT_TRUE(stat.ok());

        std::this_thread::sleep_for(std::chrono::microseconds(1));
    }

    search.join();

    uint64_t count;
    stat = db_->GetCollectionRowCount(COLLECTION_NAME, count);
    ASSERT_TRUE(stat.ok());
    ASSERT_GT(count, 0);
}

TEST_F(MySqlDBTest, SEARCH_TEST) {
    milvus::engine::meta::CollectionSchema collection_info = BuildCollectionSchema();
    auto stat = db_->CreateCollection(collection_info);

    milvus::engine::meta::CollectionSchema collection_info_get;
    collection_info_get.collection_id_ = COLLECTION_NAME;
    stat = db_->DescribeCollection(collection_info_get);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(collection_info_get.dimension_, COLLECTION_DIM);

    // prepare raw data
    size_t nb = VECTOR_COUNT;
    size_t nq = 10;
    size_t k = 5;
    milvus::engine::VectorsData xb, xq;
    xb.vector_count_ = nb;
    xb.float_data_.resize(nb * COLLECTION_DIM);
    xq.vector_count_ = nq;
    xq.float_data_.resize(nq * COLLECTION_DIM);
    xb.id_array_.resize(nb);

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis_xt(-1.0, 1.0);
    for (size_t i = 0; i < nb * COLLECTION_DIM; i++) {
        xb.float_data_[i] = dis_xt(gen);
        if (i < nb) {
            xb.id_array_[i] = i;
        }
    }
    for (size_t i = 0; i < nq * COLLECTION_DIM; i++) {
        xq.float_data_[i] = dis_xt(gen);
    }

    // result data
    // std::vector<long> nns_gt(k*nq);
    std::vector<int64_t> nns(k * nq);  // nns = nearst neg search
    // std::vector<float> dis_gt(k*nq);
    std::vector<float> dis(k * nq);

    // insert data
    stat = db_->InsertVectors(COLLECTION_NAME, "", xb);
    ASSERT_TRUE(stat.ok());

    //    sleep(2);  // wait until build index finish
    stat = db_->Flush();
    ASSERT_TRUE(stat.ok());

    std::vector<std::string> tags;
    milvus::engine::ResultIds result_ids;
    milvus::engine::ResultDistances result_distances;
    milvus::json json_params = {{"nprobe", 10}};
    stat = db_->Query(dummy_context_, COLLECTION_NAME, tags, k, json_params, xq, result_ids, result_distances);
    ASSERT_TRUE(stat.ok());
}

TEST_F(MySqlDBTest, ARHIVE_DISK_CHECK) {
    milvus::engine::meta::CollectionSchema collection_info = BuildCollectionSchema();
    auto stat = db_->CreateCollection(collection_info);

    std::vector<milvus::engine::meta::CollectionSchema> collection_schema_array;
    stat = db_->AllCollections(collection_schema_array);
    ASSERT_TRUE(stat.ok());
    bool bfound = false;
    for (auto& schema : collection_schema_array) {
        if (schema.collection_id_ == COLLECTION_NAME) {
            bfound = true;
            break;
        }
    }
    ASSERT_TRUE(bfound);

    fiu_init(0);
    FIU_ENABLE_FIU("MySQLMetaImpl.AllCollection.null_connection");
    stat = db_->AllCollections(collection_schema_array);
    ASSERT_FALSE(stat.ok());

    FIU_ENABLE_FIU("MySQLMetaImpl.AllCollection.throw_exception");
    stat = db_->AllCollections(collection_schema_array);
    ASSERT_FALSE(stat.ok());
    fiu_disable("MySQLMetaImpl.AllCollection.null_connection");
    fiu_disable("MySQLMetaImpl.AllCollection.throw_exception");

    milvus::engine::meta::CollectionSchema collection_info_get;
    collection_info_get.collection_id_ = COLLECTION_NAME;
    stat = db_->DescribeCollection(collection_info_get);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(collection_info_get.dimension_, COLLECTION_DIM);

    milvus::engine::IDNumbers vector_ids;
    milvus::engine::IDNumbers target_ids;

    uint64_t size;
    db_->Size(size);

    int64_t nb = 10;

    int loop = INSERT_LOOP;
    for (auto i = 0; i < loop; ++i) {
        milvus::engine::VectorsData xb;
        BuildVectors(nb, i, xb);
        db_->InsertVectors(COLLECTION_NAME, "", xb);
        std::this_thread::sleep_for(std::chrono::microseconds(1));
    }

    //    std::this_thread::sleep_for(std::chrono::seconds(1));
    stat = db_->Flush();
    ASSERT_TRUE(stat.ok());

    db_->Size(size);
    LOG(DEBUG) << "size=" << size;
    ASSERT_LE(size, 1 * milvus::engine::GB);

    FIU_ENABLE_FIU("MySQLMetaImpl.Size.null_connection");
    stat = db_->Size(size);
    ASSERT_FALSE(stat.ok());
    fiu_disable("MySQLMetaImpl.Size.null_connection");
    FIU_ENABLE_FIU("MySQLMetaImpl.Size.throw_exception");
    stat = db_->Size(size);
    ASSERT_FALSE(stat.ok());
    fiu_disable("MySQLMetaImpl.Size.throw_exception");
}

TEST_F(MySqlDBTest, DELETE_TEST) {
    milvus::engine::meta::CollectionSchema collection_info = BuildCollectionSchema();
    auto stat = db_->CreateCollection(collection_info);
    //    std::cout << stat.ToString() << std::endl;

    milvus::engine::meta::CollectionSchema collection_info_get;
    collection_info_get.collection_id_ = COLLECTION_NAME;
    stat = db_->DescribeCollection(collection_info_get);
    ASSERT_TRUE(stat.ok());

    bool has_collection = false;
    db_->HasCollection(COLLECTION_NAME, has_collection);
    ASSERT_TRUE(has_collection);

    milvus::engine::IDNumbers vector_ids;

    uint64_t size;
    db_->Size(size);

    int64_t nb = INSERT_LOOP;

    int loop = 20;
    for (auto i = 0; i < loop; ++i) {
        milvus::engine::VectorsData xb;
        BuildVectors(nb, i, xb);
        db_->InsertVectors(COLLECTION_NAME, "", xb);
        std::this_thread::sleep_for(std::chrono::microseconds(1));
    }

    stat = db_->Flush();
    ASSERT_TRUE(stat.ok());

    stat = db_->DropCollection(COLLECTION_NAME);
    ////    std::cout << "5 sec start" << std::endl;
    //    std::this_thread::sleep_for(std::chrono::seconds(5));
    ////    std::cout << "5 sec finish" << std::endl;
    ASSERT_TRUE(stat.ok());
    //
    db_->HasCollection(COLLECTION_NAME, has_collection);
    ASSERT_FALSE(has_collection);
}

TEST_F(MySqlDBTest, PARTITION_TEST) {
    milvus::engine::meta::CollectionSchema collection_info = BuildCollectionSchema();
    auto stat = db_->CreateCollection(collection_info);
    ASSERT_TRUE(stat.ok());

    // create partition and insert data
    const int64_t PARTITION_COUNT = 5;
    const int64_t INSERT_BATCH = 2000;
    std::string collection_name = COLLECTION_NAME;
    for (int64_t i = 0; i < PARTITION_COUNT; i++) {
        std::string partition_tag = std::to_string(i);
        std::string partition_name = collection_name + "_" + partition_tag;
        stat = db_->CreatePartition(collection_name, partition_name, partition_tag);
        ASSERT_TRUE(stat.ok());

        fiu_init(0);
        FIU_ENABLE_FIU("MySQLMetaImpl.CreatePartition.aleady_exist");
        stat = db_->CreatePartition(collection_name, partition_name, partition_tag);
        ASSERT_FALSE(stat.ok());
        fiu_disable("MySQLMetaImpl.CreatePartition.aleady_exist");

        // not allow nested partition
        stat = db_->CreatePartition(partition_name, "dumy", "dummy");
        ASSERT_FALSE(stat.ok());

        // not allow duplicated partition
        stat = db_->CreatePartition(collection_name, partition_name, partition_tag);
        ASSERT_FALSE(stat.ok());

        milvus::engine::IDNumbers vector_ids;
        vector_ids.resize(INSERT_BATCH);
        for (int64_t k = 0; k < INSERT_BATCH; k++) {
            vector_ids[k] = i * INSERT_BATCH + k;
        }

        milvus::engine::VectorsData xb;
        BuildVectors(INSERT_BATCH, i, xb);

        db_->InsertVectors(collection_name, partition_tag, xb);
        ASSERT_EQ(vector_ids.size(), INSERT_BATCH);
    }

    // duplicated partition is not allowed
    stat = db_->CreatePartition(collection_name, "", "0");
    ASSERT_FALSE(stat.ok());

    std::vector<milvus::engine::meta::CollectionSchema> partition_schema_array;
    stat = db_->ShowPartitions(collection_name, partition_schema_array);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(partition_schema_array.size(), PARTITION_COUNT);
    for (int64_t i = 0; i < PARTITION_COUNT; i++) {
        ASSERT_EQ(partition_schema_array[i].collection_id_, collection_name + "_" + std::to_string(i));
    }

    {  // build index
        milvus::engine::CollectionIndex index;
        index.engine_type_ = (int)milvus::engine::EngineType::FAISS_IVFFLAT;
        index.metric_type_ = (int)milvus::engine::MetricType::L2;
        stat = db_->CreateIndex(collection_info.collection_id_, index);
        ASSERT_TRUE(stat.ok());

        uint64_t row_count = 0;
        stat = db_->GetCollectionRowCount(COLLECTION_NAME, row_count);
        ASSERT_TRUE(stat.ok());
        ASSERT_EQ(row_count, INSERT_BATCH * PARTITION_COUNT);
    }

    {  // search
        const int64_t nq = 5;
        const int64_t topk = 10;
        const int64_t nprobe = 10;
        milvus::engine::VectorsData xq;
        BuildVectors(nq, 0, xq);

        // specify partition tags
        std::vector<std::string> tags = {"0", std::to_string(PARTITION_COUNT - 1)};
        milvus::engine::ResultIds result_ids;
        milvus::engine::ResultDistances result_distances;
        milvus::json json_params = {{"nprobe", nprobe}};
        stat = db_->Query(dummy_context_, COLLECTION_NAME, tags, topk, json_params, xq, result_ids, result_distances);
        ASSERT_TRUE(stat.ok());
        ASSERT_EQ(result_ids.size() / topk, nq);

        // search in whole collection
        tags.clear();
        result_ids.clear();
        result_distances.clear();
        stat = db_->Query(dummy_context_, COLLECTION_NAME, tags, topk, json_params, xq, result_ids, result_distances);
        ASSERT_TRUE(stat.ok());
        ASSERT_EQ(result_ids.size() / topk, nq);

        // search in all partitions(tag regex match)
        tags.push_back("\\d");
        result_ids.clear();
        result_distances.clear();
        stat = db_->Query(dummy_context_, COLLECTION_NAME, tags, topk, json_params, xq, result_ids, result_distances);
        ASSERT_TRUE(stat.ok());
        ASSERT_EQ(result_ids.size() / topk, nq);
    }

    fiu_init(0);
    {
        //create partition with dummy name
        stat = db_->CreatePartition(collection_name, "", "6");
        ASSERT_TRUE(stat.ok());

        // ensure DescribeCollection failed
        FIU_ENABLE_FIU("MySQLMetaImpl.DescribeCollection.throw_exception");
        stat = db_->CreatePartition(collection_name, "", "7");
        ASSERT_FALSE(stat.ok());
        fiu_disable("MySQLMetaImpl.DescribeCollection.throw_exception");

        //Drop partition will failed,since it firstly drop partition meta collection.
        FIU_ENABLE_FIU("MySQLMetaImpl.DropCollection.null_connection");
        stat = db_->DropPartition(collection_name + "_5");
        //TODO(sjh): add assert expr, since DropPartion always return Status::OK() for now.
        //ASSERT_TRUE(stat.ok());
        fiu_disable("MySQLMetaImpl.DropCollection.null_connection");

        std::vector<milvus::engine::meta::CollectionSchema> partition_schema_array;
        stat = db_->ShowPartitions(collection_name, partition_schema_array);
        ASSERT_TRUE(stat.ok());
        ASSERT_EQ(partition_schema_array.size(), PARTITION_COUNT + 1);

        FIU_ENABLE_FIU("MySQLMetaImpl.ShowPartitions.null_connection");
        stat = db_->ShowPartitions(collection_name, partition_schema_array);
        ASSERT_FALSE(stat.ok());

        FIU_ENABLE_FIU("MySQLMetaImpl.ShowPartitions.throw_exception");
        stat = db_->ShowPartitions(collection_name, partition_schema_array);
        ASSERT_FALSE(stat.ok());

        FIU_ENABLE_FIU("MySQLMetaImpl.DropCollection.throw_exception");
        stat = db_->DropPartition(collection_name + "_4");
        fiu_disable("MySQLMetaImpl.DropCollection.throw_exception");

        stat = db_->DropPartition(collection_name + "_0");
        ASSERT_TRUE(stat.ok());
    }

    {
        FIU_ENABLE_FIU("MySQLMetaImpl.GetPartitionName.null_connection");
        stat = db_->DropPartitionByTag(collection_name, "1");
        ASSERT_FALSE(stat.ok());
        fiu_disable("MySQLMetaImpl.GetPartitionName.null_connection");

        FIU_ENABLE_FIU("MySQLMetaImpl.GetPartitionName.throw_exception");
        stat = db_->DropPartitionByTag(collection_name, "1");
        ASSERT_FALSE(stat.ok());
        fiu_disable("MySQLMetaImpl.GetPartitionName.throw_exception");

        stat = db_->DropPartitionByTag(collection_name, "1");
        ASSERT_TRUE(stat.ok());

        stat = db_->CreatePartition(collection_name, collection_name + "_1", "1");
        FIU_ENABLE_FIU("MySQLMetaImpl.DeleteCollectionFiles.null_connection");
        stat = db_->DropPartition(collection_name + "_1");
        fiu_disable("MySQLMetaImpl.DeleteCollectionFiles.null_connection");

        FIU_ENABLE_FIU("MySQLMetaImpl.DeleteCollectionFiles.throw_exception");
        stat = db_->DropPartition(collection_name + "_1");
        fiu_disable("MySQLMetaImpl.DeleteCollectionFiles.throw_exception");
    }

    {
        FIU_ENABLE_FIU("MySQLMetaImpl.DropCollectionIndex.null_connection");
        stat = db_->DropIndex(collection_name);
        ASSERT_FALSE(stat.ok());
        fiu_disable("MySQLMetaImpl.DropCollectionIndex.null_connection");

        FIU_ENABLE_FIU("MySQLMetaImpl.DropCollectionIndex.throw_exception");
        stat = db_->DropIndex(collection_name);
        ASSERT_FALSE(stat.ok());
        fiu_disable("MySQLMetaImpl.DropCollectionIndex.throw_exception");

        stat = db_->DropIndex(collection_name);
        ASSERT_TRUE(stat.ok());
    }
}


