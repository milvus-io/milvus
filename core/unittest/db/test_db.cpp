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

#include <gtest/gtest.h>

#include <boost/filesystem.hpp>
#include <random>
#include <thread>

#include "cache/CpuCacheMgr.h"
#include "db/Constants.h"
#include "db/DB.h"
#include "db/DBFactory.h"
#include "db/DBImpl.h"
#include "db/meta/MetaConsts.h"
#include "db/utils.h"
#include "server/Config.h"
#include "utils/CommonUtil.h"

namespace {

static const char* TABLE_NAME = "test_group";
static constexpr int64_t TABLE_DIM = 256;
static constexpr int64_t VECTOR_COUNT = 25000;
static constexpr int64_t INSERT_LOOP = 1000;
static constexpr int64_t SECONDS_EACH_HOUR = 3600;
static constexpr int64_t DAY_SECONDS = 24 * 60 * 60;


milvus::engine::meta::TableSchema
BuildTableSchema() {
    milvus::engine::meta::TableSchema table_info;
    table_info.dimension_ = TABLE_DIM;
    table_info.table_id_ = TABLE_NAME;
    return table_info;
}

void
BuildVectors(int64_t n, std::vector<float>& vectors) {
    vectors.clear();
    vectors.resize(n * TABLE_DIM);
    float* data = vectors.data();
    for (int i = 0; i < n; i++) {
        for (int j = 0; j < TABLE_DIM; j++) data[TABLE_DIM * i + j] = drand48();
        data[TABLE_DIM * i] += i / 2000.;
    }
}

std::string
CurrentTmDate(int64_t offset_day = 0) {
    time_t tt;
    time(&tt);
    tt = tt + 8 * SECONDS_EACH_HOUR;
    tt = tt + 24 * SECONDS_EACH_HOUR * offset_day;
    tm t;
    gmtime_r(&tt, &t);

    std::string str =
        std::to_string(t.tm_year + 1900) + "-" + std::to_string(t.tm_mon + 1) + "-" + std::to_string(t.tm_mday);

    return str;
}

void
ConvertTimeRangeToDBDates(const std::string& start_value, const std::string& end_value,
                          std::vector<milvus::engine::meta::DateT>& dates) {
    dates.clear();

    time_t tt_start, tt_end;
    tm tm_start, tm_end;
    if (!milvus::server::CommonUtil::TimeStrToTime(start_value, tt_start, tm_start)) {
        return;
    }

    if (!milvus::server::CommonUtil::TimeStrToTime(end_value, tt_end, tm_end)) {
        return;
    }

    int64_t days = (tt_end > tt_start) ? (tt_end - tt_start) / DAY_SECONDS : (tt_start - tt_end) / DAY_SECONDS;
    if (days == 0) {
        return;
    }

    for (int64_t i = 0; i < days; i++) {
        time_t tt_day = tt_start + DAY_SECONDS * i;
        tm tm_day;
        milvus::server::CommonUtil::ConvertTime(tt_day, tm_day);

        int64_t date = tm_day.tm_year * 10000 + tm_day.tm_mon * 100 + tm_day.tm_mday;  // according to db logic
        dates.push_back(date);
    }
}

}  // namespace

TEST_F(DBTest, CONFIG_TEST) {
    {
        ASSERT_ANY_THROW(milvus::engine::ArchiveConf conf("wrong"));
        /* EXPECT_DEATH(engine::ArchiveConf conf("wrong"), ""); */
    }
    {
        milvus::engine::ArchiveConf conf("delete");
        ASSERT_EQ(conf.GetType(), "delete");
        auto criterias = conf.GetCriterias();
        ASSERT_EQ(criterias.size(), 0);
    }
    {
        milvus::engine::ArchiveConf conf("swap");
        ASSERT_EQ(conf.GetType(), "swap");
        auto criterias = conf.GetCriterias();
        ASSERT_EQ(criterias.size(), 0);
    }
    {
        ASSERT_ANY_THROW(milvus::engine::ArchiveConf conf1("swap", "disk:"));
        ASSERT_ANY_THROW(milvus::engine::ArchiveConf conf2("swap", "disk:a"));
        milvus::engine::ArchiveConf conf("swap", "disk:1024");
        auto criterias = conf.GetCriterias();
        ASSERT_EQ(criterias.size(), 1);
        ASSERT_EQ(criterias["disk"], 1024);
    }
    {
        ASSERT_ANY_THROW(milvus::engine::ArchiveConf conf1("swap", "days:"));
        ASSERT_ANY_THROW(milvus::engine::ArchiveConf conf2("swap", "days:a"));
        milvus::engine::ArchiveConf conf("swap", "days:100");
        auto criterias = conf.GetCriterias();
        ASSERT_EQ(criterias.size(), 1);
        ASSERT_EQ(criterias["days"], 100);
    }
    {
        ASSERT_ANY_THROW(milvus::engine::ArchiveConf conf1("swap", "days:"));
        ASSERT_ANY_THROW(milvus::engine::ArchiveConf conf2("swap", "days:a"));
        milvus::engine::ArchiveConf conf("swap", "days:100;disk:200");
        auto criterias = conf.GetCriterias();
        ASSERT_EQ(criterias.size(), 2);
        ASSERT_EQ(criterias["days"], 100);
        ASSERT_EQ(criterias["disk"], 200);
    }
}

TEST_F(DBTest, DB_TEST) {
    milvus::engine::meta::TableSchema table_info = BuildTableSchema();
    auto stat = db_->CreateTable(table_info);

    milvus::engine::meta::TableSchema table_info_get;
    table_info_get.table_id_ = TABLE_NAME;
    stat = db_->DescribeTable(table_info_get);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(table_info_get.dimension_, TABLE_DIM);

    milvus::engine::IDNumbers vector_ids;
    milvus::engine::IDNumbers target_ids;

    int64_t nb = 50;
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
            stat = db_->Query(dummy_context_, TABLE_NAME, tags, k, qb, 10, qxb.data(), result_ids, result_distances);
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

    int loop = INSERT_LOOP;

    for (auto i = 0; i < loop; ++i) {
        if (i == 40) {
            db_->InsertVectors(TABLE_NAME, "", qb, qxb.data(), target_ids);
            ASSERT_EQ(target_ids.size(), qb);
        } else {
            db_->InsertVectors(TABLE_NAME, "", nb, xb.data(), vector_ids);
        }
        std::this_thread::sleep_for(std::chrono::microseconds(1));
    }

    search.join();

    uint64_t count;
    stat = db_->GetTableRowCount(TABLE_NAME, count);
    ASSERT_TRUE(stat.ok());
    ASSERT_GT(count, 0);
}

TEST_F(DBTest, SEARCH_TEST) {
    milvus::scheduler::OptimizerInst::GetInstance()->Init();
    std::string config_path(CONFIG_PATH);
    config_path += CONFIG_FILE;
    milvus::server::Config& config = milvus::server::Config::GetInstance();
    milvus::Status s = config.LoadConfigFile(config_path);

    milvus::engine::meta::TableSchema table_info = BuildTableSchema();
    auto stat = db_->CreateTable(table_info);

    milvus::engine::meta::TableSchema table_info_get;
    table_info_get.table_id_ = TABLE_NAME;
    stat = db_->DescribeTable(table_info_get);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(table_info_get.dimension_, TABLE_DIM);

    // prepare raw data
    size_t nb = VECTOR_COUNT;
    size_t nq = 10;
    size_t k = 5;
    std::vector<float> xb(nb * TABLE_DIM);
    std::vector<float> xq(nq * TABLE_DIM);
    std::vector<int64_t> ids(nb);

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis_xt(-1.0, 1.0);
    for (size_t i = 0; i < nb * TABLE_DIM; i++) {
        xb[i] = dis_xt(gen);
        if (i < nb) {
            ids[i] = i;
        }
    }
    for (size_t i = 0; i < nq * TABLE_DIM; i++) {
        xq[i] = dis_xt(gen);
    }

    // result data
    // std::vector<long> nns_gt(k*nq);
    std::vector<int64_t> nns(k * nq);  // nns = nearst neg search
    // std::vector<float> dis_gt(k*nq);
    std::vector<float> dis(k * nq);

    // insert data
    const int batch_size = 100;
    for (int j = 0; j < nb / batch_size; ++j) {
        stat = db_->InsertVectors(TABLE_NAME, "", batch_size, xb.data() + batch_size * j * TABLE_DIM, ids);
        if (j == 200) {
            sleep(1);
        }
        ASSERT_TRUE(stat.ok());
    }

    milvus::engine::TableIndex index;
    index.engine_type_ = (int)milvus::engine::EngineType::FAISS_IDMAP;
    db_->CreateIndex(TABLE_NAME, index);  // wait until build index finish

    {
        std::vector<std::string> tags;
        milvus::engine::ResultIds result_ids;
        milvus::engine::ResultDistances result_distances;
        stat = db_->Query(dummy_context_, TABLE_NAME, tags, k, nq, 10, xq.data(), result_ids, result_distances);
        ASSERT_TRUE(stat.ok());
        stat = db_->Query(dummy_context_, TABLE_NAME, tags, k, 1100, 10, xq.data(), result_ids, result_distances);
        ASSERT_TRUE(stat.ok());
    }

    index.engine_type_ = (int)milvus::engine::EngineType::FAISS_IVFFLAT;
    db_->CreateIndex(TABLE_NAME, index);  // wait until build index finish

    {
        std::vector<std::string> tags;
        milvus::engine::ResultIds result_ids;
        milvus::engine::ResultDistances result_distances;
        stat = db_->Query(dummy_context_, TABLE_NAME, tags, k, nq, 10, xq.data(), result_ids, result_distances);
        ASSERT_TRUE(stat.ok());
        stat = db_->Query(dummy_context_, TABLE_NAME, tags, k, 1100, 10, xq.data(), result_ids, result_distances);
        ASSERT_TRUE(stat.ok());
    }

    index.engine_type_ = (int)milvus::engine::EngineType::FAISS_IVFSQ8;
    db_->CreateIndex(TABLE_NAME, index);  // wait until build index finish

    {
        std::vector<std::string> tags;
        milvus::engine::ResultIds result_ids;
        milvus::engine::ResultDistances result_distances;
        stat = db_->Query(dummy_context_, TABLE_NAME, tags, k, nq, 10, xq.data(), result_ids, result_distances);
        ASSERT_TRUE(stat.ok());
        stat = db_->Query(dummy_context_, TABLE_NAME, tags, k, 1100, 10, xq.data(), result_ids, result_distances);
        ASSERT_TRUE(stat.ok());
    }

#ifdef CUSTOMIZATION
    index.engine_type_ = (int)milvus::engine::EngineType::FAISS_IVFSQ8H;
    db_->CreateIndex(TABLE_NAME, index);  // wait until build index finish

    {
        std::vector<std::string> tags;
        milvus::engine::ResultIds result_ids;
        milvus::engine::ResultDistances result_distances;
        stat = db_->Query(dummy_context_, TABLE_NAME, tags, k, nq, 10, xq.data(), result_ids, result_distances);
        ASSERT_TRUE(stat.ok());
        stat = db_->Query(dummy_context_, TABLE_NAME, tags, k, 1100, 10, xq.data(), result_ids, result_distances);
        ASSERT_TRUE(stat.ok());
    }
#endif

    {  // search by specify index file
        milvus::engine::meta::DatesT dates;
        std::vector<std::string> file_ids;
        // sometimes this case run fast to merge file and build index, old file will be deleted immediately,
        // so the QueryByFileID cannot get files to search
        // input 100 files ids to avoid random failure of this case
        for (int i = 0; i < 100; i++) {
            file_ids.push_back(std::to_string(i));
        }
        milvus::engine::ResultIds result_ids;
        milvus::engine::ResultDistances result_distances;
        stat = db_->QueryByFileID(dummy_context_, TABLE_NAME, file_ids, k, nq, 10, xq.data(), dates, result_ids,
                                  result_distances);
        ASSERT_TRUE(stat.ok());
    }

    index.engine_type_ = (int)milvus::engine::EngineType::FAISS_PQ;
    db_->CreateIndex(TABLE_NAME, index);  // wait until build index finish

    {
        std::vector<std::string> tags;
        milvus::engine::ResultIds result_ids;
        milvus::engine::ResultDistances result_distances;
        stat = db_->Query(dummy_context_, TABLE_NAME, tags, k, nq, 10, xq.data(), result_ids, result_distances);
        ASSERT_TRUE(stat.ok());
        stat = db_->Query(dummy_context_, TABLE_NAME, tags, k, 1100, 10, xq.data(), result_ids, result_distances);
        ASSERT_TRUE(stat.ok());
    }

#ifdef CUSTOMIZATION
    // test FAISS_IVFSQ8H optimizer
    index.engine_type_ = (int)milvus::engine::EngineType::FAISS_IVFSQ8H;
    db_->CreateIndex(TABLE_NAME, index);  // wait until build index finish
    std::vector<std::string> partition_tag;
    milvus::engine::ResultIds result_ids;
    milvus::engine::ResultDistances result_dists;

    {
        result_ids.clear();
        result_dists.clear();
        stat = db_->Query(dummy_context_, TABLE_NAME, partition_tag, k, nq, 10, xq.data(), result_ids, result_dists);
        ASSERT_TRUE(stat.ok());
    }

    {
        result_ids.clear();
        result_dists.clear();
        stat = db_->Query(dummy_context_, TABLE_NAME, partition_tag, k, 200, 10, xq.data(), result_ids, result_dists);
        ASSERT_TRUE(stat.ok());
    }

    {  // search by specify index file
        milvus::engine::meta::DatesT dates;
        std::vector<std::string> file_ids;
        // sometimes this case run fast to merge file and build index, old file will be deleted immediately,
        // so the QueryByFileID cannot get files to search
        // input 100 files ids to avoid random failure of this case
        for (int i = 0; i < 100; i++) {
            file_ids.push_back(std::to_string(i));
        }
        result_ids.clear();
        result_dists.clear();
        stat = db_->QueryByFileID(dummy_context_, TABLE_NAME, file_ids, k, nq, 10, xq.data(), dates, result_ids,
                                  result_dists);
        ASSERT_TRUE(stat.ok());
    }

#endif
}

TEST_F(DBTest, PRELOADTABLE_TEST) {
    milvus::engine::meta::TableSchema table_info = BuildTableSchema();
    auto stat = db_->CreateTable(table_info);

    milvus::engine::meta::TableSchema table_info_get;
    table_info_get.table_id_ = TABLE_NAME;
    stat = db_->DescribeTable(table_info_get);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(table_info_get.dimension_, TABLE_DIM);

    int64_t nb = VECTOR_COUNT;
    std::vector<float> xb;
    BuildVectors(nb, xb);

    int loop = 5;
    for (auto i = 0; i < loop; ++i) {
        milvus::engine::IDNumbers vector_ids;
        db_->InsertVectors(TABLE_NAME, "", nb, xb.data(), vector_ids);
        ASSERT_EQ(vector_ids.size(), nb);
    }

    milvus::engine::TableIndex index;
    index.engine_type_ = (int)milvus::engine::EngineType::FAISS_IDMAP;
    db_->CreateIndex(TABLE_NAME, index);  // wait until build index finish

    int64_t prev_cache_usage = milvus::cache::CpuCacheMgr::GetInstance()->CacheUsage();
    stat = db_->PreloadTable(TABLE_NAME);
    ASSERT_TRUE(stat.ok());
    int64_t cur_cache_usage = milvus::cache::CpuCacheMgr::GetInstance()->CacheUsage();
    ASSERT_TRUE(prev_cache_usage < cur_cache_usage);
}

TEST_F(DBTest, SHUTDOWN_TEST) {
    db_->Stop();

    milvus::engine::meta::TableSchema table_info = BuildTableSchema();
    auto stat = db_->CreateTable(table_info);
    ASSERT_FALSE(stat.ok());

    stat = db_->DescribeTable(table_info);
    ASSERT_FALSE(stat.ok());

    bool has_table = false;
    stat = db_->HasTable(table_info.table_id_, has_table);
    ASSERT_FALSE(stat.ok());

    milvus::engine::IDNumbers ids;
    stat = db_->InsertVectors(table_info.table_id_, "", 0, nullptr, ids);
    ASSERT_FALSE(stat.ok());

    stat = db_->PreloadTable(table_info.table_id_);
    ASSERT_FALSE(stat.ok());

    uint64_t row_count = 0;
    stat = db_->GetTableRowCount(table_info.table_id_, row_count);
    ASSERT_FALSE(stat.ok());

    milvus::engine::TableIndex index;
    stat = db_->CreateIndex(table_info.table_id_, index);
    ASSERT_FALSE(stat.ok());

    stat = db_->DescribeIndex(table_info.table_id_, index);
    ASSERT_FALSE(stat.ok());

    std::vector<std::string> tags;
    milvus::engine::meta::DatesT dates;
    milvus::engine::ResultIds result_ids;
    milvus::engine::ResultDistances result_distances;
    stat =
        db_->Query(dummy_context_, table_info.table_id_, tags, 1, 1, 1, nullptr, dates, result_ids, result_distances);
    ASSERT_FALSE(stat.ok());
    std::vector<std::string> file_ids;
    stat = db_->QueryByFileID(dummy_context_, table_info.table_id_, file_ids, 1, 1, 1, nullptr, dates, result_ids,
                              result_distances);
    ASSERT_FALSE(stat.ok());

    stat = db_->DropTable(table_info.table_id_, dates);
    ASSERT_FALSE(stat.ok());
}

TEST_F(DBTest, INDEX_TEST) {
    milvus::engine::meta::TableSchema table_info = BuildTableSchema();
    auto stat = db_->CreateTable(table_info);

    int64_t nb = VECTOR_COUNT;
    std::vector<float> xb;
    BuildVectors(nb, xb);

    milvus::engine::IDNumbers vector_ids;
    db_->InsertVectors(TABLE_NAME, "", nb, xb.data(), vector_ids);
    ASSERT_EQ(vector_ids.size(), nb);

    milvus::engine::TableIndex index;
    index.engine_type_ = (int)milvus::engine::EngineType::FAISS_IVFSQ8;
    index.metric_type_ = (int)milvus::engine::MetricType::IP;
    stat = db_->CreateIndex(table_info.table_id_, index);
    ASSERT_TRUE(stat.ok());

    index.engine_type_ = (int)milvus::engine::EngineType::FAISS_IVFFLAT;
    stat = db_->CreateIndex(table_info.table_id_, index);
    ASSERT_TRUE(stat.ok());

#ifdef CUSTOMIZATION
#ifdef MILVUS_GPU_VERSION
    index.engine_type_ = (int)milvus::engine::EngineType::FAISS_IVFSQ8H;
    stat = db_->CreateIndex(table_info.table_id_, index);
    ASSERT_TRUE(stat.ok());
#endif
#endif

    milvus::engine::TableIndex index_out;
    stat = db_->DescribeIndex(table_info.table_id_, index_out);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(index.engine_type_, index_out.engine_type_);
    ASSERT_EQ(index.nlist_, index_out.nlist_);
    ASSERT_EQ(table_info.metric_type_, index_out.metric_type_);

    stat = db_->DropIndex(table_info.table_id_);
    ASSERT_TRUE(stat.ok());
}

TEST_F(DBTest, PARTITION_TEST) {
    milvus::engine::meta::TableSchema table_info = BuildTableSchema();
    auto stat = db_->CreateTable(table_info);
    ASSERT_TRUE(stat.ok());

    // create partition and insert data
    const int64_t PARTITION_COUNT = 5;
    const int64_t INSERT_BATCH = 2000;
    std::string table_name = TABLE_NAME;
    for (int64_t i = 0; i < PARTITION_COUNT; i++) {
        std::string partition_tag = std::to_string(i);
        std::string partition_name = table_name + "_" + partition_tag;
        stat = db_->CreatePartition(table_name, partition_name, partition_tag);
        ASSERT_TRUE(stat.ok());

        // not allow nested partition
        stat = db_->CreatePartition(partition_name, "dumy", "dummy");
        ASSERT_FALSE(stat.ok());

        // not allow duplicated partition
        stat = db_->CreatePartition(table_name, partition_name, partition_tag);
        ASSERT_FALSE(stat.ok());

        std::vector<float> xb;
        BuildVectors(INSERT_BATCH, xb);

        milvus::engine::IDNumbers vector_ids;
        vector_ids.resize(INSERT_BATCH);
        for (int64_t k = 0; k < INSERT_BATCH; k++) {
            vector_ids[k] = i * INSERT_BATCH + k;
        }

        db_->InsertVectors(table_name, partition_tag, INSERT_BATCH, xb.data(), vector_ids);
        ASSERT_EQ(vector_ids.size(), INSERT_BATCH);
    }

    // duplicated partition is not allowed
    stat = db_->CreatePartition(table_name, "", "0");
    ASSERT_FALSE(stat.ok());

    std::vector<milvus::engine::meta::TableSchema> partition_schema_array;
    stat = db_->ShowPartitions(table_name, partition_schema_array);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(partition_schema_array.size(), PARTITION_COUNT);
    for (int64_t i = 0; i < PARTITION_COUNT; i++) {
        ASSERT_EQ(partition_schema_array[i].table_id_, table_name + "_" + std::to_string(i));
    }

    {  // build index
        milvus::engine::TableIndex index;
        index.engine_type_ = (int)milvus::engine::EngineType::FAISS_IVFFLAT;
        index.metric_type_ = (int)milvus::engine::MetricType::L2;
        stat = db_->CreateIndex(table_info.table_id_, index);
        ASSERT_TRUE(stat.ok());

        uint64_t row_count = 0;
        stat = db_->GetTableRowCount(TABLE_NAME, row_count);
        ASSERT_TRUE(stat.ok());
        ASSERT_EQ(row_count, INSERT_BATCH * PARTITION_COUNT);
    }

    {  // search
        const int64_t nq = 5;
        const int64_t topk = 10;
        const int64_t nprobe = 10;
        std::vector<float> xq;
        BuildVectors(nq, xq);

        // specify partition tags
        std::vector<std::string> tags = {"0", std::to_string(PARTITION_COUNT - 1)};
        milvus::engine::ResultIds result_ids;
        milvus::engine::ResultDistances result_distances;
        stat = db_->Query(dummy_context_, TABLE_NAME, tags, topk, nq, nprobe, xq.data(), result_ids, result_distances);
        ASSERT_TRUE(stat.ok());
        ASSERT_EQ(result_ids.size() / topk, nq);

        // search in whole table
        tags.clear();
        result_ids.clear();
        result_distances.clear();
        stat = db_->Query(dummy_context_, TABLE_NAME, tags, topk, nq, nprobe, xq.data(), result_ids, result_distances);
        ASSERT_TRUE(stat.ok());
        ASSERT_EQ(result_ids.size() / topk, nq);

        // search in all partitions(tag regex match)
        tags.push_back("\\d");
        result_ids.clear();
        result_distances.clear();
        stat = db_->Query(dummy_context_, TABLE_NAME, tags, topk, nq, nprobe, xq.data(), result_ids, result_distances);
        ASSERT_TRUE(stat.ok());
        ASSERT_EQ(result_ids.size() / topk, nq);
    }

    stat = db_->DropPartition(table_name + "_0");
    ASSERT_TRUE(stat.ok());

    stat = db_->DropPartitionByTag(table_name, "1");
    ASSERT_TRUE(stat.ok());

    stat = db_->DropIndex(table_name);
    ASSERT_TRUE(stat.ok());

    milvus::engine::meta::DatesT dates;
    stat = db_->DropTable(table_name, dates);
    ASSERT_TRUE(stat.ok());
}

TEST_F(DBTest2, ARHIVE_DISK_CHECK) {
    milvus::engine::meta::TableSchema table_info = BuildTableSchema();
    auto stat = db_->CreateTable(table_info);

    std::vector<milvus::engine::meta::TableSchema> table_schema_array;
    stat = db_->AllTables(table_schema_array);
    ASSERT_TRUE(stat.ok());
    bool bfound = false;
    for (auto& schema : table_schema_array) {
        if (schema.table_id_ == TABLE_NAME) {
            bfound = true;
            break;
        }
    }
    ASSERT_TRUE(bfound);

    milvus::engine::meta::TableSchema table_info_get;
    table_info_get.table_id_ = TABLE_NAME;
    stat = db_->DescribeTable(table_info_get);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(table_info_get.dimension_, TABLE_DIM);

    uint64_t size;
    db_->Size(size);

    int64_t nb = 10;
    std::vector<float> xb;
    BuildVectors(nb, xb);

    int loop = INSERT_LOOP;
    for (auto i = 0; i < loop; ++i) {
        milvus::engine::IDNumbers vector_ids;
        db_->InsertVectors(TABLE_NAME, "", nb, xb.data(), vector_ids);
        std::this_thread::sleep_for(std::chrono::microseconds(1));
    }

    std::this_thread::sleep_for(std::chrono::seconds(1));

    db_->Size(size);
    LOG(DEBUG) << "size=" << size;
    ASSERT_LE(size, 1 * milvus::engine::G);
}

TEST_F(DBTest2, DELETE_TEST) {
    milvus::engine::meta::TableSchema table_info = BuildTableSchema();
    auto stat = db_->CreateTable(table_info);

    milvus::engine::meta::TableSchema table_info_get;
    table_info_get.table_id_ = TABLE_NAME;
    stat = db_->DescribeTable(table_info_get);
    ASSERT_TRUE(stat.ok());

    bool has_table = false;
    db_->HasTable(TABLE_NAME, has_table);
    ASSERT_TRUE(has_table);

    uint64_t size;
    db_->Size(size);

    int64_t nb = VECTOR_COUNT;
    std::vector<float> xb;
    BuildVectors(nb, xb);

    milvus::engine::IDNumbers vector_ids;
    stat = db_->InsertVectors(TABLE_NAME, "", nb, xb.data(), vector_ids);
    milvus::engine::TableIndex index;
    stat = db_->CreateIndex(TABLE_NAME, index);

    std::vector<milvus::engine::meta::DateT> dates;
    stat = db_->DropTable(TABLE_NAME, dates);
    std::this_thread::sleep_for(std::chrono::seconds(2));
    ASSERT_TRUE(stat.ok());

    db_->HasTable(TABLE_NAME, has_table);
    ASSERT_FALSE(has_table);
}

TEST_F(DBTest2, DELETE_BY_RANGE_TEST) {
    milvus::engine::meta::TableSchema table_info = BuildTableSchema();
    auto stat = db_->CreateTable(table_info);

    milvus::engine::meta::TableSchema table_info_get;
    table_info_get.table_id_ = TABLE_NAME;
    stat = db_->DescribeTable(table_info_get);
    ASSERT_TRUE(stat.ok());

    bool has_table = false;
    db_->HasTable(TABLE_NAME, has_table);
    ASSERT_TRUE(has_table);

    uint64_t size;
    db_->Size(size);
    ASSERT_EQ(size, 0UL);

    int64_t nb = VECTOR_COUNT;
    std::vector<float> xb;
    BuildVectors(nb, xb);

    milvus::engine::IDNumbers vector_ids;
    stat = db_->InsertVectors(TABLE_NAME, "", nb, xb.data(), vector_ids);
    milvus::engine::TableIndex index;
    stat = db_->CreateIndex(TABLE_NAME, index);

    db_->Size(size);
    ASSERT_NE(size, 0UL);

    std::vector<milvus::engine::meta::DateT> dates;
    std::string start_value = CurrentTmDate(-5);
    std::string end_value = CurrentTmDate(5);
    ConvertTimeRangeToDBDates(start_value, end_value, dates);

    stat = db_->DropTable(TABLE_NAME, dates);
    ASSERT_TRUE(stat.ok());

    uint64_t row_count = 0;
    db_->GetTableRowCount(TABLE_NAME, row_count);
    ASSERT_EQ(row_count, 0UL);
}
