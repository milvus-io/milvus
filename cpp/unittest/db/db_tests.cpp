////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include "utils.h"
#include "db/DB.h"
#include "db/DBImpl.h"
#include "db/meta/MetaConsts.h"
#include "db/Factories.h"
#include "cache/CpuCacheMgr.h"
#include "utils/CommonUtil.h"

#include <gtest/gtest.h>
#include <easylogging++.h>

#include <boost/filesystem.hpp>

#include <thread>
#include <random>

using namespace zilliz::milvus;

namespace {

    static const char* TABLE_NAME = "test_group";
    static constexpr int64_t TABLE_DIM = 256;
    static constexpr int64_t VECTOR_COUNT = 250000;
    static constexpr int64_t INSERT_LOOP = 10000;
    static constexpr int64_t SECONDS_EACH_HOUR = 3600;
    static constexpr int64_t DAY_SECONDS = 24 * 60 * 60;

    engine::meta::TableSchema BuildTableSchema() {
        engine::meta::TableSchema table_info;
        table_info.dimension_ = TABLE_DIM;
        table_info.table_id_ = TABLE_NAME;
        table_info.engine_type_ = (int)engine::EngineType::FAISS_IDMAP;
        return table_info;
    }

    void BuildVectors(int64_t n, std::vector<float>& vectors) {
        vectors.clear();
        vectors.resize(n*TABLE_DIM);
        float* data = vectors.data();
        for(int i = 0; i < n; i++) {
            for(int j = 0; j < TABLE_DIM; j++) data[TABLE_DIM * i + j] = drand48();
            data[TABLE_DIM * i] += i / 2000.;
        }
    }

    std::string CurrentTmDate(int64_t offset_day = 0) {
        time_t tt;
        time( &tt );
        tt = tt + 8*SECONDS_EACH_HOUR;
        tt = tt + 24*SECONDS_EACH_HOUR*offset_day;
        tm* t= gmtime( &tt );

        std::string str = std::to_string(t->tm_year + 1900) + "-" + std::to_string(t->tm_mon + 1)
                          + "-" + std::to_string(t->tm_mday);

        return str;
    }

    void
    ConvertTimeRangeToDBDates(const std::string &start_value,
                              const std::string &end_value,
                              std::vector<engine::meta::DateT > &dates) {
        dates.clear();

        time_t tt_start, tt_end;
        tm tm_start, tm_end;
        if (!zilliz::milvus::server::CommonUtil::TimeStrToTime(start_value, tt_start, tm_start)) {
            return;
        }

        if (!zilliz::milvus::server::CommonUtil::TimeStrToTime(end_value, tt_end, tm_end)) {
            return;
        }

        long days = (tt_end > tt_start) ? (tt_end - tt_start) / DAY_SECONDS : (tt_start - tt_end) /
                                                                              DAY_SECONDS;
        if (days == 0) {
            return;
        }

        for (long i = 0; i < days; i++) {
            time_t tt_day = tt_start + DAY_SECONDS * i;
            tm tm_day;
            zilliz::milvus::server::CommonUtil::ConvertTime(tt_day, tm_day);

            long date = tm_day.tm_year * 10000 + tm_day.tm_mon * 100 +
                        tm_day.tm_mday;//according to db logic
            dates.push_back(date);
        }
    }

}

TEST_F(DBTest, CONFIG_TEST) {
    {
        ASSERT_ANY_THROW(engine::ArchiveConf conf("wrong"));
        /* EXPECT_DEATH(engine::ArchiveConf conf("wrong"), ""); */
    }
    {
        engine::ArchiveConf conf("delete");
        ASSERT_EQ(conf.GetType(), "delete");
        auto criterias = conf.GetCriterias();
        ASSERT_TRUE(criterias.size() == 0);
    }
    {
        engine::ArchiveConf conf("swap");
        ASSERT_EQ(conf.GetType(), "swap");
        auto criterias = conf.GetCriterias();
        ASSERT_TRUE(criterias.size() == 0);
    }
    {
        ASSERT_ANY_THROW(engine::ArchiveConf conf1("swap", "disk:"));
        ASSERT_ANY_THROW(engine::ArchiveConf conf2("swap", "disk:a"));
        engine::ArchiveConf conf("swap", "disk:1024");
        auto criterias = conf.GetCriterias();
        ASSERT_TRUE(criterias.size() == 1);
        ASSERT_TRUE(criterias["disk"] == 1024);
    }
    {
        ASSERT_ANY_THROW(engine::ArchiveConf conf1("swap", "days:"));
        ASSERT_ANY_THROW(engine::ArchiveConf conf2("swap", "days:a"));
        engine::ArchiveConf conf("swap", "days:100");
        auto criterias = conf.GetCriterias();
        ASSERT_TRUE(criterias.size() == 1);
        ASSERT_TRUE(criterias["days"] == 100);
    }
    {
        ASSERT_ANY_THROW(engine::ArchiveConf conf1("swap", "days:"));
        ASSERT_ANY_THROW(engine::ArchiveConf conf2("swap", "days:a"));
        engine::ArchiveConf conf("swap", "days:100;disk:200");
        auto criterias = conf.GetCriterias();
        ASSERT_TRUE(criterias.size() == 2);
        ASSERT_TRUE(criterias["days"] == 100);
        ASSERT_TRUE(criterias["disk"] == 200);
    }
}


TEST_F(DBTest, DB_TEST) {
    db_->Open(GetOptions(), &db_);
    engine::meta::TableSchema table_info = BuildTableSchema();
    engine::Status stat = db_->CreateTable(table_info);

    engine::meta::TableSchema table_info_get;
    table_info_get.table_id_ = TABLE_NAME;
    stat = db_->DescribeTable(table_info_get);
    ASSERT_STATS(stat);
    ASSERT_EQ(table_info_get.dimension_, TABLE_DIM);

    engine::IDNumbers vector_ids;
    engine::IDNumbers target_ids;

    int64_t nb = 50;
    std::vector<float> xb;
    BuildVectors(nb, xb);

    int64_t qb = 5;
    std::vector<float> qxb;
    BuildVectors(qb, qxb);

    std::thread search([&]() {
        engine::QueryResults results;
        int k = 10;
        std::this_thread::sleep_for(std::chrono::seconds(2));

        INIT_TIMER;
        std::stringstream ss;
        uint64_t count = 0;
        uint64_t prev_count = 0;

        for (auto j=0; j<10; ++j) {
            ss.str("");
            db_->Size(count);
            prev_count = count;

            START_TIMER;
            stat = db_->Query(TABLE_NAME, k, qb, 10, qxb.data(), results);
            ss << "Search " << j << " With Size " << count/engine::meta::M << " M";
            STOP_TIMER(ss.str());

            ASSERT_STATS(stat);
            for (auto k=0; k<qb; ++k) {
                ASSERT_EQ(results[k][0].first, target_ids[k]);
                ss.str("");
                ss << "Result [" << k << "]:";
                for (auto result : results[k]) {
                    ss << result.first << " ";
                }
                /* LOG(DEBUG) << ss.str(); */
            }
            ASSERT_TRUE(count >= prev_count);
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    });

    int loop = INSERT_LOOP;

    for (auto i=0; i<loop; ++i) {
        if (i==40) {
            db_->InsertVectors(TABLE_NAME, qb, qxb.data(), target_ids);
            ASSERT_EQ(target_ids.size(), qb);
        } else {
            db_->InsertVectors(TABLE_NAME, nb, xb.data(), vector_ids);
        }
        std::this_thread::sleep_for(std::chrono::microseconds(1));
    }

    search.join();

    uint64_t count;
    stat = db_->GetTableRowCount(TABLE_NAME, count);
    ASSERT_STATS(stat);
    ASSERT_TRUE(count > 0);
};

TEST_F(DBTest, SEARCH_TEST) {
    engine::meta::TableSchema table_info = BuildTableSchema();
    engine::Status stat = db_->CreateTable(table_info);

    engine::meta::TableSchema table_info_get;
    table_info_get.table_id_ = TABLE_NAME;
    stat = db_->DescribeTable(table_info_get);
    ASSERT_STATS(stat);
    ASSERT_EQ(table_info_get.dimension_, TABLE_DIM);

    // prepare raw data
    size_t nb = VECTOR_COUNT;
    size_t nq = 10;
    size_t k = 5;
    std::vector<float> xb(nb*TABLE_DIM);
    std::vector<float> xq(nq*TABLE_DIM);
    std::vector<long> ids(nb);

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis_xt(-1.0, 1.0);
    for (size_t i = 0; i < nb*TABLE_DIM; i++) {
        xb[i] = dis_xt(gen);
        if (i < nb){
            ids[i] = i;
        }
    }
    for (size_t i = 0; i < nq*TABLE_DIM; i++) {
        xq[i] = dis_xt(gen);
    }

    // result data
    //std::vector<long> nns_gt(k*nq);
    std::vector<long> nns(k*nq);  // nns = nearst neg search
    //std::vector<float> dis_gt(k*nq);
    std::vector<float> dis(k*nq);

    // insert data
    const int batch_size = 100;
    for (int j = 0; j < nb / batch_size; ++j) {
        stat = db_->InsertVectors(TABLE_NAME, batch_size, xb.data()+batch_size*j*TABLE_DIM, ids);
        if (j == 200){ sleep(1);}
        ASSERT_STATS(stat);
    }

    db_->BuildIndex(TABLE_NAME); // wait until build index finish

    {
        engine::QueryResults results;
        stat = db_->Query(TABLE_NAME, k, nq, 10, xq.data(), results);
        ASSERT_STATS(stat);
    }

    {//search by specify index file
        engine::meta::DatesT dates;
        std::vector<std::string> file_ids = {"4", "5", "6"};
        engine::QueryResults results;
        stat = db_->Query(TABLE_NAME, file_ids, k, nq, 10, xq.data(), dates, results);
        ASSERT_STATS(stat);
    }

    // TODO(linxj): add groundTruth assert
};

TEST_F(DBTest, PRELOADTABLE_TEST) {
    engine::meta::TableSchema table_info = BuildTableSchema();
    engine::Status stat = db_->CreateTable(table_info);

    engine::meta::TableSchema table_info_get;
    table_info_get.table_id_ = TABLE_NAME;
    stat = db_->DescribeTable(table_info_get);
    ASSERT_STATS(stat);
    ASSERT_EQ(table_info_get.dimension_, TABLE_DIM);

    engine::IDNumbers vector_ids;
    engine::IDNumbers target_ids;

    int64_t nb = 100000;
    std::vector<float> xb;
    BuildVectors(nb, xb);

    int loop = 5;

    for (auto i=0; i<loop; ++i) {
        db_->InsertVectors(TABLE_NAME, nb, xb.data(), target_ids);
        ASSERT_EQ(target_ids.size(), nb);
    }
    db_->BuildIndex(TABLE_NAME);

    int64_t prev_cache_usage = cache::CpuCacheMgr::GetInstance()->CacheUsage();
    stat = db_->PreloadTable(TABLE_NAME);
    ASSERT_STATS(stat);
    int64_t cur_cache_usage = cache::CpuCacheMgr::GetInstance()->CacheUsage();
    ASSERT_TRUE(prev_cache_usage < cur_cache_usage);

}

TEST_F(DBTest2, ARHIVE_DISK_CHECK) {

    engine::meta::TableSchema table_info = BuildTableSchema();
    engine::Status stat = db_->CreateTable(table_info);

    std::vector<engine::meta::TableSchema> table_schema_array;
    stat = db_->AllTables(table_schema_array);
    ASSERT_STATS(stat);
    bool bfound = false;
    for(auto& schema : table_schema_array) {
        if(schema.table_id_ == TABLE_NAME) {
            bfound = true;
            break;
        }
    }
    ASSERT_TRUE(bfound);

    engine::meta::TableSchema table_info_get;
    table_info_get.table_id_ = TABLE_NAME;
    stat = db_->DescribeTable(table_info_get);
    ASSERT_STATS(stat);
    ASSERT_EQ(table_info_get.dimension_, TABLE_DIM);

    engine::IDNumbers vector_ids;
    engine::IDNumbers target_ids;

    uint64_t size;
    db_->Size(size);

    int64_t nb = 10;
    std::vector<float> xb;
    BuildVectors(nb, xb);

    int loop = INSERT_LOOP;
    for (auto i=0; i<loop; ++i) {
        db_->InsertVectors(TABLE_NAME, nb, xb.data(), vector_ids);
        std::this_thread::sleep_for(std::chrono::microseconds(1));
    }

    std::this_thread::sleep_for(std::chrono::seconds(1));

    db_->Size(size);
    LOG(DEBUG) << "size=" << size;
    ASSERT_LE(size, 1 * engine::meta::G);
};

TEST_F(DBTest2, DELETE_TEST) {
    engine::meta::TableSchema table_info = BuildTableSchema();
    engine::Status stat = db_->CreateTable(table_info);

    engine::meta::TableSchema table_info_get;
    table_info_get.table_id_ = TABLE_NAME;
    stat = db_->DescribeTable(table_info_get);
    ASSERT_STATS(stat);

    bool has_table = false;
    db_->HasTable(TABLE_NAME, has_table);
    ASSERT_TRUE(has_table);

    engine::IDNumbers vector_ids;

    uint64_t size;
    db_->Size(size);

    int64_t nb = INSERT_LOOP;
    std::vector<float> xb;
    BuildVectors(nb, xb);

    int loop = 20;
    for (auto i=0; i<loop; ++i) {
        db_->InsertVectors(TABLE_NAME, nb, xb.data(), vector_ids);
        std::this_thread::sleep_for(std::chrono::microseconds(1));
    }

    std::vector<engine::meta::DateT> dates;
    stat = db_->DeleteTable(TABLE_NAME, dates);
    std::this_thread::sleep_for(std::chrono::seconds(2));
    ASSERT_TRUE(stat.ok());

    db_->HasTable(TABLE_NAME, has_table);
    ASSERT_FALSE(has_table);
};

TEST_F(DBTest2, DELETE_BY_RANGE_TEST) {
    auto options = engine::OptionsFactory::Build();
    options.meta.path = "/tmp/milvus_test";
    options.meta.backend_uri = "sqlite://:@:/";
    auto db_ = engine::DBFactory::Build(options);

    engine::meta::TableSchema table_info = BuildTableSchema();
    engine::Status stat = db_->CreateTable(table_info);

    engine::meta::TableSchema table_info_get;
    table_info_get.table_id_ = TABLE_NAME;
    stat = db_->DescribeTable(table_info_get);
    ASSERT_STATS(stat);

    bool has_table = false;
    db_->HasTable(TABLE_NAME, has_table);
    ASSERT_TRUE(has_table);

    engine::IDNumbers vector_ids;

    uint64_t size;
    db_->Size(size);

    int64_t nb = INSERT_LOOP;
    std::vector<float> xb;
    BuildVectors(nb, xb);

    int loop = 20;
    for (auto i=0; i<loop; ++i) {
        db_->InsertVectors(TABLE_NAME, nb, xb.data(), vector_ids);
        std::this_thread::sleep_for(std::chrono::microseconds(1));
    }

    std::vector<engine::meta::DateT> dates;
    engine::meta::DateT date;
    std::string start_value = CurrentTmDate(-3);
    std::string end_value = CurrentTmDate(-2);
    ConvertTimeRangeToDBDates(start_value, end_value, dates);

    db_->DeleteTable(TABLE_NAME, dates);
}