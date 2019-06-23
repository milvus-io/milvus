////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include <gtest/gtest.h>
#include <thread>
#include <easylogging++.h>
#include <boost/filesystem.hpp>

#include "utils.h"
#include "db/DB.h"
#include "db/DBImpl.h"
#include "db/MetaConsts.h"
#include "db/Factories.h"

using namespace zilliz::milvus;

namespace {

    static const std::string TABLE_NAME = "test_group";
    static constexpr int64_t TABLE_DIM = 256;

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
        ASSERT_TRUE(criterias.size() == 1);
        ASSERT_TRUE(criterias["disk"] == 512);
    }
    {
        engine::ArchiveConf conf("swap");
        ASSERT_EQ(conf.GetType(), "swap");
        auto criterias = conf.GetCriterias();
        ASSERT_TRUE(criterias.size() == 1);
        ASSERT_TRUE(criterias["disk"] == 512);
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
    static const std::string table_name = "test_group";
    static const int table_dim = 256;

    engine::meta::TableSchema table_info;
    table_info.dimension_ = table_dim;
    table_info.table_id_ = table_name;
    table_info.engine_type_ = (int)engine::EngineType::FAISS_IDMAP;
    engine::Status stat = db_->CreateTable(table_info);

    engine::meta::TableSchema table_info_get;
    table_info_get.table_id_ = table_name;
    stat = db_->DescribeTable(table_info_get);
    ASSERT_STATS(stat);
    ASSERT_EQ(table_info_get.dimension_, table_dim);

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
            stat = db_->Query(table_name, k, qb, qxb.data(), results);
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

    int loop = 100000;

    for (auto i=0; i<loop; ++i) {
        if (i==40) {
            db_->InsertVectors(table_name, qb, qxb.data(), target_ids);
            ASSERT_EQ(target_ids.size(), qb);
        } else {
            db_->InsertVectors(table_name, nb, xb.data(), vector_ids);
        }
        std::this_thread::sleep_for(std::chrono::microseconds(1));
    }

    search.join();
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
    size_t nb = 250000;
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

    sleep(2); // wait until build index finish

    engine::QueryResults results;
    stat = db_->Query(TABLE_NAME, k, nq, xq.data(), results);
    ASSERT_STATS(stat);

    // TODO(linxj): add groundTruth assert
};

TEST_F(DBTest2, ARHIVE_DISK_CHECK) {

    engine::meta::TableSchema table_info = BuildTableSchema();
    engine::Status stat = db_->CreateTable(table_info);

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

    int loop = 100000;
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

    ASSERT_TRUE(boost::filesystem::exists(table_info_get.location_));

    engine::IDNumbers vector_ids;

    uint64_t size;
    db_->Size(size);

    int64_t nb = 100000;
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
    ASSERT_FALSE(boost::filesystem::exists(table_info_get.location_));

    stat = db_->DropAll();
    ASSERT_TRUE(stat.ok());
};

TEST_F(MySQLDBTest, DB_TEST) {

    auto options = GetOptions();
    auto db_ = engine::DBFactory::Build(options);

    static const std::string table_name = "test_group";
    static const int table_dim = 256;

    engine::meta::TableSchema table_info;
    table_info.dimension_ = table_dim;
    table_info.table_id_ = table_name;
    table_info.engine_type_ = (int)engine::EngineType::FAISS_IDMAP;
    engine::Status stat = db_->CreateTable(table_info);

    engine::meta::TableSchema table_info_get;
    table_info_get.table_id_ = table_name;
    stat = db_->DescribeTable(table_info_get);
    ASSERT_STATS(stat);
    ASSERT_EQ(table_info_get.dimension_, table_dim);

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
            stat = db_->Query(table_name, k, qb, qxb.data(), results);
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

    int loop = 100000;

    for (auto i=0; i<loop; ++i) {
        if (i==40) {
            db_->InsertVectors(table_name, qb, qxb.data(), target_ids);
            ASSERT_EQ(target_ids.size(), qb);
        } else {
            db_->InsertVectors(table_name, nb, xb.data(), vector_ids);
        }
        std::this_thread::sleep_for(std::chrono::microseconds(1));
    }

    search.join();

    stat = db_->DropAll();
    ASSERT_TRUE(stat.ok());
};

TEST_F(MySQLDBTest, SEARCH_TEST) {
    auto options = GetOptions();
    auto db_ = engine::DBFactory::Build(options);

    engine::meta::TableSchema table_info = BuildTableSchema();
    engine::Status stat = db_->CreateTable(table_info);

    engine::meta::TableSchema table_info_get;
    table_info_get.table_id_ = TABLE_NAME;
    stat = db_->DescribeTable(table_info_get);
    ASSERT_STATS(stat);
    ASSERT_EQ(table_info_get.dimension_, TABLE_DIM);

    // prepare raw data
    size_t nb = 250000;
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

    sleep(2); // wait until build index finish

    engine::QueryResults results;
    stat = db_->Query(TABLE_NAME, k, nq, xq.data(), results);
    ASSERT_STATS(stat);

    stat = db_->DropAll();
    ASSERT_TRUE(stat.ok());

    // TODO(linxj): add groundTruth assert
};

TEST_F(MySQLDBTest, ARHIVE_DISK_CHECK) {

    auto options = GetOptions();
    options.meta.archive_conf = engine::ArchiveConf("delete", "disk:1");
    auto db_ = engine::DBFactory::Build(options);

    engine::meta::TableSchema table_info = BuildTableSchema();
    engine::Status stat = db_->CreateTable(table_info);

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

    int loop = 100000;
    for (auto i=0; i<loop; ++i) {
        db_->InsertVectors(TABLE_NAME, nb, xb.data(), vector_ids);
        std::this_thread::sleep_for(std::chrono::microseconds(1));
    }

    std::this_thread::sleep_for(std::chrono::seconds(10)); //change to 10 to make sure files are discarded

    db_->Size(size);
    LOG(DEBUG) << "size=" << size;
    ASSERT_LE(size, 1 * engine::meta::G);

    stat = db_->DropAll();
    ASSERT_TRUE(stat.ok());
};

TEST_F(MySQLDBTest, DELETE_TEST) {

    auto options = GetOptions();
    options.meta.archive_conf = engine::ArchiveConf("delete", "disk:1");
    auto db_ = engine::DBFactory::Build(options);

    engine::meta::TableSchema table_info = BuildTableSchema();
    engine::Status stat = db_->CreateTable(table_info);

    engine::meta::TableSchema table_info_get;
    table_info_get.table_id_ = TABLE_NAME;
    stat = db_->DescribeTable(table_info_get);
    ASSERT_STATS(stat);

    ASSERT_TRUE(boost::filesystem::exists(table_info_get.location_));

    engine::IDNumbers vector_ids;

    uint64_t size;
    db_->Size(size);

    int64_t nb = 100000;
    std::vector<float> xb;
    BuildVectors(nb, xb);

    int loop = 20;
    for (auto i=0; i<loop; ++i) {
        db_->InsertVectors(TABLE_NAME, nb, xb.data(), vector_ids);
        std::this_thread::sleep_for(std::chrono::microseconds(1));
    }

    std::vector<engine::meta::DateT> dates;
    stat = db_->DeleteTable(TABLE_NAME, dates);

    std::this_thread::sleep_for(std::chrono::seconds(10)); //change to 10 to make sure files are discarded

    ASSERT_TRUE(stat.ok());
//    std::cout << table_info_get.location_ << std::endl;
    ASSERT_FALSE(boost::filesystem::exists(table_info_get.location_));

    stat = db_->DropAll();
    ASSERT_TRUE(stat.ok());
};
