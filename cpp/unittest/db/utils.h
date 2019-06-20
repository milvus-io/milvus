////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <gtest/gtest.h>
#include <chrono>
//#include <src/db/MySQLMetaImpl.h>

#include "db/DB.h"
#include "db/DBMetaImpl.h"
#include "db/MySQLMetaImpl.h"


#define TIMING

#ifdef TIMING
#define INIT_TIMER auto start = std::chrono::high_resolution_clock::now();
#define START_TIMER  start = std::chrono::high_resolution_clock::now();
#define STOP_TIMER(name)  LOG(DEBUG) << "RUNTIME of " << name << ": " << \
    std::chrono::duration_cast<std::chrono::milliseconds>( \
            std::chrono::high_resolution_clock::now()-start \
    ).count() << " ms ";
#else
#define INIT_TIMER
#define START_TIMER
#define STOP_TIMER(name)
#endif


void ASSERT_STATS(zilliz::milvus::engine::Status& stat);


class DBTest : public ::testing::Test {
protected:
    zilliz::milvus::engine::DB* db_;

    void InitLog();
    virtual void SetUp() override;
    virtual void TearDown() override;
    virtual zilliz::milvus::engine::Options GetOptions();
};

class DBTest2 : public DBTest {
protected:
    virtual zilliz::milvus::engine::Options GetOptions() override;
};


class MetaTest : public DBTest {
protected:
    std::shared_ptr<zilliz::milvus::engine::meta::DBMetaImpl> impl_;

    virtual void SetUp() override;
    virtual void TearDown() override;
};

class MySQLTest : public ::testing::Test {
protected:
//    std::shared_ptr<zilliz::milvus::engine::meta::MySQLMetaImpl> impl_;
    zilliz::milvus::engine::DBMetaOptions getDBMetaOptions();
};

class MySQLDBTest : public  ::testing::Test {

};
