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
#include "db/meta/SqliteMetaImpl.h"
#include "db/meta/MySQLMetaImpl.h"
#include "scheduler/SchedInst.h"
#include "scheduler/ResourceFactory.h"


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

class BaseTest : public ::testing::Test {
protected:
    void InitLog();

    virtual void SetUp() override;
    virtual zilliz::milvus::engine::Options GetOptions();
};

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class DBTest : public BaseTest {
 protected:
    zilliz::milvus::engine::DB *db_;

    virtual void SetUp() override;
    virtual void TearDown() override;
};

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class DBTest2 : public DBTest {
 protected:
    virtual zilliz::milvus::engine::Options GetOptions() override;
};

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class MetaTest : public BaseTest {
 protected:
    std::shared_ptr<zilliz::milvus::engine::meta::SqliteMetaImpl> impl_;

    virtual void SetUp() override;
    virtual void TearDown() override;
};

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class MySqlDBTest : public DBTest {
protected:
    zilliz::milvus::engine::Options GetOptions();
};

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class MySqlMetaTest : public BaseTest {
 protected:
    std::shared_ptr<zilliz::milvus::engine::meta::MySQLMetaImpl> impl_;

    virtual void SetUp() override;
    virtual void TearDown() override;
    zilliz::milvus::engine::Options GetOptions();
};


/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class MemManagerTest : public BaseTest {
};

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class MemManagerTest2 : public DBTest {
};
