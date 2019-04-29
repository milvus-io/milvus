////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <gtest/gtest.h>
#include <chrono>

#include "db/DB.h"
#include "db/DBMetaImpl.h"


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


void ASSERT_STATS(zilliz::vecwise::engine::Status& stat);


class DBTest : public ::testing::Test {
protected:
    zilliz::vecwise::engine::DB* db_;

    void InitLog();
    virtual void SetUp() override;
    virtual void TearDown() override;
};


class MetaTest : public DBTest {
protected:
    std::shared_ptr<zilliz::vecwise::engine::meta::DBMetaImpl> impl_;

    virtual void SetUp() override;
    virtual void TearDown() override;
};
