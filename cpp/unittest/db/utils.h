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
    virtual void TearDown() override;
    virtual zilliz::milvus::engine::Options GetOptions();
};

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class DBTest : public BaseTest {
 protected:
    zilliz::milvus::engine::DBPtr db_;

    virtual void SetUp() override;
    virtual void TearDown() override;
};

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class DBTest2 : public DBTest {
 protected:
    virtual zilliz::milvus::engine::Options GetOptions() override;
};

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class EngineTest : public DBTest {
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
class MemManagerTest : public MetaTest {
};

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class MemManagerTest2 : public DBTest {
};
