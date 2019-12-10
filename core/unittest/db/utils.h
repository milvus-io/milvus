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
#include <memory>

#include "db/DB.h"
#include "db/meta/MySQLMetaImpl.h"
#include "db/meta/SqliteMetaImpl.h"
#include "scheduler/ResourceFactory.h"
#include "scheduler/SchedInst.h"

#define TIMING

#ifdef TIMING
#define INIT_TIMER auto start = std::chrono::high_resolution_clock::now();
#define START_TIMER start = std::chrono::high_resolution_clock::now();
#define STOP_TIMER(name)                                                                                            \
    LOG(DEBUG) << "RUNTIME of " << name << ": "                                                                     \
               << std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - \
                                                                        start)                                      \
                      .count()                                                                                      \
               << " ms ";
#else
#define INIT_TIMER
#define START_TIMER
#define STOP_TIMER(name)
#endif

static const char* CONFIG_PATH = "/tmp/milvus_test";
static const char* CONFIG_FILE = "/server_config.yaml";

class BaseTest : public ::testing::Test {
 protected:
    void
    InitLog();

    void
    SetUp() override;
    void
    TearDown() override;
    virtual milvus::engine::DBOptions
    GetOptions();

    std::shared_ptr<milvus::server::Context> dummy_context_;
};

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class DBTest : public BaseTest {
 protected:
    milvus::engine::DBPtr db_;

    void
    SetUp() override;
    void
    TearDown() override;
};

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class DBTest2 : public DBTest {
 protected:
    milvus::engine::DBOptions
    GetOptions() override;
};

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class EngineTest : public DBTest {};

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class MetaTest : public BaseTest {
 protected:
    std::shared_ptr<milvus::engine::meta::SqliteMetaImpl> impl_;

    void
    SetUp() override;
    void
    TearDown() override;
};

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class MySqlDBTest : public DBTest {
 protected:
    milvus::engine::DBOptions
    GetOptions() override;
};

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class MySqlMetaTest : public BaseTest {
 protected:
    std::shared_ptr<milvus::engine::meta::MySQLMetaImpl> impl_;

    void
    SetUp() override;
    void
    TearDown() override;
    milvus::engine::DBOptions
    GetOptions() override;
};

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class MemManagerTest : public MetaTest {};

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class MemManagerTest2 : public DBTest {};
