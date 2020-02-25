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

#pragma once

#include <gtest/gtest.h>
#include <chrono>
//#include <src/db/MySQLMetaImpl.h>

#include "db/DB.h"
#include "db/meta/MySQLMetaImpl.h"
#include "db/meta/SqliteMetaImpl.h"
#include <fiu-control.h>
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

#ifdef FIU_ENABLE
#define FIU_ENABLE_FIU(name) fiu_enable(name, 1, nullptr, 0)
#endif

void
ASSERT_STATS(milvus::Status& stat);

// class TestEnv : public ::testing::Environment {
// public:
//
//    static std::string getURI() {
//        if (const char* uri = std::getenv("MILVUS_DBMETA_URI")) {
//            return uri;
//        }
//        else {
//            return "";
//        }
//    }
//
//    void SetUp() override {
//        getURI();
//    }
//
//};
//
//::testing::Environment* const test_env =
//        ::testing::AddGlobalTestEnvironment(new TestEnv);

class MetricTest : public ::testing::Test {
 protected:
    milvus::engine::DBPtr db_;

    void
    InitLog();
    void
    SetUp() override;
    void
    TearDown() override;
    virtual milvus::engine::DBOptions
    GetOptions();
};
