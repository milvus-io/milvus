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

#include "db/Options.h"
#include "db/meta/SqliteMetaImpl.h"
#include "db/engine/EngineFactory.h"
#include "db/Utils.h"
#include "utils/Status.h"
#include "utils/Exception.h"

#include <gtest/gtest.h>
#include <thread>
#include <boost/filesystem.hpp>
#include <vector>

TEST(DBMiscTest, EXCEPTION_TEST) {
    milvus::Exception ex1(100, "error");
    std::string what = ex1.what();
    ASSERT_EQ(what, "error");
    ASSERT_EQ(ex1.code(), 100);

    milvus::InvalidArgumentException ex2;
    ASSERT_EQ(ex2.code(), milvus::SERVER_INVALID_ARGUMENT);
}

TEST(DBMiscTest, OPTIONS_TEST) {
    try {
        milvus::engine::ArchiveConf archive("$$##");
    } catch (std::exception &ex) {
        ASSERT_TRUE(true);
    }

    {
        milvus::engine::ArchiveConf archive("delete", "no");
        ASSERT_TRUE(archive.GetCriterias().empty());
    }

    {
        milvus::engine::ArchiveConf archive("delete", "1:2");
        ASSERT_TRUE(archive.GetCriterias().empty());
    }

    {
        milvus::engine::ArchiveConf archive("delete", "1:2:3");
        ASSERT_TRUE(archive.GetCriterias().empty());
    }

    {
        milvus::engine::ArchiveConf archive("delete");
        milvus::engine::ArchiveConf::CriteriaT criterial = {
            {"disk", 1024},
            {"days", 100}
        };
        archive.SetCriterias(criterial);

        auto crit = archive.GetCriterias();
        ASSERT_EQ(criterial["disk"], 1024);
        ASSERT_EQ(criterial["days"], 100);
    }
}

TEST(DBMiscTest, META_TEST) {
    milvus::engine::DBMetaOptions options;
    options.path_ = "/tmp/milvus_test";
    milvus::engine::meta::SqliteMetaImpl impl(options);

    time_t tt;
    time(&tt);
    int delta = 10;
    milvus::engine::meta::DateT dt = milvus::engine::utils::GetDate(tt, delta);
    ASSERT_GT(dt, 0);
}

TEST(DBMiscTest, UTILS_TEST) {
    milvus::engine::DBMetaOptions options;
    options.path_ = "/tmp/milvus_test/main";
    options.slave_paths_.push_back("/tmp/milvus_test/slave_1");
    options.slave_paths_.push_back("/tmp/milvus_test/slave_2");

    const std::string TABLE_NAME = "test_tbl";
    auto status = milvus::engine::utils::CreateTablePath(options, TABLE_NAME);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(boost::filesystem::exists(options.path_));
    for (auto &path : options.slave_paths_) {
        ASSERT_TRUE(boost::filesystem::exists(path));
    }

//    options.slave_paths.push_back("/");
//    status =  engine::utils::CreateTablePath(options, TABLE_NAME);
//    ASSERT_FALSE(status.ok());
//
//    options.path = "/";
//    status =  engine::utils::CreateTablePath(options, TABLE_NAME);
//    ASSERT_FALSE(status.ok());

    milvus::engine::meta::TableFileSchema file;
    file.id_ = 50;
    file.table_id_ = TABLE_NAME;
    file.file_type_ = 3;
    file.date_ = 155000;
    status = milvus::engine::utils::GetTableFilePath(options, file);
    ASSERT_FALSE(status.ok());
    ASSERT_TRUE(file.location_.empty());

    status = milvus::engine::utils::DeleteTablePath(options, TABLE_NAME);
    ASSERT_TRUE(status.ok());

    status = milvus::engine::utils::DeleteTableFilePath(options, file);
    ASSERT_TRUE(status.ok());
}
