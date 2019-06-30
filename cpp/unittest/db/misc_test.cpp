////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include <gtest/gtest.h>
#include <thread>
#include <easylogging++.h>
#include <boost/filesystem.hpp>

#include "db/FaissExecutionEngine.h"
#include "db/Exception.h"
#include "db/Status.h"
#include "db/Options.h"
#include "db/DBMetaImpl.h"

#include <vector>

using namespace zilliz::milvus;

TEST(DBMiscTest, ENGINE_API_TEST) {
    //engine api AddWithIdArray
    const uint16_t dim = 512;
    const long n = 10;
    engine::FaissExecutionEngine engine(512, "/tmp/1", "IDMap", "IDMap,Flat");
    std::vector<float> vectors;
    std::vector<long> ids;
    for (long i = 0; i < n; i++) {
        for (uint16_t k = 0; k < dim; k++) {
            vectors.push_back((float) k);
        }
        ids.push_back(i);
    }

    auto status = engine.AddWithIdArray(vectors, ids);
    ASSERT_TRUE(status.ok());
}

TEST(DBMiscTest, EXCEPTION_TEST) {
    engine::Exception ex1("");
    std::string what = ex1.what();
    ASSERT_FALSE(what.empty());

    engine::OutOfRangeException ex2;
    what = ex2.what();
    ASSERT_FALSE(what.empty());
}

TEST(DBMiscTest, STATUS_TEST) {
    engine::Status status = engine::Status::OK();
    std::string str = status.ToString();
    ASSERT_FALSE(str.empty());

    status = engine::Status::Error("wrong", "mistake");
    ASSERT_TRUE(status.IsError());
    str = status.ToString();
    ASSERT_FALSE(str.empty());

    status = engine::Status::NotFound("wrong", "mistake");
    ASSERT_TRUE(status.IsNotFound());
    str = status.ToString();
    ASSERT_FALSE(str.empty());

    status = engine::Status::DBTransactionError("wrong", "mistake");
    ASSERT_TRUE(status.IsDBTransactionError());
    str = status.ToString();
    ASSERT_FALSE(str.empty());
}

TEST(DBMiscTest, OPTIONS_TEST) {
    try {
        engine::ArchiveConf archive("$$##");
    } catch (std::exception& ex) {
        ASSERT_TRUE(true);
    }

    {
        engine::ArchiveConf archive("delete", "no");
        ASSERT_TRUE(archive.GetCriterias().empty());
    }

    {
        engine::ArchiveConf archive("delete", "1:2");
        ASSERT_TRUE(archive.GetCriterias().empty());
    }

    {
        engine::ArchiveConf archive("delete", "1:2:3");
        ASSERT_TRUE(archive.GetCriterias().empty());
    }
}

TEST(DBMiscTest, META_TEST) {
    engine::DBMetaOptions options;
    options.path = "/tmp/milvus_test";
    engine::meta::DBMetaImpl impl(options);

    time_t tt;
    time( &tt );
    int delta = 10;
    engine::meta::DateT dt = impl.GetDate(tt, delta);
    ASSERT_GT(dt, 0);
}