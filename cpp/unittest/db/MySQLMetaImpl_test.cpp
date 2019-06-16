////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include <gtest/gtest.h>
#include <thread>
#include <easylogging++.h>
#include <stdlib.h>
#include <time.h>

#include "utils.h"
#include "db/MySQLMetaImpl.h"
#include "db/Factories.h"
#include "db/Utils.h"
#include "db/MetaConsts.h"

using namespace zilliz::milvus::engine;

TEST_F(MySQLTest, InitializeTest) {
    DBMetaOptions options;
    //dialect+driver://username:password@host:port/database
    options.backend_uri = "mysql://root:1234@:/test";
    meta::MySQLMetaImpl impl(options);
    auto status = impl.Initialize();
    std::cout << status.ToString() << std::endl;
    //ASSERT_TRUE(status.ok());
}