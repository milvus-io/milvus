////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include <gtest/gtest.h>
#include "license/License.h"

using namespace zilliz::vecwise::server;

TEST(LicenseTest, LICENSE_TEST) {
    std::string path1 = "/tmp/vecwise_engine.license";
    ServerError err = LicenseValidate(path1);
    ASSERT_EQ(err, server::SERVER_SUCCESS);
}
