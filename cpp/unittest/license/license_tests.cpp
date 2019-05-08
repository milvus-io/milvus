////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include <gtest/gtest.h>
#include "license/License.h"
#include "utils/Error.h"

using namespace zilliz::vecwise;

TEST(LicenseTest, LICENSE_TEST) {
    std::string path1 = "/tmp/vecwise_engine.license";
    server::ServerError err = server::LicenseValidate(path1);
    ASSERT_EQ(err, server::SERVER_SUCCESS);
}
