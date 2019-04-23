////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include <gtest/gtest.h>
#include "utils/CommonUtil.h"
#include "utils/Error.h"

using namespace zilliz::vecwise;


TEST(CommonTest, COMMON_TEST) {
    std::string path1 = "/tmp/vecwise_test/common_test_12345/";
    std::string path2 = path1 + "abcdef";
    server::ServerError err = server::CommonUtil::CreateDirectory(path2);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    ASSERT_TRUE(server::CommonUtil::IsDirectoryExit(path2));

    err = server::CommonUtil::DeleteDirectory(path1);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    ASSERT_FALSE(server::CommonUtil::IsDirectoryExit(path1));
}
