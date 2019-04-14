////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include <gtest/gtest.h>


TEST(LogTest, INIT_TEST) {
    ASSERT_STREQ("A", "A");
}

TEST(LogTest, RUN_TEST) {
    ASSERT_STREQ("B", "B");
}

TEST(LogTest, FINISH_TEST) {
    ASSERT_STREQ("C", "C");
}