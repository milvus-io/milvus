////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include <gtest/gtest.h>
#include <thread>
#include <easylogging++.h>

#include "utils.h"
#include "db/DBMetaImpl.h"
#include "db/Factories.h"

using namespace zilliz::vecwise::engine;

TEST_F(MetaTest, GROUP_TEST) {
    auto group_id = "meta_test_group";

    meta::GroupSchema group;
    group.group_id = group_id;
    auto status = impl_->add_group(group);
    ASSERT_TRUE(status.ok());

    auto gid = group.id;
    group.id = -1;
    status = impl_->get_group(group);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(group.id, gid);
    ASSERT_EQ(group.group_id, group_id);

    group.group_id = "not_found";
    status = impl_->get_group(group);
    ASSERT_TRUE(!status.ok());

    group.group_id = group_id;
    status = impl_->add_group(group);
    ASSERT_TRUE(!status.ok());
}
