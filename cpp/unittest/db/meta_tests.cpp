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

TEST_F(DBTest, META_TEST) {
    auto impl = DBMetaImplFactory::Build();

    auto group_id = "meta_test_group";

    meta::GroupSchema group;
    group.group_id = group_id;
    auto status = impl->add_group(group);
    ASSERT_TRUE(status.ok());

    status = impl->get_group(group);
    ASSERT_TRUE(status.ok());

    group.group_id = "not_found";
    status = impl->get_group(group);
    ASSERT_TRUE(!status.ok());

    impl->drop_all();
}
