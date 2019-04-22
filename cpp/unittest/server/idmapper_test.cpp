////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include <gtest/gtest.h>

#include "server/VecIdMapper.h"

using namespace zilliz::vecwise;


TEST(IdMapperTest, IDMAPPER_TEST) {
    server::IVecIdMapper* mapper = server::IVecIdMapper::GetInstance();

    std::vector<int64_t> nid = {1,50, 900, 10000};
    std::vector<std::string> sid = {"one", "fifty", "nine zero zero", "many"};
    server::ServerError err = mapper->Put(nid, sid);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    sid.clear();
    err = mapper->Put(nid, sid);
    ASSERT_NE(err, server::SERVER_SUCCESS);

    std::vector<std::string> res;
    err = mapper->Get(nid, res);
    ASSERT_EQ(res.size(), nid.size());

    std::string str_id;
    err = mapper->Get(50, str_id);
    ASSERT_EQ(str_id, "fifty");

    err = mapper->Delete(900);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    err = mapper->Get(900, str_id);
    ASSERT_NE(err, server::SERVER_SUCCESS);
}

