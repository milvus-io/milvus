////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include <gtest/gtest.h>
#include "server/ServerConfig.h"
#include "server/VecIdMapper.h"
#include "utils/TimeRecorder.h"
#include "utils/CommonUtil.h"

using namespace zilliz::vecwise;


TEST(IdMapperTest, IDMAPPER_TEST) {
    server::IVecIdMapper* mapper = server::IVecIdMapper::GetInstance();

    std::vector<std::string> nid = {"1", "50", "900", "10000"};
    std::vector<std::string> sid = {"one", "fifty", "nine zero zero", "many"};
    server::ServerError err = mapper->Put(nid, sid);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    err = mapper->Put(nid, std::vector<std::string>());
    ASSERT_NE(err, server::SERVER_SUCCESS);

    std::vector<std::string> res;
    err = mapper->Get(nid, res);
    ASSERT_EQ(res.size(), nid.size());
    for(size_t i = 0; i < res.size(); i++) {
        ASSERT_EQ(res[i], sid[i]);
    }

    std::string str_id;
    err = mapper->Get(nid[1], str_id);
    ASSERT_EQ(str_id, "fifty");

    err = mapper->Delete(nid[2]);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    err = mapper->Get(nid[2], str_id);
    ASSERT_NE(err, server::SERVER_SUCCESS);

    //test performance
    nid.clear();
    sid.clear();
    const int64_t count = 1000000;
    {
        server::TimeRecorder rc("prepare id data");
        for (int64_t i = 0; i < count; i++) {
            nid.push_back(std::to_string(i + 100000));
            sid.push_back("val_" + std::to_string(i));
        }
        rc.Record("done!");
    }

    {
        std::string str_info = "Insert " + std::to_string(count) + " k/v into mapper";
        server::TimeRecorder rc(str_info);
        err = mapper->Put(nid, sid);
        ASSERT_EQ(err, server::SERVER_SUCCESS);
        rc.Record("done!");
    }

    {
        std::string str_info = "Get " + std::to_string(count) + " k/v from mapper";
        server::TimeRecorder rc(str_info);
        std::vector<std::string> res;
        err = mapper->Get(nid, res);
        ASSERT_EQ(res.size(), nid.size());
        rc.Record("done!");
    }
}

