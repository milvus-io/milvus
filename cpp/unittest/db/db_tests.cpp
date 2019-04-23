////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include <gtest/gtest.h>

#include "db/DB.h"

using namespace zilliz::vecwise;

namespace {
    void ASSERT_STATS(engine::Status& stat) {
        ASSERT_TRUE(stat.ok());
        if(!stat.ok()) {
            std::cout << stat.ToString() << std::endl;
        }
    }
}

TEST(DBTest, DB_TEST) {
    static const std::string group_name = "test_group";
    static const int group_dim = 256;

    engine::Options opt;
    opt.meta.backend_uri = "http://127.0.0.1";
    opt.meta.path = "/tmp/vecwise_test/db_test";

    engine::DB* db = nullptr;
    engine::DB::Open(opt, &db);
    ASSERT_TRUE(db != nullptr);

    engine::meta::GroupSchema group_info;
    group_info.dimension = group_dim;
    group_info.group_id = group_name;
    engine::Status stat = db->add_group(group_info);
    //ASSERT_STATS(stat);

    engine::meta::GroupSchema group_info_get;
    group_info_get.group_id = group_name;
    stat = db->get_group(group_info_get);
    ASSERT_STATS(stat);
    ASSERT_EQ(group_info_get.dimension, group_dim);

    engine::IDNumbers vector_ids;
    std::vector<float> vec_f;
    for(int i = 0; i < group_dim; i++){
        vec_f.push_back((float)i);
        vector_ids.push_back(i+1);
    }
    stat = db->add_vectors(group_name, 1, vec_f.data(), vector_ids);
    ASSERT_STATS(stat);

    engine::QueryResults results;
    std::vector<float> vec_s = vec_f;
    stat = db->search(group_name, 1, 1, vec_f.data(), results);
    ASSERT_STATS(stat);
    ASSERT_EQ(results.size(), 1);
    ASSERT_EQ(results[0][0], vector_ids[0]);

    delete db;
}