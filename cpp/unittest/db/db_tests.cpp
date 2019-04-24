////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include <gtest/gtest.h>
#include <faiss/IndexFlat.h>
#include <faiss/MetaIndexes.h>
#include <faiss/AutoTune.h>
#include <thread>
#include <easylogging++.h>

#include "db/DB.h"
#include "faiss/Index.h"

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
    opt.memory_sync_interval = 1;
    opt.meta.backend_uri = "http://127.0.0.1";
    opt.meta.path = "/tmp/vecwise_test/db_test";

    engine::DB* db = nullptr;
    engine::DB::Open(opt, &db);
    ASSERT_TRUE(db != nullptr);

    engine::meta::GroupSchema group_info;
    group_info.dimension = group_dim;
    group_info.group_id = group_name;
    engine::Status stat = db->add_group(group_info);

    engine::meta::GroupSchema group_info_get;
    group_info_get.group_id = group_name;
    stat = db->get_group(group_info_get);
    ASSERT_STATS(stat);
    ASSERT_EQ(group_info_get.dimension, group_dim);

    engine::IDNumbers vector_ids;
    engine::IDNumbers target_ids;

    int d = 256;
    int nb = 10;
    float *xb = new float[d * nb];
    for(int i = 0; i < nb; i++) {
        for(int j = 0; j < d; j++) xb[d * i + j] = drand48();
        xb[d * i] += i / 2000.;
    }

    int qb = 1;
    float *qxb = new float[d * qb];
    for(int i = 0; i < qb; i++) {
        for(int j = 0; j < d; j++) qxb[d * i + j] = drand48();
        qxb[d * i] += i / 2000.;
    }

    int loop = 500000;

    for (auto i=0; i<loop; ++i) {
        if (i==40) {
            db->add_vectors(group_name, qb, qxb, target_ids);
        } else {
            db->add_vectors(group_name, nb, xb, vector_ids);
        }
    }

    std::this_thread::sleep_for(std::chrono::seconds(10));

    long count = 0;
    db->count(group_name, count);
    LOG(DEBUG) << "Count=" << count;

    engine::QueryResults results;
    int k = 10;
    LOG(DEBUG) << "PRE";
    stat = db->search(group_name, k, qb, qxb, results);
    LOG(DEBUG) << "POST";
    ASSERT_STATS(stat);
    ASSERT_EQ(results[0][0], target_ids[0]);

    delete [] xb;
    delete [] qxb;
    delete db;
    engine::DB::Open(opt, &db);
    db->drop_all();
    delete db;
}

TEST(SearchTest, DB_TEST) {
    static const std::string group_name = "test_group";
    static const int group_dim = 256;

    engine::Options opt;
    opt.meta.backend_uri = "http://127.0.0.1";
    opt.meta.path = "/tmp/search_test";
    opt.index_trigger_size = 100000 * group_dim;
    opt.memory_sync_interval = 1;
    opt.merge_trigger_number = 1;

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


    // prepare raw data
    size_t nb = 250000;
    size_t nq = 10;
    size_t k = 5;
    std::vector<float> xb(nb*group_dim);
    std::vector<float> xq(nq*group_dim);
    std::vector<long> ids(nb);

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis_xt(-1.0, 1.0);
    for (size_t i = 0; i < nb*group_dim; i++) {
        xb[i] = dis_xt(gen);
        if (i < nb){
            ids[i] = i;
        }
    }
    for (size_t i = 0; i < nq*group_dim; i++) {
        xq[i] = dis_xt(gen);
    }

    // result data
    //std::vector<long> nns_gt(k*nq);
    std::vector<long> nns(k*nq);  // nns = nearst neg search
    //std::vector<float> dis_gt(k*nq);
    std::vector<float> dis(k*nq);

    // prepare ground-truth
    //faiss::Index* index_gt(faiss::index_factory(group_dim, "IDMap,Flat"));
    //index_gt->add_with_ids(nb, xb.data(), ids.data());
    //index_gt->search(nq, xq.data(), 1, dis_gt.data(), nns_gt.data());

    // insert data
    const int batch_size = 100;
    for (int j = 0; j < nb / batch_size; ++j) {
        stat = db->add_vectors(group_name, batch_size, xb.data()+batch_size*j*group_dim, ids);
        if (j == 200){ sleep(1);}
        ASSERT_STATS(stat);
    }

    sleep(3); // wait until build index finish

    engine::QueryResults results;
    stat = db->search(group_name, k, nq, xq.data(), results);
    ASSERT_STATS(stat);

    // TODO(linxj): add groundTruth assert

    delete db;

    engine::DB::Open(opt, &db);
    db->drop_all();
    delete db;
}
