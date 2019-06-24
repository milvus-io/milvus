/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include <chrono>
#include <chrono>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <gtest/gtest.h>
//#include "prometheus/registry.h"
//#include "prometheus/exposer.h"
#include <cache/CpuCacheMgr.h>

#include "metrics/Metrics.h"
#include "../db/utils.h"
#include "db/DB.h"
#include "db/DBMetaImpl.h"
#include "db/Factories.h"


using namespace zilliz::milvus;



TEST_F(DBTest, Metric_Tes) {

    server::SystemInfo::GetInstance().Init();
//    server::Metrics::GetInstance().Init();
//    server::Metrics::GetInstance().exposer_ptr()->RegisterCollectable(server::Metrics::GetInstance().registry_ptr());
    server::Metrics::GetInstance().Init();

//    server::PrometheusMetrics::GetInstance().exposer_ptr()->RegisterCollectable(server::PrometheusMetrics::GetInstance().registry_ptr());
    zilliz::milvus::cache::CpuCacheMgr::GetInstance()->SetCapacity(1UL*1024*1024*1024);
    std::cout<<zilliz::milvus::cache::CpuCacheMgr::GetInstance()->CacheCapacity()<<std::endl;

    static const std::string group_name = "test_group";
    static const int group_dim = 256;

    engine::meta::TableSchema group_info;
    group_info.dimension_ = group_dim;
    group_info.table_id_ = group_name;
    engine::Status stat = db_->CreateTable(group_info);

    engine::meta::TableSchema group_info_get;
    group_info_get.table_id_ = group_name;
    stat = db_->DescribeTable(group_info_get);


    engine::IDNumbers vector_ids;
    engine::IDNumbers target_ids;

    int d = 256;
    int nb = 50;
    float *xb = new float[d * nb];
    for(int i = 0; i < nb; i++) {
        for(int j = 0; j < d; j++) xb[d * i + j] = drand48();
        xb[d * i] += i / 2000.;
    }

    int qb = 5;
    float *qxb = new float[d * qb];
    for(int i = 0; i < qb; i++) {
        for(int j = 0; j < d; j++) qxb[d * i + j] = drand48();
        qxb[d * i] += i / 2000.;
    }

    std::thread search([&]() {
        engine::QueryResults results;
        int k = 10;
        std::this_thread::sleep_for(std::chrono::seconds(2));

        INIT_TIMER;
        std::stringstream ss;
        uint64_t count = 0;
        uint64_t prev_count = 0;

        for (auto j=0; j<10; ++j) {
            ss.str("");
            db_->Size(count);
            prev_count = count;

            START_TIMER;
//            stat = db_->Query(group_name, k, qb, qxb, results);
            ss << "Search " << j << " With Size " << (float)(count*group_dim*sizeof(float))/(1024*1024) << " M";


//            ASSERT_STATS(stat);
            for (auto k=0; k<qb; ++k) {
//                ASSERT_EQ(results[k][0].first, target_ids[k]);
                ss.str("");
                ss << "Result [" << k << "]:";
//                for (auto result : results[k]) {
//                    ss << result.first << " ";
//                }

            }
            ASSERT_TRUE(count >= prev_count);
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    });

    int loop = 100000;

    for (auto i=0; i<loop; ++i) {
        if (i==40) {
            db_->InsertVectors(group_name, qb, qxb, target_ids);
            ASSERT_EQ(target_ids.size(), qb);
        } else {
            db_->InsertVectors(group_name, nb, xb, vector_ids);
        }
        std::this_thread::sleep_for(std::chrono::microseconds(2000));
    }

    search.join();

    delete [] xb;
    delete [] qxb;
};


