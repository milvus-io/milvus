// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include <chrono>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <gtest/gtest.h>
#include <fiu-local.h>
#include <fiu-control.h>

#include "cache/CpuCacheMgr.h"
#include "config/Config.h"
#include "metrics/utils.h"
#include "db/DB.h"
#include "db/meta/SqliteMetaImpl.h"
#include "metrics/Metrics.h"

namespace {
static constexpr int64_t COLLECTION_DIM = 256;

void
BuildVectors(uint64_t n, milvus::engine::VectorsData& vectors) {
    vectors.vector_count_ = n;
    vectors.float_data_.clear();
    vectors.float_data_.resize(n * COLLECTION_DIM);
    float* data = vectors.float_data_.data();
    for (uint64_t i = 0; i < n; i++) {
        for (int64_t j = 0; j < COLLECTION_DIM; j++) data[COLLECTION_DIM * i + j] = drand48();
        data[COLLECTION_DIM * i] += i / 2000.;
    }
}
} // namespace

TEST_F(MetricTest, METRIC_TEST) {
    fiu_init(0);

#ifdef MILVUS_GPU_VERSION
    FIU_ENABLE_FIU("SystemInfo.Init.nvmInit_fail");
    milvus::server::SystemInfo::GetInstance().initialized_ = false;
    milvus::server::SystemInfo::GetInstance().Init();
    fiu_disable("SystemInfo.Init.nvmInit_fail");
    FIU_ENABLE_FIU("SystemInfo.Init.nvm_getDevice_fail");
    milvus::server::SystemInfo::GetInstance().initialized_ = false;
    milvus::server::SystemInfo::GetInstance().Init();
    fiu_disable("SystemInfo.Init.nvm_getDevice_fail");
    milvus::server::SystemInfo::GetInstance().initialized_ = false;
#endif

    milvus::server::SystemInfo::GetInstance().Init();
    milvus::server::Metrics::GetInstance().Init();

    std::string system_info;
    milvus::server::SystemInfo::GetInstance().GetSysInfoJsonStr(system_info);

    milvus::cache::CpuCacheMgr::GetInstance()->SetCapacity(1UL * 1024 * 1024 * 1024);
    std::cout << milvus::cache::CpuCacheMgr::GetInstance()->CacheCapacity() << std::endl;

    static const char* group_name = "test_group";
    static const int group_dim = 256;

    milvus::engine::meta::CollectionSchema group_info;
    group_info.dimension_ = group_dim;
    group_info.collection_id_ = group_name;
    auto stat = db_->CreateCollection(group_info);

    milvus::engine::meta::CollectionSchema group_info_get;
    group_info_get.collection_id_ = group_name;
    stat = db_->DescribeCollection(group_info_get);

    int nb = 50;
    milvus::engine::VectorsData xb;
    BuildVectors(nb, xb);

    int qb = 5;
    milvus::engine::VectorsData xq;
    BuildVectors(qb, xq);

    std::thread search([&]() {
//        std::vector<std::string> tags;
//        milvus::engine::ResultIds result_ids;
//        milvus::engine::ResultDistances result_distances;
        std::this_thread::sleep_for(std::chrono::seconds(1));

        INIT_TIMER;
        std::stringstream ss;
        uint64_t count = 0;
        uint64_t prev_count = 0;

        for (auto j = 0; j < 10; ++j) {
            ss.str("");
            db_->Size(count);
            prev_count = count;

            START_TIMER;
//            stat = db_->Query(group_name, tags, k, qb, qxb, result_ids, result_distances);
            ss << "Search " << j << " With Size " << (float)(count * group_dim * sizeof(float)) / (1024 * 1024)
               << " M";

            for (auto k = 0; k < qb; ++k) {
//                ASSERT_EQ(results[k][0].first, target_ids[k]);
                ss.str("");
                ss << "Result [" << k << "]:";
//                for (auto result : results[k]) {
//                    ss << result.first << " ";
//                }
            }
            ASSERT_TRUE(count >= prev_count);
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    });

    int loop = 100;

    for (auto i = 0; i < loop; ++i) {
        if (i == 40) {
            xq.id_array_.clear();
            db_->InsertVectors(group_name, "", xq);
            ASSERT_EQ(xq.id_array_.size(), qb);
        } else {
            xb.id_array_.clear();
            db_->InsertVectors(group_name, "", xb);
            ASSERT_EQ(xb.id_array_.size(), nb);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }

    search.join();
}

TEST_F(MetricTest, COLLECTOR_METRICS_TEST) {
    auto status = milvus::Status::OK();
    milvus::server::CollectInsertMetrics insert_metrics0(0, status);
    status = milvus::Status(milvus::DB_ERROR, "error");
    milvus::server::CollectInsertMetrics insert_metrics1(0, status);

    milvus::server::CollectQueryMetrics query_metrics(10);

    milvus::server::CollectMergeFilesMetrics merge_metrics();

    milvus::server::CollectBuildIndexMetrics build_index_metrics();

    milvus::server::CollectExecutionEngineMetrics execution_metrics(10);

    milvus::server::CollectSerializeMetrics serialize_metrics(10);

    milvus::server::CollectAddMetrics add_metrics(10, 128);

    milvus::server::CollectDurationMetrics duration_metrics_raw(milvus::engine::meta::SegmentSchema::RAW);
    milvus::server::CollectDurationMetrics duration_metrics_index(milvus::engine::meta::SegmentSchema::TO_INDEX);
    milvus::server::CollectDurationMetrics duration_metrics_delete(milvus::engine::meta::SegmentSchema::TO_DELETE);

    milvus::server::CollectSearchTaskMetrics search_metrics_raw(milvus::engine::meta::SegmentSchema::RAW);
    milvus::server::CollectSearchTaskMetrics search_metrics_index(milvus::engine::meta::SegmentSchema::TO_INDEX);
    milvus::server::CollectSearchTaskMetrics search_metrics_delete(milvus::engine::meta::SegmentSchema::TO_DELETE);

    milvus::server::MetricCollector metric_collector();
}


