// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <gtest/gtest.h>
#include <cmath>
#include <vector>

#include "scheduler/task/SearchTask.h"
#include "utils/TimeRecorder.h"

namespace {

namespace ms = milvus;

static constexpr uint64_t NQ = 15;
static constexpr uint64_t TOP_K = 64;

void
BuildResult(uint64_t nq,
            uint64_t topk,
            bool ascending,
            std::vector<int64_t> &output_ids,
            std::vector<float> &output_distence) {
    output_ids.clear();
    output_ids.resize(nq * topk);
    output_distence.clear();
    output_distence.resize(nq * topk);

    for (uint64_t i = 0; i < nq; i++) {
        for (uint64_t j = 0; j < topk; j++) {
            output_ids[i * topk + j] = (int64_t) (drand48() * 100000);
            output_distence[i * topk + j] = ascending ? (j + drand48()) : ((topk - j) + drand48());
        }
    }
}

void
CheckResult(const ms::scheduler::Id2DistanceMap &src_1,
            const ms::scheduler::Id2DistanceMap &src_2,
            const ms::scheduler::Id2DistanceMap &target,
            bool ascending) {
    for (uint64_t i = 0; i < target.size() - 1; i++) {
        if (ascending) {
            ASSERT_LE(target[i].second, target[i + 1].second);
        } else {
            ASSERT_GE(target[i].second, target[i + 1].second);
        }
    }

    using ID2DistMap = std::map<int64_t, float>;
    ID2DistMap src_map_1, src_map_2;
    for (const auto &pair : src_1) {
        src_map_1.insert(pair);
    }
    for (const auto &pair : src_2) {
        src_map_2.insert(pair);
    }

    for (const auto &pair : target) {
        ASSERT_TRUE(src_map_1.find(pair.first) != src_map_1.end() || src_map_2.find(pair.first) != src_map_2.end());

        float dist = src_map_1.find(pair.first) != src_map_1.end() ? src_map_1[pair.first] : src_map_2[pair.first];
        ASSERT_LT(fabs(pair.second - dist), std::numeric_limits<float>::epsilon());
    }
}

void
CheckCluster(const std::vector<int64_t> &target_ids,
             const std::vector<float> &target_distence,
             const ms::scheduler::ResultSet &src_result,
             int64_t nq,
             int64_t topk) {
    ASSERT_EQ(src_result.size(), nq);
    for (int64_t i = 0; i < nq; i++) {
        auto &res = src_result[i];
        ASSERT_EQ(res.size(), topk);

        if (res.empty()) {
            continue;
        }

        ASSERT_EQ(res[0].first, target_ids[i * topk]);
        ASSERT_EQ(res[topk - 1].first, target_ids[i * topk + topk - 1]);
    }
}

void
CheckTopkResult(const ms::scheduler::ResultSet &src_result,
                bool ascending,
                int64_t nq,
                int64_t topk) {
    ASSERT_EQ(src_result.size(), nq);
    for (int64_t i = 0; i < nq; i++) {
        auto &res = src_result[i];
        ASSERT_EQ(res.size(), topk);

        if (res.empty()) {
            continue;
        }

        for (int64_t k = 0; k < topk - 1; k++) {
            if (ascending) {
                ASSERT_LE(res[k].second, res[k + 1].second);
            } else {
                ASSERT_GE(res[k].second, res[k + 1].second);
            }
        }
    }
}

} // namespace

TEST(DBSearchTest, TOPK_TEST) {
    bool ascending = true;
    std::vector<int64_t> target_ids;
    std::vector<float> target_distence;
    ms::scheduler::ResultSet src_result;
    auto status = ms::scheduler::XSearchTask::ClusterResult(target_ids, target_distence, NQ, TOP_K, src_result);
    ASSERT_FALSE(status.ok());
    ASSERT_TRUE(src_result.empty());

    BuildResult(NQ, TOP_K, ascending, target_ids, target_distence);
    status = ms::scheduler::XSearchTask::ClusterResult(target_ids, target_distence, NQ, TOP_K, src_result);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(src_result.size(), NQ);

    ms::scheduler::ResultSet target_result;
    status = ms::scheduler::XSearchTask::TopkResult(target_result, TOP_K, ascending, target_result);
    ASSERT_TRUE(status.ok());

    status = ms::scheduler::XSearchTask::TopkResult(target_result, TOP_K, ascending, src_result);
    ASSERT_FALSE(status.ok());

    status = ms::scheduler::XSearchTask::TopkResult(src_result, TOP_K, ascending, target_result);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(src_result.empty());
    ASSERT_EQ(target_result.size(), NQ);

    std::vector<int64_t> src_ids;
    std::vector<float> src_distence;
    uint64_t wrong_topk = TOP_K - 10;
    BuildResult(NQ, wrong_topk, ascending, src_ids, src_distence);

    status = ms::scheduler::XSearchTask::ClusterResult(src_ids, src_distence, NQ, wrong_topk, src_result);
    ASSERT_TRUE(status.ok());

    status = ms::scheduler::XSearchTask::TopkResult(src_result, TOP_K, ascending, target_result);
    ASSERT_TRUE(status.ok());
    for (uint64_t i = 0; i < NQ; i++) {
        ASSERT_EQ(target_result[i].size(), TOP_K);
    }

    wrong_topk = TOP_K + 10;
    BuildResult(NQ, wrong_topk, ascending, src_ids, src_distence);

    status = ms::scheduler::XSearchTask::TopkResult(src_result, TOP_K, ascending, target_result);
    ASSERT_TRUE(status.ok());
    for (uint64_t i = 0; i < NQ; i++) {
        ASSERT_EQ(target_result[i].size(), TOP_K);
    }
}

TEST(DBSearchTest, MERGE_TEST) {
    bool ascending = true;
    std::vector<int64_t> target_ids;
    std::vector<float> target_distence;
    std::vector<int64_t> src_ids;
    std::vector<float> src_distence;
    ms::scheduler::ResultSet src_result, target_result;

    uint64_t src_count = 5, target_count = 8;
    BuildResult(1, src_count, ascending, src_ids, src_distence);
    BuildResult(1, target_count, ascending, target_ids, target_distence);
    auto status = ms::scheduler::XSearchTask::ClusterResult(src_ids, src_distence, 1, src_count, src_result);
    ASSERT_TRUE(status.ok());
    status = ms::scheduler::XSearchTask::ClusterResult(target_ids, target_distence, 1, target_count, target_result);
    ASSERT_TRUE(status.ok());

    {
        ms::scheduler::Id2DistanceMap src = src_result[0];
        ms::scheduler::Id2DistanceMap target = target_result[0];
        status = ms::scheduler::XSearchTask::MergeResult(src, target, 10, ascending);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(target.size(), 10);
        CheckResult(src_result[0], target_result[0], target, ascending);
    }

    {
        ms::scheduler::Id2DistanceMap src = src_result[0];
        ms::scheduler::Id2DistanceMap target;
        status = ms::scheduler::XSearchTask::MergeResult(src, target, 10, ascending);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(target.size(), src_count);
        ASSERT_TRUE(src.empty());
        CheckResult(src_result[0], target_result[0], target, ascending);
    }

    {
        ms::scheduler::Id2DistanceMap src = src_result[0];
        ms::scheduler::Id2DistanceMap target = target_result[0];
        status = ms::scheduler::XSearchTask::MergeResult(src, target, 30, ascending);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(target.size(), src_count + target_count);
        CheckResult(src_result[0], target_result[0], target, ascending);
    }

    {
        ms::scheduler::Id2DistanceMap target = src_result[0];
        ms::scheduler::Id2DistanceMap src = target_result[0];
        status = ms::scheduler::XSearchTask::MergeResult(src, target, 30, ascending);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(target.size(), src_count + target_count);
        CheckResult(src_result[0], target_result[0], target, ascending);
    }
}

TEST(DBSearchTest, PARALLEL_CLUSTER_TEST) {
    bool ascending = true;
    std::vector<int64_t> target_ids;
    std::vector<float> target_distence;
    ms::scheduler::ResultSet src_result;

    auto DoCluster = [&](int64_t nq, int64_t topk) {
        ms::TimeRecorder rc("DoCluster");
        src_result.clear();
        BuildResult(nq, topk, ascending, target_ids, target_distence);
        rc.RecordSection("build id/dietance map");

        auto status = ms::scheduler::XSearchTask::ClusterResult(target_ids, target_distence, nq, topk, src_result);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(src_result.size(), nq);

        rc.RecordSection("cluster result");

        CheckCluster(target_ids, target_distence, src_result, nq, topk);
        rc.RecordSection("check result");
    };

    DoCluster(10000, 1000);
    DoCluster(333, 999);
    DoCluster(1, 1000);
    DoCluster(1, 1);
    DoCluster(7, 0);
    DoCluster(9999, 1);
    DoCluster(10001, 1);
    DoCluster(58273, 1234);
}

TEST(DBSearchTest, PARALLEL_TOPK_TEST) {
    std::vector<int64_t> target_ids;
    std::vector<float> target_distence;
    ms::scheduler::ResultSet src_result;

    std::vector<int64_t> insufficient_ids;
    std::vector<float> insufficient_distence;
    ms::scheduler::ResultSet insufficient_result;

    auto DoTopk = [&](int64_t nq, int64_t topk, int64_t insufficient_topk, bool ascending) {
        src_result.clear();
        insufficient_result.clear();

        ms::TimeRecorder rc("DoCluster");

        BuildResult(nq, topk, ascending, target_ids, target_distence);
        auto status = ms::scheduler::XSearchTask::ClusterResult(target_ids, target_distence, nq, topk, src_result);
        rc.RecordSection("cluster result");

        BuildResult(nq, insufficient_topk, ascending, insufficient_ids, insufficient_distence);
        status = ms::scheduler::XSearchTask::ClusterResult(target_ids,
                                                           target_distence,
                                                           nq,
                                                           insufficient_topk,
                                                           insufficient_result);
        rc.RecordSection("cluster result");

        ms::scheduler::XSearchTask::TopkResult(insufficient_result, topk, ascending, src_result);
        ASSERT_TRUE(status.ok());
        rc.RecordSection("topk");

        CheckTopkResult(src_result, ascending, nq, topk);
        rc.RecordSection("check result");
    };

    DoTopk(5, 10, 4, false);
    DoTopk(20005, 998, 123, true);
//    DoTopk(9987, 12, 10, false);
//    DoTopk(77777, 1000, 1, false);
//    DoTopk(5432, 8899, 8899, true);
}
