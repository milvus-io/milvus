////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include "db/scheduler/task/SearchTask.h"
#include "utils/TimeRecorder.h"

#include <gtest/gtest.h>
#include <cmath>
#include <vector>

using namespace zilliz::milvus;

namespace {

static constexpr uint64_t NQ = 15;
static constexpr uint64_t TOP_K = 64;

void BuildResult(uint64_t nq,
                 uint64_t topk,
                 bool ascending,
                 std::vector<long> &output_ids,
                 std::vector<float> &output_distence) {
    output_ids.clear();
    output_ids.resize(nq*topk);
    output_distence.clear();
    output_distence.resize(nq*topk);

    for(uint64_t i = 0; i < nq; i++) {
        for(uint64_t j = 0; j < topk; j++) {
            output_ids[i * topk + j] = (long)(drand48()*100000);
            output_distence[i * topk + j] = ascending ? (j + drand48()) : ((topk - j) + drand48());
        }
    }
}

void CheckResult(const engine::SearchContext::Id2DistanceMap& src_1,
        const engine::SearchContext::Id2DistanceMap& src_2,
        const engine::SearchContext::Id2DistanceMap& target,
        bool ascending) {
    for(uint64_t i = 0; i < target.size() - 1; i++) {
        if(ascending) {
            ASSERT_LE(target[i].second, target[i + 1].second);
        } else {
            ASSERT_GE(target[i].second, target[i + 1].second);
        }
    }

    using ID2DistMap = std::map<long, float>;
    ID2DistMap src_map_1, src_map_2;
    for(const auto& pair : src_1) {
        src_map_1.insert(pair);
    }
    for(const auto& pair : src_2) {
        src_map_2.insert(pair);
    }

    for(const auto& pair : target) {
        ASSERT_TRUE(src_map_1.find(pair.first) != src_map_1.end() || src_map_2.find(pair.first) != src_map_2.end());

        float dist = src_map_1.find(pair.first) != src_map_1.end() ? src_map_1[pair.first] : src_map_2[pair.first];
        ASSERT_LT(fabs(pair.second - dist), std::numeric_limits<float>::epsilon());
    }
}

void CheckCluster(const std::vector<long>& target_ids,
        const std::vector<float>& target_distence,
        const engine::SearchContext::ResultSet& src_result,
        int64_t nq,
        int64_t topk) {
    ASSERT_EQ(src_result.size(), nq);
    for(int64_t i = 0; i < nq; i++) {
        auto& res = src_result[i];
        ASSERT_EQ(res.size(), topk);

        if(res.empty()) {
            continue;
        }

        ASSERT_EQ(res[0].first, target_ids[i*topk]);
        ASSERT_EQ(res[topk - 1].first, target_ids[i*topk + topk - 1]);
    }
}

void CheckTopkResult(const engine::SearchContext::ResultSet& src_result,
                     bool ascending,
                     int64_t nq,
                     int64_t topk) {
    ASSERT_EQ(src_result.size(), nq);
    for(int64_t i = 0; i < nq; i++) {
        auto& res = src_result[i];
        ASSERT_EQ(res.size(), topk);

        if(res.empty()) {
            continue;
        }

        for(int64_t k = 0; k < topk - 1; k++) {
            if(ascending) {
                ASSERT_LE(res[k].second, res[k + 1].second);
            } else {
                ASSERT_GE(res[k].second, res[k + 1].second);
            }
        }
    }
}

}

TEST(DBSearchTest, TOPK_TEST) {
    bool ascending = true;
    std::vector<long> target_ids;
    std::vector<float> target_distence;
    engine::SearchContext::ResultSet src_result;
    auto status = engine::SearchTask::ClusterResult(target_ids, target_distence, NQ, TOP_K, src_result);
    ASSERT_FALSE(status.ok());
    ASSERT_TRUE(src_result.empty());

    BuildResult(NQ, TOP_K, ascending, target_ids, target_distence);
    status = engine::SearchTask::ClusterResult(target_ids, target_distence, NQ, TOP_K, src_result);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(src_result.size(), NQ);

    engine::SearchContext::ResultSet target_result;
    status = engine::SearchTask::TopkResult(target_result, TOP_K, ascending, target_result);
    ASSERT_TRUE(status.ok());

    status = engine::SearchTask::TopkResult(target_result, TOP_K, ascending, src_result);
    ASSERT_FALSE(status.ok());

    status = engine::SearchTask::TopkResult(src_result, TOP_K, ascending, target_result);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(src_result.empty());
    ASSERT_EQ(target_result.size(), NQ);

    std::vector<long> src_ids;
    std::vector<float> src_distence;
    uint64_t wrong_topk = TOP_K - 10;
    BuildResult(NQ, wrong_topk, ascending, src_ids, src_distence);

    status = engine::SearchTask::ClusterResult(src_ids, src_distence, NQ, wrong_topk, src_result);
    ASSERT_TRUE(status.ok());

    status = engine::SearchTask::TopkResult(src_result, TOP_K, ascending, target_result);
    ASSERT_TRUE(status.ok());
    for(uint64_t i = 0; i < NQ; i++) {
        ASSERT_EQ(target_result[i].size(), TOP_K);
    }

    wrong_topk = TOP_K + 10;
    BuildResult(NQ, wrong_topk, ascending, src_ids, src_distence);

    status = engine::SearchTask::TopkResult(src_result, TOP_K, ascending, target_result);
    ASSERT_TRUE(status.ok());
    for(uint64_t i = 0; i < NQ; i++) {
        ASSERT_EQ(target_result[i].size(), TOP_K);
    }
}

TEST(DBSearchTest, MERGE_TEST) {
    bool ascending = true;
    std::vector<long> target_ids;
    std::vector<float> target_distence;
    std::vector<long> src_ids;
    std::vector<float> src_distence;
    engine::SearchContext::ResultSet src_result, target_result;

    uint64_t src_count = 5, target_count = 8;
    BuildResult(1, src_count, ascending, src_ids, src_distence);
    BuildResult(1, target_count, ascending, target_ids, target_distence);
    auto status = engine::SearchTask::ClusterResult(src_ids, src_distence, 1, src_count, src_result);
    ASSERT_TRUE(status.ok());
    status = engine::SearchTask::ClusterResult(target_ids, target_distence, 1, target_count, target_result);
    ASSERT_TRUE(status.ok());

    {
        engine::SearchContext::Id2DistanceMap src = src_result[0];
        engine::SearchContext::Id2DistanceMap target = target_result[0];
        status = engine::SearchTask::MergeResult(src, target, 10, ascending);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(target.size(), 10);
        CheckResult(src_result[0], target_result[0], target, ascending);
    }

    {
        engine::SearchContext::Id2DistanceMap src = src_result[0];
        engine::SearchContext::Id2DistanceMap target;
        status = engine::SearchTask::MergeResult(src, target, 10, ascending);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(target.size(), src_count);
        ASSERT_TRUE(src.empty());
        CheckResult(src_result[0], target_result[0], target, ascending);
    }

    {
        engine::SearchContext::Id2DistanceMap src = src_result[0];
        engine::SearchContext::Id2DistanceMap target = target_result[0];
        status = engine::SearchTask::MergeResult(src, target, 30, ascending);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(target.size(), src_count + target_count);
        CheckResult(src_result[0], target_result[0], target, ascending);
    }

    {
        engine::SearchContext::Id2DistanceMap target = src_result[0];
        engine::SearchContext::Id2DistanceMap src = target_result[0];
        status = engine::SearchTask::MergeResult(src, target, 30, ascending);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(target.size(), src_count + target_count);
        CheckResult(src_result[0], target_result[0], target, ascending);
    }
}

TEST(DBSearchTest, PARALLEL_CLUSTER_TEST) {
    bool ascending = true;
    std::vector<long> target_ids;
    std::vector<float> target_distence;
    engine::SearchContext::ResultSet src_result;

    auto DoCluster = [&](int64_t nq, int64_t topk) {
        server::TimeRecorder rc("DoCluster");
        src_result.clear();
        BuildResult(nq, topk, ascending, target_ids, target_distence);
        rc.RecordSection("build id/dietance map");

        auto status = engine::SearchTask::ClusterResult(target_ids, target_distence, nq, topk, src_result);
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
    std::vector<long> target_ids;
    std::vector<float> target_distence;
    engine::SearchContext::ResultSet src_result;

    std::vector<long> insufficient_ids;
    std::vector<float> insufficient_distence;
    engine::SearchContext::ResultSet insufficient_result;

    auto DoTopk = [&](int64_t nq, int64_t topk,int64_t insufficient_topk, bool ascending) {
        src_result.clear();
        insufficient_result.clear();

        server::TimeRecorder rc("DoCluster");

        BuildResult(nq, topk, ascending, target_ids, target_distence);
        auto status = engine::SearchTask::ClusterResult(target_ids, target_distence, nq, topk, src_result);
        rc.RecordSection("cluster result");

        BuildResult(nq, insufficient_topk, ascending, insufficient_ids, insufficient_distence);
        status = engine::SearchTask::ClusterResult(target_ids, target_distence, nq, insufficient_topk, insufficient_result);
        rc.RecordSection("cluster result");

        engine::SearchTask::TopkResult(insufficient_result, topk, ascending, src_result);
        ASSERT_TRUE(status.ok());
        rc.RecordSection("topk");

        CheckTopkResult(src_result, ascending, nq, topk);
        rc.RecordSection("check result");
    };

    DoTopk(5, 10, 4, false);
    DoTopk(20005, 998, 123, true);
    DoTopk(9987, 12, 10, false);
    DoTopk(77777, 1000, 1, false);
    DoTopk(5432, 8899, 8899, true);
}