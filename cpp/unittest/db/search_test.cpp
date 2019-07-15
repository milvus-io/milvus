////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include <gtest/gtest.h>

#include "db/scheduler/task/SearchTask.h"
#include <cmath>
#include <vector>

using namespace zilliz::milvus;

namespace {

static constexpr uint64_t NQ = 15;
static constexpr uint64_t TOP_K = 64;

void BuildResult(uint64_t nq,
                 uint64_t top_k,
                 std::vector<long> &output_ids,
                 std::vector<float> &output_distence) {
    output_ids.clear();
    output_ids.resize(nq*top_k);
    output_distence.clear();
    output_distence.resize(nq*top_k);

    for(uint64_t i = 0; i < nq; i++) {
        for(uint64_t j = 0; j < top_k; j++) {
            output_ids[i * top_k + j] = (long)(drand48()*100000);
            output_distence[i * top_k + j] = j + drand48();
        }
    }
}

void CheckResult(const engine::SearchContext::Id2DistanceMap& src_1,
        const engine::SearchContext::Id2DistanceMap& src_2,
        const engine::SearchContext::Id2DistanceMap& target) {
    for(uint64_t i = 0; i < target.size() - 1; i++) {
        ASSERT_LE(target[i].second, target[i + 1].second);
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

}

TEST(DBSearchTest, TOPK_TEST) {
    std::vector<long> target_ids;
    std::vector<float> target_distence;
    engine::SearchContext::ResultSet src_result;
    auto status = engine::SearchTask::ClusterResult(target_ids, target_distence, NQ, TOP_K, src_result);
    ASSERT_FALSE(status.ok());
    ASSERT_TRUE(src_result.empty());

    BuildResult(NQ, TOP_K, target_ids, target_distence);
    status = engine::SearchTask::ClusterResult(target_ids, target_distence, NQ, TOP_K, src_result);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(src_result.size(), NQ);

    engine::SearchContext::ResultSet target_result;
    status = engine::SearchTask::TopkResult(target_result, TOP_K, true, target_result);
    ASSERT_TRUE(status.ok());

    status = engine::SearchTask::TopkResult(target_result, TOP_K, true, src_result);
    ASSERT_FALSE(status.ok());

    status = engine::SearchTask::TopkResult(src_result, TOP_K, true, target_result);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(src_result.empty());
    ASSERT_EQ(target_result.size(), NQ);

    std::vector<long> src_ids;
    std::vector<float> src_distence;
    uint64_t wrong_topk = TOP_K - 10;
    BuildResult(NQ, wrong_topk, src_ids, src_distence);

    status = engine::SearchTask::ClusterResult(src_ids, src_distence, NQ, wrong_topk, src_result);
    ASSERT_TRUE(status.ok());

    status = engine::SearchTask::TopkResult(src_result, TOP_K, true, target_result);
    ASSERT_TRUE(status.ok());
    for(uint64_t i = 0; i < NQ; i++) {
        ASSERT_EQ(target_result[i].size(), TOP_K);
    }

    wrong_topk = TOP_K + 10;
    BuildResult(NQ, wrong_topk, src_ids, src_distence);

    status = engine::SearchTask::TopkResult(src_result, TOP_K, true, target_result);
    ASSERT_TRUE(status.ok());
    for(uint64_t i = 0; i < NQ; i++) {
        ASSERT_EQ(target_result[i].size(), TOP_K);
    }
}

TEST(DBSearchTest, MERGE_TEST) {
    std::vector<long> target_ids;
    std::vector<float> target_distence;
    std::vector<long> src_ids;
    std::vector<float> src_distence;
    engine::SearchContext::ResultSet src_result, target_result;

    uint64_t src_count = 5, target_count = 8;
    BuildResult(1, src_count, src_ids, src_distence);
    BuildResult(1, target_count, target_ids, target_distence);
    auto status = engine::SearchTask::ClusterResult(src_ids, src_distence, 1, src_count, src_result);
    ASSERT_TRUE(status.ok());
    status = engine::SearchTask::ClusterResult(target_ids, target_distence, 1, target_count, target_result);
    ASSERT_TRUE(status.ok());

    {
        engine::SearchContext::Id2DistanceMap src = src_result[0];
        engine::SearchContext::Id2DistanceMap target = target_result[0];
        status = engine::SearchTask::MergeResult(src, target, 10, true);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(target.size(), 10);
        CheckResult(src_result[0], target_result[0], target);
    }

    {
        engine::SearchContext::Id2DistanceMap src = src_result[0];
        engine::SearchContext::Id2DistanceMap target;
        status = engine::SearchTask::MergeResult(src, target, 10, true);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(target.size(), src_count);
        ASSERT_TRUE(src.empty());
        CheckResult(src_result[0], target_result[0], target);
    }

    {
        engine::SearchContext::Id2DistanceMap src = src_result[0];
        engine::SearchContext::Id2DistanceMap target = target_result[0];
        status = engine::SearchTask::MergeResult(src, target, 30, true);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(target.size(), src_count + target_count);
        CheckResult(src_result[0], target_result[0], target);
    }

    {
        engine::SearchContext::Id2DistanceMap target = src_result[0];
        engine::SearchContext::Id2DistanceMap src = target_result[0];
        status = engine::SearchTask::MergeResult(src, target, 30, true);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(target.size(), src_count + target_count);
        CheckResult(src_result[0], target_result[0], target);
    }
}
