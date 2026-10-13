// Copyright (C) 2019-2026 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include <gtest/gtest.h>
#include <cmath>
#include <cstdlib>
#include <limits>
#include <numeric>
#include <vector>

#include "clustering/analyze_c.h"
#include "common/EasyAssert.h"
#include "pb/clustering.pb.h"

namespace {
std::string
CentroidBytes(int count) {
    milvus::proto::clustering::ClusteringCentroidsStats stats;
    for (int i = 0; i < count; ++i) {
        auto* centroid = stats.add_centroids();
        centroid->set_dim(4);
        for (float value : {float(i % 2 * 10000 + i / 2),
                            float(i % 31),
                            float(i % 11),
                            1.0f}) {
            centroid->mutable_float_vector()->add_data(value);
        }
    }
    return stats.SerializeAsString();
}

void
CheckSuccess(CStatus status) {
    EXPECT_EQ(status.error_code, 0)
        << (status.error_msg ? status.error_msg : "");
    if (status.error_code != 0) {
        free(const_cast<char*>(status.error_msg));
    }
}

void
CheckFailure(CStatus status) {
    EXPECT_NE(status.error_code, 0);
    if (status.error_code != 0) {
        free(const_cast<char*>(status.error_msg));
    }
}
}  // namespace

TEST(CompactionPlanTest, CardinalCoverageAndOwnership) {
    const auto bytes = CentroidBytes(4096);
    std::vector<uint64_t> counts(4096, 1);
    counts[0] = 1000;
    counts[1] = 0;
    CClusteringCompactionPlan plan = nullptr;
    auto status = BuildClusteringCompactionPlan(
        &plan,
        reinterpret_cast<const uint8_t*>(bytes.data()),
        bytes.size(),
        101,
        counts.data(),
        counts.size(),
        "CARDINAL_KMEANS",
        R"({"compaction_max_rows":128,"random_state":42,"search_quant_type":"NONE","refine_quant_type":"NONE"})");
    CheckSuccess(status);
    ASSERT_NE(plan, nullptr);
    uint64_t rows = 0, groups = 0;
    uint32_t clusters = 0;
    const uint64_t* result_counts = nullptr;
    CheckSuccess(GetClusteringCompactionPlanMeta(
        plan, &rows, &clusters, &result_counts, &groups));
    EXPECT_EQ(clusters, counts.size());
    EXPECT_EQ(rows, std::accumulate(counts.begin(), counts.end(), uint64_t(0)));
    EXPECT_GT(groups, 1);
    std::vector<bool> seen(counts.size(), false);
    uint64_t group_rows = 0;
    for (uint64_t i = 0; i < groups; ++i) {
        CClusteringGroup group{};
        CheckSuccess(GetClusteringCompactionPlanGroup(plan, i, &group));
        uint64_t expected = 0;
        for (uint64_t j = 0; j < group.count; ++j) {
            const auto offset = group.offsets[j];
            ASSERT_LT(offset, counts.size());
            EXPECT_FALSE(seen[offset]);
            EXPECT_GT(counts[offset], 0);
            seen[offset] = true;
            expected += counts[offset];
        }
        EXPECT_EQ(group.rows, expected);
        group_rows += group.rows;
    }
    EXPECT_EQ(group_rows, rows);
    EXPECT_FALSE(seen[1]);
    counts[0] = 2;
    EXPECT_EQ(result_counts[0], 1000)
        << "handle owns counts independently of caller";
    CClusteringGroup invalid{};
    CheckFailure(GetClusteringCompactionPlanGroup(plan, groups, &invalid));
    CheckSuccess(DeleteClusteringCompactionPlan(plan));
}

TEST(CompactionPlanTest, InvalidInputDoesNotPublishHandle) {
    const auto bytes = CentroidBytes(2);
    uint64_t counts[] = {1, 1};
    for (const auto* provider : {"KMEANS", "unknown-provider"}) {
        CClusteringCompactionPlan plan = reinterpret_cast<void*>(1);
        CheckFailure(BuildClusteringCompactionPlan(
            &plan,
            reinterpret_cast<const uint8_t*>(bytes.data()),
            bytes.size(),
            101,
            counts,
            2,
            provider,
            R"({"compaction_max_rows":10})"));
        EXPECT_EQ(plan, nullptr);
    }
    CClusteringCompactionPlan plan = nullptr;
    counts[0] = std::numeric_limits<int64_t>::max();
    CheckFailure(BuildClusteringCompactionPlan(
        &plan,
        reinterpret_cast<const uint8_t*>(bytes.data()),
        bytes.size(),
        101,
        counts,
        2,
        "CARDINAL_KMEANS",
        "{}"));
    EXPECT_EQ(plan, nullptr);
    CheckFailure(BuildClusteringCompactionPlan(
        &plan,
        reinterpret_cast<const uint8_t*>(bytes.data()),
        bytes.size(),
        101,
        nullptr,
        2,
        "CARDINAL_KMEANS",
        "{}"));
    CheckFailure(GetClusteringCompactionPlanMeta(
        nullptr, nullptr, nullptr, nullptr, nullptr));
    CheckSuccess(DeleteClusteringCompactionPlan(nullptr));
}

TEST(CompactionPlanTest, RejectsMalformedCentroids) {
    milvus::proto::clustering::ClusteringCentroidsStats stats;
    ASSERT_TRUE(stats.ParseFromString(CentroidBytes(2)));
    stats.mutable_centroids(1)->mutable_float_vector()->set_data(
        0, std::numeric_limits<float>::infinity());
    const auto bytes = stats.SerializeAsString();
    const uint64_t counts[] = {1, 1};
    CClusteringCompactionPlan plan = nullptr;
    CheckFailure(BuildClusteringCompactionPlan(
        &plan,
        reinterpret_cast<const uint8_t*>(bytes.data()),
        bytes.size(),
        101,
        counts,
        2,
        "CARDINAL_KMEANS",
        "{}"));
    EXPECT_EQ(plan, nullptr);
}
