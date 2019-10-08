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

using namespace milvus::scheduler;

namespace {

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

void CheckTopkResult(const std::vector<long> &input_ids_1,
                     const std::vector<float> &input_distance_1,
                     const std::vector<long> &input_ids_2,
                     const std::vector<float> &input_distance_2,
                     uint64_t nq,
                     uint64_t topk,
                     bool ascending,
                     const ResultSet& result) {
    ASSERT_EQ(result.size(), nq);
    ASSERT_EQ(input_ids_1.size(), input_distance_1.size());
    ASSERT_EQ(input_ids_2.size(), input_distance_2.size());

    uint64_t input_k1 = input_ids_1.size() / nq;
    uint64_t input_k2 = input_ids_2.size() / nq;

    for (int64_t i = 0; i < nq; i++) {
        std::vector<float> src_vec(input_distance_1.begin()+i*input_k1, input_distance_1.begin()+(i+1)*input_k1);
        src_vec.insert(src_vec.end(), input_distance_2.begin()+i*input_k2, input_distance_2.begin()+(i+1)*input_k2);
        if (ascending) {
            std::sort(src_vec.begin(), src_vec.end());
        } else {
            std::sort(src_vec.begin(), src_vec.end(), std::greater<float>());
        }

        uint64_t n = std::min(topk, input_k1+input_k2);
        for (uint64_t j = 0; j < n; j++) {
            if (src_vec[j] != result[i][j].second) {
                std::cout << src_vec[j] << " " << result[i][j].second << std::endl;
            }
            ASSERT_TRUE(src_vec[j] == result[i][j].second);
        }
    }
}

} // namespace

TEST(DBSearchTest, TOPK_TEST) {
    uint64_t NQ = 15;
    uint64_t TOP_K = 64;
    bool ascending;
    std::vector<long> ids1, ids2;
    std::vector<float> dist1, dist2;
    ResultSet result;
    milvus::Status status;

    /* test1, id1/dist1 valid, id2/dist2 empty */
    ascending = true;
    BuildResult(NQ, TOP_K, ascending, ids1, dist1);
    status = XSearchTask::TopkResult(ids1, dist1, TOP_K, NQ, TOP_K, ascending, result);
    ASSERT_TRUE(status.ok());
    CheckTopkResult(ids1, dist1, ids2, dist2, NQ, TOP_K, ascending, result);

    /* test2, id1/dist1 valid, id2/dist2 valid */
    BuildResult(NQ, TOP_K, ascending, ids2, dist2);
    status = XSearchTask::TopkResult(ids2, dist2, TOP_K, NQ, TOP_K, ascending, result);
    ASSERT_TRUE(status.ok());
    CheckTopkResult(ids1, dist1, ids2, dist2, NQ, TOP_K, ascending, result);

    /* test3, id1/dist1 small topk */
    ids1.clear();
    dist1.clear();
    result.clear();
    BuildResult(NQ, TOP_K/2, ascending, ids1, dist1);
    status = XSearchTask::TopkResult(ids1, dist1, TOP_K/2, NQ, TOP_K, ascending, result);
    ASSERT_TRUE(status.ok());
    status = XSearchTask::TopkResult(ids2, dist2, TOP_K, NQ, TOP_K, ascending, result);
    ASSERT_TRUE(status.ok());
    CheckTopkResult(ids1, dist1, ids2, dist2, NQ, TOP_K, ascending, result);

    /* test4, id1/dist1 small topk, id2/dist2 small topk */
    ids2.clear();
    dist2.clear();
    result.clear();
    BuildResult(NQ, TOP_K/3, ascending, ids2, dist2);
    status = XSearchTask::TopkResult(ids1, dist1, TOP_K/2, NQ, TOP_K, ascending, result);
    ASSERT_TRUE(status.ok());
    status = XSearchTask::TopkResult(ids2, dist2, TOP_K/3, NQ, TOP_K, ascending, result);
    ASSERT_TRUE(status.ok());
    CheckTopkResult(ids1, dist1, ids2, dist2, NQ, TOP_K, ascending, result);

/////////////////////////////////////////////////////////////////////////////////////////
    ascending = false;
    ids1.clear();
    dist1.clear();
    ids2.clear();
    dist2.clear();
    result.clear();

    /* test1, id1/dist1 valid, id2/dist2 empty */
    BuildResult(NQ, TOP_K, ascending, ids1, dist1);
    status = XSearchTask::TopkResult(ids1, dist1, TOP_K, NQ, TOP_K, ascending, result);
    ASSERT_TRUE(status.ok());
    CheckTopkResult(ids1, dist1, ids2, dist2, NQ, TOP_K, ascending, result);

    /* test2, id1/dist1 valid, id2/dist2 valid */
    BuildResult(NQ, TOP_K, ascending, ids2, dist2);
    status = XSearchTask::TopkResult(ids2, dist2, TOP_K, NQ, TOP_K, ascending, result);
    ASSERT_TRUE(status.ok());
    CheckTopkResult(ids1, dist1, ids2, dist2, NQ, TOP_K, ascending, result);

    /* test3, id1/dist1 small topk */
    ids1.clear();
    dist1.clear();
    result.clear();
    BuildResult(NQ, TOP_K/2, ascending, ids1, dist1);
    status = XSearchTask::TopkResult(ids1, dist1, TOP_K/2, NQ, TOP_K, ascending, result);
    ASSERT_TRUE(status.ok());
    status = XSearchTask::TopkResult(ids2, dist2, TOP_K, NQ, TOP_K, ascending, result);
    ASSERT_TRUE(status.ok());
    CheckTopkResult(ids1, dist1, ids2, dist2, NQ, TOP_K, ascending, result);

    /* test4, id1/dist1 small topk, id2/dist2 small topk */
    ids2.clear();
    dist2.clear();
    result.clear();
    BuildResult(NQ, TOP_K/3, ascending, ids2, dist2);
    status = XSearchTask::TopkResult(ids1, dist1, TOP_K/2, NQ, TOP_K, ascending, result);
    ASSERT_TRUE(status.ok());
    status = XSearchTask::TopkResult(ids2, dist2, TOP_K/3, NQ, TOP_K, ascending, result);
    ASSERT_TRUE(status.ok());
    CheckTopkResult(ids1, dist1, ids2, dist2, NQ, TOP_K, ascending, result);
}

TEST(DBSearchTest, REDUCE_PERF_TEST) {
    int32_t nq = 100;
    int32_t top_k = 1000;
    int32_t index_file_num = 478;   /* sift1B dataset, index files num */
    bool ascending = true;
    std::vector<long> input_ids;
    std::vector<float> input_distance;
    ResultSet final_result;
    milvus::Status status;

    double span, reduce_cost = 0.0;
    milvus::TimeRecorder rc("");

    for (int32_t i = 0; i < index_file_num; i++) {
        BuildResult(nq, top_k, ascending, input_ids, input_distance);

        rc.RecordSection("do search for context: " + std::to_string(i));

        // pick up topk result
        status = XSearchTask::TopkResult(input_ids, input_distance, top_k, nq, top_k, ascending, final_result);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(final_result.size(), nq);

        span = rc.RecordSection("reduce topk for context: " + std::to_string(i));
        reduce_cost += span;
    }
    std::cout << "total reduce time: " << reduce_cost/1000 << " ms" << std::endl;
}
