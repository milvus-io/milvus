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
#include "utils/ThreadPool.h"

namespace {

namespace ms = milvus::scheduler;

void
BuildResult(uint64_t nq,
            uint64_t topk,
            bool ascending,
            std::vector<int64_t>& output_ids,
            std::vector<float>& output_distence) {
    output_ids.clear();
    output_ids.resize(nq * topk);
    output_distence.clear();
    output_distence.resize(nq * topk);

    for (uint64_t i = 0; i < nq; i++) {
        for (uint64_t j = 0; j < topk; j++) {
            output_ids[i * topk + j] = (int64_t)(drand48() * 100000);
            output_distence[i * topk + j] = ascending ? (j + drand48()) : ((topk - j) + drand48());
        }
    }
}

void
CheckTopkResult(const std::vector<int64_t>& input_ids_1,
                const std::vector<float>& input_distance_1,
                const std::vector<int64_t>& input_ids_2,
                const std::vector<float>& input_distance_2,
                uint64_t nq,
                uint64_t topk,
                bool ascending,
                const milvus::scheduler::ResultSet& result) {
    ASSERT_EQ(result.size(), nq);
    ASSERT_EQ(input_ids_1.size(), input_distance_1.size());
    ASSERT_EQ(input_ids_2.size(), input_distance_2.size());

    uint64_t input_k1 = input_ids_1.size() / nq;
    uint64_t input_k2 = input_ids_2.size() / nq;

    for (int64_t i = 0; i < nq; i++) {
        std::vector<float>
            src_vec(input_distance_1.begin() + i * input_k1, input_distance_1.begin() + (i + 1) * input_k1);
        src_vec.insert(src_vec.end(),
                       input_distance_2.begin() + i * input_k2,
                       input_distance_2.begin() + (i + 1) * input_k2);
        if (ascending) {
            std::sort(src_vec.begin(), src_vec.end());
        } else {
            std::sort(src_vec.begin(), src_vec.end(), std::greater<float>());
        }

        uint64_t n = std::min(topk, input_k1 + input_k2);
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
    std::vector<int64_t> ids1, ids2;
    std::vector<float> dist1, dist2;
    ms::ResultSet result;

    /* test1, id1/dist1 valid, id2/dist2 empty */
    ascending = true;
    BuildResult(NQ, TOP_K, ascending, ids1, dist1);
    ms::XSearchTask::MergeTopkToResultSet(ids1, dist1, TOP_K, NQ, TOP_K, ascending, result);
    CheckTopkResult(ids1, dist1, ids2, dist2, NQ, TOP_K, ascending, result);

    /* test2, id1/dist1 valid, id2/dist2 valid */
    BuildResult(NQ, TOP_K, ascending, ids2, dist2);
    ms::XSearchTask::MergeTopkToResultSet(ids2, dist2, TOP_K, NQ, TOP_K, ascending, result);
    CheckTopkResult(ids1, dist1, ids2, dist2, NQ, TOP_K, ascending, result);

    /* test3, id1/dist1 small topk */
    ids1.clear();
    dist1.clear();
    result.clear();
    BuildResult(NQ, TOP_K/2, ascending, ids1, dist1);
    ms::XSearchTask::MergeTopkToResultSet(ids1, dist1, TOP_K/2, NQ, TOP_K, ascending, result);
    ms::XSearchTask::MergeTopkToResultSet(ids2, dist2, TOP_K, NQ, TOP_K, ascending, result);
    CheckTopkResult(ids1, dist1, ids2, dist2, NQ, TOP_K, ascending, result);

    /* test4, id1/dist1 small topk, id2/dist2 small topk */
    ids2.clear();
    dist2.clear();
    result.clear();
    BuildResult(NQ, TOP_K/3, ascending, ids2, dist2);
    ms::XSearchTask::MergeTopkToResultSet(ids1, dist1, TOP_K/2, NQ, TOP_K, ascending, result);
    ms::XSearchTask::MergeTopkToResultSet(ids2, dist2, TOP_K/3, NQ, TOP_K, ascending, result);
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
    ms::XSearchTask::MergeTopkToResultSet(ids1, dist1, TOP_K, NQ, TOP_K, ascending, result);
    CheckTopkResult(ids1, dist1, ids2, dist2, NQ, TOP_K, ascending, result);

    /* test2, id1/dist1 valid, id2/dist2 valid */
    BuildResult(NQ, TOP_K, ascending, ids2, dist2);
    ms::XSearchTask::MergeTopkToResultSet(ids2, dist2, TOP_K, NQ, TOP_K, ascending, result);
    CheckTopkResult(ids1, dist1, ids2, dist2, NQ, TOP_K, ascending, result);

    /* test3, id1/dist1 small topk */
    ids1.clear();
    dist1.clear();
    result.clear();
    BuildResult(NQ, TOP_K/2, ascending, ids1, dist1);
    ms::XSearchTask::MergeTopkToResultSet(ids1, dist1, TOP_K/2, NQ, TOP_K, ascending, result);
    ms::XSearchTask::MergeTopkToResultSet(ids2, dist2, TOP_K, NQ, TOP_K, ascending, result);
    CheckTopkResult(ids1, dist1, ids2, dist2, NQ, TOP_K, ascending, result);

    /* test4, id1/dist1 small topk, id2/dist2 small topk */
    ids2.clear();
    dist2.clear();
    result.clear();
    BuildResult(NQ, TOP_K/3, ascending, ids2, dist2);
    ms::XSearchTask::MergeTopkToResultSet(ids1, dist1, TOP_K/2, NQ, TOP_K, ascending, result);
    ms::XSearchTask::MergeTopkToResultSet(ids2, dist2, TOP_K/3, NQ, TOP_K, ascending, result);
    CheckTopkResult(ids1, dist1, ids2, dist2, NQ, TOP_K, ascending, result);
}

TEST(DBSearchTest, REDUCE_PERF_TEST) {
    int32_t nq = 100;
    int32_t top_k = 1000;
    int32_t index_file_num = 478;   /* sift1B dataset, index files num */
    bool ascending = true;
    std::vector<std::vector<int64_t>> id_vec;
    std::vector<std::vector<float>> dist_vec;
    std::vector<uint64_t> k_vec;
    std::vector<int64_t> input_ids;
    std::vector<float> input_distance;
    ms::ResultSet final_result, final_result_2, final_result_3;

    int32_t i, k, step;
    double reduce_cost = 0.0;
    milvus::TimeRecorder rc("");

    for (i = 0; i < index_file_num; i++) {
        BuildResult(nq, top_k, ascending, input_ids, input_distance);
        id_vec.push_back(input_ids);
        dist_vec.push_back(input_distance);
        k_vec.push_back(top_k);
    }

    rc.RecordSection("Method-1 result reduce start");

    /* method-1 */
    for (i = 0; i < index_file_num; i++) {
        ms::XSearchTask::MergeTopkToResultSet(id_vec[i], dist_vec[i], k_vec[i], nq, top_k, ascending, final_result);
        ASSERT_EQ(final_result.size(), nq);
    }

    reduce_cost = rc.RecordSection("Method-1 result reduce done");
    std::cout << "Method-1: total reduce time " << reduce_cost/1000 << " ms" << std::endl;

    /* method-2 */
    std::vector<std::vector<int64_t>> id_vec_2(id_vec);
    std::vector<std::vector<float>> dist_vec_2(dist_vec);
    std::vector<uint64_t> k_vec_2(k_vec);

    rc.RecordSection("Method-2 result reduce start");

    for (step = 1; step < index_file_num; step *= 2) {
        for (i = 0; i+step < index_file_num; i += step*2) {
            ms::XSearchTask::MergeTopkArray(id_vec_2[i], dist_vec_2[i], k_vec_2[i],
                                            id_vec_2[i+step], dist_vec_2[i+step], k_vec_2[i+step],
                                            nq, top_k, ascending);
        }
    }
    ms::XSearchTask::MergeTopkToResultSet(id_vec_2[0], dist_vec_2[0], k_vec_2[0], nq, top_k, ascending, final_result_2);
    ASSERT_EQ(final_result_2.size(), nq);

    reduce_cost = rc.RecordSection("Method-2 result reduce done");
    std::cout << "Method-2: total reduce time " << reduce_cost/1000 << " ms" << std::endl;

    for (i = 0; i < nq; i++) {
        ASSERT_EQ(final_result[i].size(), final_result_2[i].size());
        for (k = 0; k < final_result.size(); k++) {
            ASSERT_EQ(final_result[i][k].first, final_result_2[i][k].first);
            ASSERT_EQ(final_result[i][k].second, final_result_2[i][k].second);
        }
    }

    /* method-3 parallel */
    std::vector<std::vector<int64_t>> id_vec_3(id_vec);
    std::vector<std::vector<float>> dist_vec_3(dist_vec);
    std::vector<uint64_t> k_vec_3(k_vec);

    uint32_t max_thread_count = std::min(std::thread::hardware_concurrency() - 1, (uint32_t)MAX_THREADS_NUM);
    milvus::ThreadPool threadPool(max_thread_count);
    std::list<std::future<void>> threads_list;

    rc.RecordSection("Method-3 parallel result reduce start");

    for (step = 1; step < index_file_num; step *= 2) {
        for (i = 0; i+step < index_file_num; i += step*2) {
            threads_list.push_back(
                threadPool.enqueue(ms::XSearchTask::MergeTopkArray,
                                   std::ref(id_vec_3[i]), std::ref(dist_vec_3[i]), std::ref(k_vec_3[i]),
                                   std::ref(id_vec_3[i+step]), std::ref(dist_vec_3[i+step]), std::ref(k_vec_3[i+step]),
                                   nq, top_k, ascending));
        }

        while (threads_list.size() > 0) {
            int nready = 0;
            for (auto it = threads_list.begin(); it != threads_list.end(); it = it) {
                auto &p = *it;
                std::chrono::milliseconds span(0);
                if (p.wait_for(span) == std::future_status::ready) {
                    threads_list.erase(it++);
                    ++nready;
                } else {
                    ++it;
                }
            }

            if (nready == 0) {
                std::this_thread::yield();
            }
        }
    }
    ms::XSearchTask::MergeTopkToResultSet(id_vec_3[0], dist_vec_3[0], k_vec_3[0], nq, top_k, ascending, final_result_3);
    ASSERT_EQ(final_result_3.size(), nq);

    reduce_cost = rc.RecordSection("Method-3 parallel result reduce done");
    std::cout << "Method-3 parallel: total reduce time " << reduce_cost/1000 << " ms" << std::endl;

    for (i = 0; i < nq; i++) {
        ASSERT_EQ(final_result[i].size(), final_result_3[i].size());
        for (k = 0; k < final_result.size(); k++) {
            ASSERT_EQ(final_result[i][k].first, final_result_3[i][k].first);
            ASSERT_EQ(final_result[i][k].second, final_result_3[i][k].second);
        }
    }
}
