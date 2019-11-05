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
#include "utils/ThreadPool.h"
#include "utils/TimeRecorder.h"

namespace {

namespace ms = milvus::scheduler;

void
BuildResult(std::vector<int64_t>& output_ids, std::vector<float>& output_distance, uint64_t input_k, uint64_t topk,
            uint64_t nq, bool ascending) {
    output_ids.clear();
    output_ids.resize(nq * topk);
    output_distance.clear();
    output_distance.resize(nq * topk);

    for (uint64_t i = 0; i < nq; i++) {
        // insert valid items
        for (uint64_t j = 0; j < input_k; j++) {
            output_ids[i * topk + j] = (int64_t)(drand48() * 100000);
            output_distance[i * topk + j] = ascending ? (j + drand48()) : ((input_k - j) + drand48());
        }
        // insert invalid items
        for (uint64_t j = input_k; j < topk; j++) {
            output_ids[i * topk + j] = -1;
            output_distance[i * topk + j] = -1.0;
        }
    }
}

void
CopyResult(std::vector<int64_t>& output_ids, std::vector<float>& output_distance, uint64_t output_topk,
           std::vector<int64_t>& input_ids, std::vector<float>& input_distance, uint64_t input_topk, uint64_t nq) {
    ASSERT_TRUE(input_ids.size() >= nq * input_topk);
    ASSERT_TRUE(input_distance.size() >= nq * input_topk);
    ASSERT_TRUE(output_topk <= input_topk);
    output_ids.clear();
    output_ids.resize(nq * output_topk);
    output_distance.clear();
    output_distance.resize(nq * output_topk);

    for (uint64_t i = 0; i < nq; i++) {
        for (uint64_t j = 0; j < output_topk; j++) {
            output_ids[i * output_topk + j] = input_ids[i * input_topk + j];
            output_distance[i * output_topk + j] = input_distance[i * input_topk + j];
        }
    }
}

void
CheckTopkResult(const std::vector<int64_t>& input_ids_1, const std::vector<float>& input_distance_1,
                const std::vector<int64_t>& input_ids_2, const std::vector<float>& input_distance_2, uint64_t topk,
                uint64_t nq, bool ascending, const milvus::scheduler::ResultSet& result) {
    ASSERT_EQ(result.size(), nq);
    ASSERT_EQ(input_ids_1.size(), input_distance_1.size());
    ASSERT_EQ(input_ids_2.size(), input_distance_2.size());

    for (int64_t i = 0; i < nq; i++) {
        std::vector<float> src_vec(input_distance_1.begin() + i * topk, input_distance_1.begin() + (i + 1) * topk);
        src_vec.insert(src_vec.end(), input_distance_2.begin() + i * topk, input_distance_2.begin() + (i + 1) * topk);
        if (ascending) {
            std::sort(src_vec.begin(), src_vec.end());
        } else {
            std::sort(src_vec.begin(), src_vec.end(), std::greater<float>());
        }

        // erase invalid items
        std::vector<float>::iterator iter;
        for (iter = src_vec.begin(); iter != src_vec.end();) {
            if (*iter < 0.0)
                iter = src_vec.erase(iter);
            else
                ++iter;
        }

        uint64_t n = std::min(topk, result[i].size());
        for (uint64_t j = 0; j < n; j++) {
            if (result[i][j].first < 0) {
                continue;
            }
            if (src_vec[j] != result[i][j].second) {
                std::cout << src_vec[j] << " " << result[i][j].second << std::endl;
            }
            ASSERT_TRUE(src_vec[j] == result[i][j].second);
        }
    }
}

}  // namespace

void
MergeTopkToResultSetTest(uint64_t topk_1, uint64_t topk_2, uint64_t nq, uint64_t topk, bool ascending) {
    std::vector<int64_t> ids1, ids2;
    std::vector<float> dist1, dist2;
    ms::ResultSet result;
    BuildResult(ids1, dist1, topk_1, topk, nq, ascending);
    BuildResult(ids2, dist2, topk_2, topk, nq, ascending);
    ms::XSearchTask::MergeTopkToResultSet(ids1, dist1, topk_1, nq, topk, ascending, result);
    ms::XSearchTask::MergeTopkToResultSet(ids2, dist2, topk_2, nq, topk, ascending, result);
    CheckTopkResult(ids1, dist1, ids2, dist2, topk, nq, ascending, result);
}

TEST(DBSearchTest, MERGE_RESULT_SET_TEST) {
    uint64_t NQ = 15;
    uint64_t TOP_K = 64;

    /* test1, id1/dist1 valid, id2/dist2 empty */
    MergeTopkToResultSetTest(TOP_K, 0, NQ, TOP_K, true);
    MergeTopkToResultSetTest(TOP_K, 0, NQ, TOP_K, false);

    /* test2, id1/dist1 valid, id2/dist2 valid */
    MergeTopkToResultSetTest(TOP_K, TOP_K, NQ, TOP_K, true);
    MergeTopkToResultSetTest(TOP_K, TOP_K, NQ, TOP_K, false);

    /* test3, id1/dist1 small topk */
    MergeTopkToResultSetTest(TOP_K / 2, TOP_K, NQ, TOP_K, true);
    MergeTopkToResultSetTest(TOP_K / 2, TOP_K, NQ, TOP_K, false);

    /* test4, id1/dist1 small topk, id2/dist2 small topk */
    MergeTopkToResultSetTest(TOP_K / 2, TOP_K / 3, NQ, TOP_K, true);
    MergeTopkToResultSetTest(TOP_K / 2, TOP_K / 3, NQ, TOP_K, false);
}

// void MergeTopkArrayTest(uint64_t topk_1, uint64_t topk_2, uint64_t nq, uint64_t topk, bool ascending) {
//    std::vector<int64_t> ids1, ids2;
//    std::vector<float> dist1, dist2;
//    ms::ResultSet result;
//    BuildResult(ids1, dist1, topk_1, topk, nq, ascending);
//    BuildResult(ids2, dist2, topk_2, topk, nq, ascending);
//    uint64_t result_topk = std::min(topk, topk_1 + topk_2);
//    ms::XSearchTask::MergeTopkArray(ids1, dist1, topk_1, ids2, dist2, topk_2, nq, topk, ascending);
//    if (ids1.size() != result_topk * nq) {
//        std::cout << ids1.size() << " " << result_topk * nq << std::endl;
//    }
//    ASSERT_TRUE(ids1.size() == result_topk * nq);
//    ASSERT_TRUE(dist1.size() == result_topk * nq);
//    for (uint64_t i = 0; i < nq; i++) {
//        for (uint64_t k = 1; k < result_topk; k++) {
//            float f0 = dist1[i * topk + k - 1];
//            float f1 = dist1[i * topk + k];
//            if (ascending) {
//                if (f1 < f0) {
//                    std::cout << f0 << " " << f1 << std::endl;
//                }
//                ASSERT_TRUE(f1 >= f0);
//            } else {
//                if (f1 > f0) {
//                    std::cout << f0 << " " << f1 << std::endl;
//                }
//                ASSERT_TRUE(f1 <= f0);
//            }
//        }
//    }
//}

// TEST(DBSearchTest, MERGE_ARRAY_TEST) {
//    uint64_t NQ = 15;
//    uint64_t TOP_K = 64;
//
//    /* test1, id1/dist1 valid, id2/dist2 empty */
//    MergeTopkArrayTest(TOP_K, 0, NQ, TOP_K, true);
//    MergeTopkArrayTest(TOP_K, 0, NQ, TOP_K, false);
//    MergeTopkArrayTest(0, TOP_K, NQ, TOP_K, true);
//    MergeTopkArrayTest(0, TOP_K, NQ, TOP_K, false);

//    /* test2, id1/dist1 valid, id2/dist2 valid */
//    MergeTopkArrayTest(TOP_K, TOP_K, NQ, TOP_K, true);
//    MergeTopkArrayTest(TOP_K, TOP_K, NQ, TOP_K, false);
//
//    /* test3, id1/dist1 small topk */
//    MergeTopkArrayTest(TOP_K/2, TOP_K, NQ, TOP_K, true);
//    MergeTopkArrayTest(TOP_K/2, TOP_K, NQ, TOP_K, false);
//    MergeTopkArrayTest(TOP_K, TOP_K/2, NQ, TOP_K, true);
//    MergeTopkArrayTest(TOP_K, TOP_K/2, NQ, TOP_K, false);
//
//    /* test4, id1/dist1 small topk, id2/dist2 small topk */
//    MergeTopkArrayTest(TOP_K/2, TOP_K/3, NQ, TOP_K, true);
//    MergeTopkArrayTest(TOP_K/2, TOP_K/3, NQ, TOP_K, false);
//    MergeTopkArrayTest(TOP_K/3, TOP_K/2, NQ, TOP_K, true);
//    MergeTopkArrayTest(TOP_K/3, TOP_K/2, NQ, TOP_K, false);
//}

TEST(DBSearchTest, REDUCE_PERF_TEST) {
    int32_t index_file_num = 478; /* sift1B dataset, index files num */
    bool ascending = true;

    std::vector<int32_t> thread_vec = {4, 8};
    std::vector<int32_t> nq_vec = {1, 10, 100};
    std::vector<int32_t> topk_vec = {1, 4, 16, 64};
    int32_t NQ = nq_vec[nq_vec.size() - 1];
    int32_t TOPK = topk_vec[topk_vec.size() - 1];

    std::vector<std::vector<int64_t>> id_vec;
    std::vector<std::vector<float>> dist_vec;
    std::vector<int64_t> input_ids;
    std::vector<float> input_distance;
    int32_t i, k, step;

    /* generate testing data */
    for (i = 0; i < index_file_num; i++) {
        BuildResult(input_ids, input_distance, TOPK, TOPK, NQ, ascending);
        id_vec.push_back(input_ids);
        dist_vec.push_back(input_distance);
    }

    for (int32_t max_thread_num : thread_vec) {
        milvus::ThreadPool threadPool(max_thread_num);
        std::list<std::future<void>> threads_list;

        for (int32_t nq : nq_vec) {
            for (int32_t top_k : topk_vec) {
                ms::ResultSet final_result, final_result_2, final_result_3;

                std::vector<std::vector<int64_t>> id_vec_1(index_file_num);
                std::vector<std::vector<float>> dist_vec_1(index_file_num);
                for (i = 0; i < index_file_num; i++) {
                    CopyResult(id_vec_1[i], dist_vec_1[i], top_k, id_vec[i], dist_vec[i], TOPK, nq);
                }

                std::string str1 = "Method-1 " + std::to_string(max_thread_num) + " " + std::to_string(nq) + " " +
                                   std::to_string(top_k);
                milvus::TimeRecorder rc1(str1);

                ///////////////////////////////////////////////////////////////////////////////////////
                /* method-1 */
                for (i = 0; i < index_file_num; i++) {
                    ms::XSearchTask::MergeTopkToResultSet(id_vec_1[i], dist_vec_1[i], top_k, nq, top_k, ascending,
                                                          final_result);
                    ASSERT_EQ(final_result.size(), nq);
                }

                rc1.RecordSection("reduce done");

                //                ///////////////////////////////////////////////////////////////////////////////////////
                //                /* method-2 */
                //                std::vector<std::vector<int64_t>> id_vec_2(index_file_num);
                //                std::vector<std::vector<float>> dist_vec_2(index_file_num);
                //                std::vector<uint64_t> k_vec_2(index_file_num);
                //                for (i = 0; i < index_file_num; i++) {
                //                    CopyResult(id_vec_2[i], dist_vec_2[i], top_k, id_vec[i], dist_vec[i], TOPK, nq);
                //                    k_vec_2[i] = top_k;
                //                }
                //
                //                std::string str2 = "Method-2 " + std::to_string(max_thread_num) + " " +
                //                                    std::to_string(nq) + " " + std::to_string(top_k);
                //                milvus::TimeRecorder rc2(str2);
                //
                //                for (step = 1; step < index_file_num; step *= 2) {
                //                    for (i = 0; i + step < index_file_num; i += step * 2) {
                //                        ms::XSearchTask::MergeTopkArray(id_vec_2[i], dist_vec_2[i], k_vec_2[i],
                //                                                        id_vec_2[i + step], dist_vec_2[i + step],
                //                                                        k_vec_2[i + step], nq, top_k, ascending);
                //                    }
                //                }
                //                ms::XSearchTask::MergeTopkToResultSet(id_vec_2[0],
                //                                                      dist_vec_2[0],
                //                                                      k_vec_2[0],
                //                                                      nq,
                //                                                      top_k,
                //                                                      ascending,
                //                                                      final_result_2);
                //                ASSERT_EQ(final_result_2.size(), nq);
                //
                //                rc2.RecordSection("reduce done");
                //
                //                for (i = 0; i < nq; i++) {
                //                    ASSERT_EQ(final_result[i].size(), final_result_2[i].size());
                //                    for (k = 0; k < final_result[i].size(); k++) {
                //                        if (final_result[i][k].first != final_result_2[i][k].first) {
                //                            std::cout << i << " " << k << std::endl;
                //                        }
                //                        ASSERT_EQ(final_result[i][k].first, final_result_2[i][k].first);
                //                        ASSERT_EQ(final_result[i][k].second, final_result_2[i][k].second);
                //                    }
                //                }
                //
                //                ///////////////////////////////////////////////////////////////////////////////////////
                //                /* method-3 parallel */
                //                std::vector<std::vector<int64_t>> id_vec_3(index_file_num);
                //                std::vector<std::vector<float>> dist_vec_3(index_file_num);
                //                std::vector<uint64_t> k_vec_3(index_file_num);
                //                for (i = 0; i < index_file_num; i++) {
                //                    CopyResult(id_vec_3[i], dist_vec_3[i], top_k, id_vec[i], dist_vec[i], TOPK, nq);
                //                    k_vec_3[i] = top_k;
                //                }
                //
                //                std::string str3 = "Method-3 " + std::to_string(max_thread_num) + " " +
                //                                    std::to_string(nq) + " " + std::to_string(top_k);
                //                milvus::TimeRecorder rc3(str3);
                //
                //                for (step = 1; step < index_file_num; step *= 2) {
                //                    for (i = 0; i + step < index_file_num; i += step * 2) {
                //                        threads_list.push_back(
                //                            threadPool.enqueue(ms::XSearchTask::MergeTopkArray,
                //                                               std::ref(id_vec_3[i]),
                //                                               std::ref(dist_vec_3[i]),
                //                                               std::ref(k_vec_3[i]),
                //                                               std::ref(id_vec_3[i + step]),
                //                                               std::ref(dist_vec_3[i + step]),
                //                                               std::ref(k_vec_3[i + step]),
                //                                               nq,
                //                                               top_k,
                //                                               ascending));
                //                    }
                //
                //                    while (threads_list.size() > 0) {
                //                        int nready = 0;
                //                        for (auto it = threads_list.begin(); it != threads_list.end(); it = it) {
                //                            auto &p = *it;
                //                            std::chrono::milliseconds span(0);
                //                            if (p.wait_for(span) == std::future_status::ready) {
                //                                threads_list.erase(it++);
                //                                ++nready;
                //                            } else {
                //                                ++it;
                //                            }
                //                        }
                //
                //                        if (nready == 0) {
                //                            std::this_thread::yield();
                //                        }
                //                    }
                //                }
                //                ms::XSearchTask::MergeTopkToResultSet(id_vec_3[0],
                //                                                      dist_vec_3[0],
                //                                                      k_vec_3[0],
                //                                                      nq,
                //                                                      top_k,
                //                                                      ascending,
                //                                                      final_result_3);
                //                ASSERT_EQ(final_result_3.size(), nq);
                //
                //                rc3.RecordSection("reduce done");
                //
                //                for (i = 0; i < nq; i++) {
                //                    ASSERT_EQ(final_result[i].size(), final_result_3[i].size());
                //                    for (k = 0; k < final_result[i].size(); k++) {
                //                        ASSERT_EQ(final_result[i][k].first, final_result_3[i][k].first);
                //                        ASSERT_EQ(final_result[i][k].second, final_result_3[i][k].second);
                //                    }
                //                }
            }
        }
    }
}
