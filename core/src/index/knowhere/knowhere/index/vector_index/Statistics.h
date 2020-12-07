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

#pragma once

#include <stdio.h>
#include <algorithm>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "knowhere/common/Log.h"
#include "knowhere/index/IndexType.h"

namespace milvus {
namespace knowhere {

extern int32_t STATISTICS_LEVEL;

inline uint64_t
upper_bound_of_pow2(uint64_t x) {
    --x;
    x |= (x >> 1);
    x |= (x >> 2);
    x |= (x >> 4);
    x |= (x >> 8);
    x |= (x >> 16);
    x |= (x >> 32);
    return x + 1;
}

inline int
len_of_pow2(uint64_t x) {
    return __builtin_popcountl(x - 1);
}

/*
 * class: Statistics
 */
class Statistics {
 public:
    static const size_t NQ_Histogram_Slices = 13;
    static const size_t Filter_Histogram_Slices = 21;

    explicit Statistics(std::string& idx_t)
        : index_type(idx_t),
          nq_cnt(0),
          batch_cnt(0),
          total_query_time(0.0),
          nq_stat(NQ_Histogram_Slices, 0),
          filter_stat(Filter_Histogram_Slices, 0) {
    }

    /*
     * Get index type
     * @retval: index type in string
     */
    const std::string&
    IndexType() {
        return index_type;
    }

    /*
     * To string (may be for log output)
     * @retval: string output
     */
    virtual std::string
    ToString();

    virtual ~Statistics() = default;

    /*
     * Clear all counts
     * @retval: none
     */
    virtual void
    Clear() {
        total_query_time = 0.0;
        nq_cnt = 0;
        batch_cnt = 0;
        nq_stat.resize(NQ_Histogram_Slices, 0);
        filter_stat.resize(Filter_Histogram_Slices, 0);
    }

    /*
     * Get batch count of the queries (Level 1)
     * @retval: query batch count
     */
    size_t
    BatchCount() {
        return batch_cnt;
    }

    /*
     * Get the statistics of the nq (Level 1)
     * @retval: count nq 1, 2, 3~4, 5~8, 9~16,…, 1024~2048, larger than 2048 (13 slices)
     */
    const std::vector<size_t>&
    NQHistogram() {
        return nq_stat;
    }

    /*
     * Get query response per-second (Level 1)
     * @retval: Qps
     */
    double
    Qps() {
        // ms -> s
        return total_query_time ? (nq_cnt * 1000.0 / total_query_time) : 0.0;
    }

    /*
     * Get the statistics of the filter for each batch (Level 2)
     * @retval: count 0~5%, 5~10%, 10~15%, ...95~100%, 100% (21 slices)
     */
    const std::vector<size_t>&
    FilterHistograms() {
        return filter_stat;
    }

 public:
    std::string& index_type;
    size_t batch_cnt;
    size_t nq_cnt;
    double total_query_time;  // unit: ms
    std::vector<size_t> nq_stat;
    std::vector<size_t> filter_stat;
};
using StatisticsPtr = std::shared_ptr<Statistics>;

/*
 * class: HNSWStatistics
 */
class HNSWStatistics : public Statistics {
 public:
    explicit HNSWStatistics(std::string& idx_t)
        : Statistics(idx_t), distribution(), target_level(1), access_total(0), ef_sum(0) {
    }

    ~HNSWStatistics() override = default;

    /*
     * To string (may be for log output)
     * @retval: string output
     */
    std::string
    ToString() override;

    /*
     * Clear all counts
     * @retval: none
     */
    void
    Clear() override {
        Statistics::Clear();
        access_total = 0;
        ef_sum = 0;
    }

    /*
     * Get nodes count in each level
     * @retval: none
     */
    const std::vector<size_t>&
    LevelNodesNum() {
        return distribution;
    }

    /*
     * Get average search parameter ‘ef’ (average for batches) (Level 1)
     * @retval: avg Ef
     */
    double
    AvgSearchEf() {
        return nq_cnt ? ef_sum / nq_cnt : 0;
    }

    /*
     * Cumulative distribution function of nodes access (Level 3)
     * @param: none (axis_x = {5,10,15,20,...100} by default)
     * @retval: Access CDF
     */
    virtual std::vector<double>
    AccessCDF() {
        std::vector<size_t> axis_x(20);
        for (size_t i = 0; i < 20; ++i) {
            axis_x[i] = (i + 1) * 5;
        }

        return AccessCDF(axis_x);
    }

    /*
     * Cumulative distribution function of nodes access
     * @param: axis_x[in] specified by users and should be in ascending order
     * @retval: Access CDF
     */
    virtual std::vector<double>
    AccessCDF(const std::vector<size_t>& axis_x) = 0;

 public:
    std::vector<size_t> distribution;
    size_t target_level;
    size_t access_total;
    size_t ef_sum;
};

/*
 * class: LibHNSWStatistics
 * for index: HNSW
 */
class LibHNSWStatistics : public HNSWStatistics {
 public:
    explicit LibHNSWStatistics(std::string& idx_t) : HNSWStatistics(idx_t), access_cnt_map() {
    }

    ~LibHNSWStatistics() override = default;

    void
    Clear() override {
        HNSWStatistics::Clear();
        access_cnt_map.clear();
    }

    std::vector<double>
    AccessCDF(const std::vector<size_t>& axis_x) override;

 public:
    std::unordered_map<int64_t, size_t> access_cnt_map;
    std::mutex hash_lock;
};

/*
 * class: RHNSWStatistics
 * for index: RHNSW_FLAT, RHNSE_SQ, RHNSW_PQ
 */
class RHNSWStatistics : public HNSWStatistics {
 public:
    explicit RHNSWStatistics(std::string& idx_t) : HNSWStatistics(idx_t), access_cnt() {
    }

    ~RHNSWStatistics() override = default;

    std::vector<double>
    AccessCDF(const std::vector<size_t>& axis_x) override;

 public:
    std::vector<size_t> access_cnt;
};

/*
 * class: IVFStatistics
 * for index: IVF_FLAT, IVF_PQ, IVF_SQ8
 */
class IVFStatistics : public Statistics {
 public:
    explicit IVFStatistics(std::string& idx_t)
        : Statistics(idx_t), nprobe_count(), access_cnt(), nlist(0) {
    }

    ~IVFStatistics() override = default;

    /*
     * To string (may be for log output)
     * @retval: string output
     */
    std::string
    ToString() override;

    /*
     * Clear all counts
     * @retval: none
     */
    void
    Clear() override {
        Statistics::Clear();
        nprobe_count.clear();
    }

    /*
     * Get the statistics of the search parameter nprboe (count of batches)  (Level 1)
     * @retval: nprobe
     */
    int64_t
    Nlist() {
        return nlist;
    }

    /*
     * Get the statistics of the search parameter nprboe (count of batches)  (Level 1)
     * @retval: <nprobe, count>
     */
    std::map<int64_t, size_t>
    SearchNprobe() {
        auto rst = nprobe_count;
        return rst;
    }

    /*
     * Cumulative distribution function of bucket access (Level 3)
     * @param: axis_x[in] specified by users and should be in ascending order
     * @retval: Access CDF
     */
    std::vector<double>
    AccessCDF(const std::vector<size_t>& axis_x);

 public:
    std::map<int64_t, size_t> nprobe_count;
    std::vector<size_t> access_cnt;
    size_t access_total;
    int64_t nlist;
};

}  // namespace knowhere
}  // namespace milvus
