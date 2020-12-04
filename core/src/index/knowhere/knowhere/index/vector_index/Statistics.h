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
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "knowhere/common/Log.h"
#include "knowhere/index/IndexType.h"

#include <faiss/IVFlib.h>
#include <faiss/IndexIVF.h>

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
        : index_type(idx_t)
        , nq_cnt(0)
        , batch_cnt(0)
        , total_query_time(0.0)
        , nq_stat(NQ_Histogram_Slices, 0)
        , filter_stat(Filter_Histogram_Slices, 0) {
    }

    virtual ~Statistics() = default;

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
    double total_query_time; // unit: ms
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
        : Statistics(idx_t)
        , distribution()
        , target_level(1)
        , access_total(0)
        , ef_sum(0) {
    }

    virtual ~HNSWStatistics() = default;

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
    virtual void
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
 * class: HNSWStatistics
 * for index: HNSW
 */
class LibHNSWStatistics : public HNSWStatistics {
 public:
    explicit LibHNSWStatistics(std::string& idx_t)
        : HNSWStatistics(idx_t)
        , access_cnt_map() {
    }

    virtual ~LibHNSWStatistics() = default;

    virtual void
    Clear() override {
        HNSWStatistics::Clear();
        access_cnt_map.clear();
    }

    std::vector<double>
    AccessCDF(const std::vector<size_t>& axis_x) override;

 public:
    std::unordered_map<int64_t, size_t> access_cnt_map;
};

/*
 * class: RHNSWStatistics
 * for index: RHNSW_FLAT, RHNSE_SQ, RHNSW_PQ
 */
class RHNSWStatistics : public HNSWStatistics {
 public:
    explicit RHNSWStatistics(std::string& idx_t)
        : HNSWStatistics(idx_t)
        , access_cnt() {
    }

    virtual ~RHNSWStatistics() = default;

    std::vector<double>
    AccessCDF(const std::vector<size_t>& axis_x) override;

 public:
    std::vector<size_t> access_cnt;
};


// todo
class IVFStatistics : public Statistics {
 public:
    std::vector<double> cdf;
    std::vector<std::pair<int64_t, int64_t>> nprobe_count;
    int64_t nprobe_access_count;
    double total_quantizer_search_time;
    double total_data_search_time;

    explicit IVFStatistics(std::string& idx_t)
        : Statistics(idx_t), total_quantizer_search_time(0), total_data_search_time(0), nprobe_access_count(0) {
    }

    std::vector<std::pair<int64_t, int64_t>>
    SearchNprobe() {
        return nprobe_count;
    }

    const std::vector<double>
    AccessCDF(const std::vector<int64_t>& axis_x) {
        std::vector<double> cdfresult;
        for (auto x : axis_x) {
            cdfresult.push_back(cdf[x]);
        }
        return cdfresult;
    }

    const std::vector<double>
    AccessCDF() {
        std::vector<double> cdfresult;
        for (int i = 5; i <= 100; i += 5) {
            cdfresult.push_back(cdf[i]);
        }
        return cdfresult;
    }

    void
    CaculateStatistics(std::vector<int> nprobe_statistics) {
        nprobe_count.clear();
        for (int i = 0; i < nprobe_statistics.size(); i++) {
            nprobe_count.push_back(std::pair<int64_t, int64_t>(i, nprobe_statistics[i]));
        }
        std::sort(nprobe_statistics.begin(), nprobe_statistics.end());
        std::reverse(nprobe_statistics.begin(), nprobe_statistics.end());

        int sum_id = 0;
        int total = 0;
        cdf.clear();
        for (int i = 1; i <= 100; i++) {
            while (sum_id < int(nprobe_statistics.size() * 0.01 * i)) {
                total += nprobe_statistics[sum_id++];
            }
            cdf.push_back(total / (double)nprobe_access_count);
        }

        return;
    }

    void
    Clear() override {
        Statistics::Clear();
        cdf.assign(cdf.size(), 0);
        nprobe_count.assign(nprobe_count.size(), std::pair<int, int>(0, 0));
        nprobe_access_count = 0;
        total_quantizer_search_time = 0;
        total_data_search_time = 0;
    }

    void
    show() {
        LOG_KNOWHERE_DEBUG_ << "IVFStatistics:";

        if (STATISTICS_LEVEL == 0) {
            LOG_KNOWHERE_DEBUG_ << "There is nothing because configuration STATISTICS_LEVEL = 0";
        }
        if (STATISTICS_LEVEL >= 1) {
            LOG_KNOWHERE_DEBUG_ << "Total queries: " << nq_cnt;
            LOG_KNOWHERE_DEBUG_ << "The frequency distribution of the num of queries:";
            LOG_KNOWHERE_DEBUG_ << "[1, 1].count = " << nq_fd[0];
            LOG_KNOWHERE_DEBUG_ << "[2, 2].count = " << nq_fd[1];
            for (auto i = 2; i < 12; ++i)
                LOG_KNOWHERE_DEBUG_ << "[" << ((1 << (i - 1)) | 1) << ", " << (1 << i) << "].count = " << nq_fd[i];
            LOG_KNOWHERE_DEBUG_ << "[2048, +00).count = " << nq_fd[12];

            LOG_KNOWHERE_DEBUG_ << "Total batches: " << batch_cnt;

            LOG_KNOWHERE_DEBUG_ << "Total query_time: " << total_query_time << " ms";
            LOG_KNOWHERE_DEBUG_ << "Total quantizer search time: " << total_quantizer_search_time << " ms";
            LOG_KNOWHERE_DEBUG_ << "Total data search time: " << total_data_search_time << " ms";
            LOG_KNOWHERE_DEBUG_ << "The total number of nprobe access: " << nprobe_access_count;
        }
        if (STATISTICS_LEVEL >= 2) {
//            if (nq_cnt)
//                LOG_KNOWHERE_DEBUG_ << "The percentage of 1 in bitset: " << filter_percentage_sum * 100 / nq_cnt << "%";
//            else
//                LOG_KNOWHERE_DEBUG_ << "The percentage of 1 in bitset: 0%";
        }
        if (STATISTICS_LEVEL >= 3) {
            LOG_KNOWHERE_DEBUG_ << "Total queries: " << nq_cnt;
            for (int i = 0; i < cdf.size(); i++) {
                LOG_KNOWHERE_DEBUG_ << "Top " << i + 1 << "% access count " << cdf[i];
            }
        }
    }

    std::string
    ToString(const std::string& index_name) override {
        std::ostringstream ret;
        ret << "IVFStatistics:";
        if (STATISTICS_LEVEL == 0) {
            ret << "There is nothing because configuration STATISTICS_LEVEL = 0" << std::endl;
        }
        if (STATISTICS_LEVEL >= 1) {
            ret << "Total queries: " << nq_cnt << std::endl;
            ret << "The frequency distribution of the num of queries:" << std::endl;
            ret << "[1, 1].count = " << nq_fd[0] << std::endl;
            ret << "[2, 2].count = " << nq_fd[1] << std::endl;
            for (auto i = 2; i < 12; ++i)
                ret << "[" << ((1 << (i - 1)) | 1) << ", " << (1 << i) << "].count = " << nq_fd[i] << std::endl;
            ret << "[2048, +00).count = " << nq_fd[12] << std::endl;

            ret << "Total batches: " << batch_cnt << std::endl;
            ret << "Total query_time: " << total_query_time << " ms" << std::endl;
            ret << "Total quantizer search time: " << total_quantizer_search_time << " ms" << std::endl;
            ret << "Total data search time" << total_data_search_time << " ms" << std::endl;
            ret << "The total number of nprobe access: " << nprobe_access_count;
        }
        if (STATISTICS_LEVEL >= 2) {
//            if (nq_cnt)
//                ret << "The percentage of 1 in bitset: " << filter_percentage_sum * 100 / nq_cnt << "%" << std::endl;
//            else
//                ret << "The percentage of 1 in bitset: 0%" << std::endl;
        }
        if (STATISTICS_LEVEL >= 3) {
            ret << "Total queries: " << nq_cnt << std::endl;
            for (int i = 0; i < cdf.size(); i++) {
                ret << "Top " << i + 1 << "% access count " << cdf[i];
            }
        }
        return ret.str();
    }
};

}  // namespace knowhere
}  // namespace milvus
