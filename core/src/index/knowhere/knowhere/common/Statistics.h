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

#include <string>
#include <vector>
#include <memory>
#include <utility>
#include <stdio.h>
#include <algorithm>
#include <functional>
#include <unordered_map>
#include <src/index/knowhere/knowhere/index/IndexType.h>
#include "Log.h"

#include <faiss/IVFlib.h>
#include <faiss/IndexIVF.h>

namespace milvus {
namespace knowhere {


inline uint64_t upper_bound_of_pow2(uint64_t x) {
    --x;
    x |= (x >> 1);
    x |= (x >> 2);
    x |= (x >> 4);
    x |= (x >> 8);
    x |= (x >> 16);
    x |= (x >> 32);
    return x + 1;
}

inline int len_of_pow2(uint64_t x) {
    return __builtin_popcountl(x - 1);
}

class Statistics {
 public:
    explicit Statistics(std::string &idx_t):index_type(idx_t), filter_percentage_sum(0.0), nq_cnt(0), batch_cnt(0),
                                   total_query_time(0.0) { filter_cdf.resize(21, 0); nq_fd.resize(13, 0); }
    virtual ~Statistics() = default;
    virtual std::string ToString(const std::string &index_name) = 0;
    virtual void Clear() {
        filter_percentage_sum = total_query_time = 0.0;
        nq_cnt = batch_cnt = 0;
        filter_cdf.resize(21, 0);
        nq_fd.resize(13, 0);
    }
    std::string IndexType() { return index_type; }
    double Qps() { return nq_cnt ? total_query_time / nq_cnt : 0.0; }
    int64_t BatchCount() { return batch_cnt; }
    int64_t QueryCount() { return nq_cnt; }
    const std::vector<int>& FilterHistograms() { return filter_cdf; }
    const std::vector<int>& NqFD() { return nq_fd; }

    std::string &index_type;
    double filter_percentage_sum; // the sum of percentage of 1 in bitset before search
    int64_t nq_cnt;
    int64_t batch_cnt;
    double total_query_time;
    std::vector<int> filter_cdf;
    std::vector<int> nq_fd;
};
using StatisticsPtr = std::shared_ptr<Statistics>;

class HNSWStatistics : public Statistics {
 public:
    explicit HNSWStatistics(std::string &idx_t):Statistics(idx_t), max_level(0), access_total(0),
                                       target_level(1), ef_sum(0) {}

    void
    Clear() override {
        Statistics::Clear();
        access_total = 0;
        target_level = 1;
        max_level = 0;
        distribution.clear();
        access_cnt.clear();
        ef_sum = 0;
    }

    void GenSplitIdx(std::vector<int> &split_idx, size_t len) {
        for (auto i = 0; i < split_idx.size(); ++i) {
            split_idx[i] = (int)(((double)split_idx[i] / 100.0) * len);
        }
    }

    void
    CaculateStatistics(std::vector<double> &access_lorenz_curve, const std::vector<int> &stat_len) {
        std::vector<int64_t> cnts;
        access_total = 0;
        for (auto &elem : access_cnt) {
            cnts.push_back(elem.second);
            access_total += elem.second;
        }
        std::sort(cnts.begin(), cnts.end(), std::greater<int64_t>());
        size_t len = cnts.size();
        auto gini_len = 100;
//        std::vector<int> stat_len(gini_len, 0);
        int64_t tmp_cnt = 0;
        access_lorenz_curve.resize(gini_len);
        access_lorenz_curve[gini_len - 1] = 1.0;
        int j = 0;
        size_t i = 0;
        for (i = 1; i <= len && j < gini_len; ++i) {
            tmp_cnt += cnts[i];
            if (i >= stat_len[j]) {
                access_lorenz_curve[j] = (double)tmp_cnt / access_total + (j == 0 ? 0.0 : access_lorenz_curve[j - 1]);
                tmp_cnt = 0;
                j++;
            }
            if (j >= gini_len)
                break;
        }
    }

    std::string
    ToString(const std::string &index_name) override {
        std::ostringstream ret;
        ret << index_name << " Statistics:" << std::endl;
        if (STATISTICS_ENABLE == 0) {
            ret << "There is nothing because configuration STATISTICS_ENABLE = 0" << std::endl;
            return ret.str();
        }
        if (STATISTICS_ENABLE >= 1) {
            ret << "Total queries: " << nq_cnt << std::endl;
            ret << "The frequency distribution of the num of queries:" << std::endl;
            ret << "[1, 1].count = " << nq_fd[0] << std::endl;
            ret << "[2, 2].count = " << nq_fd[1] << std::endl;
            for (auto i = 2; i < 12; ++i)
                ret << "[" << ((1 << (i - 1)) | 1) << ", " << (1 << i) << "].count = " << nq_fd[i] << std::endl;
            ret << "[2048, +00).count = " << nq_fd[12] << std::endl;
            ret << "Total batches: " << batch_cnt << std::endl;
            ret << "Total ef: " << ef_sum << std::endl;
            ret << "Total query_time: " << total_query_time << " ms" << std::endl;
        }
        if (STATISTICS_ENABLE >= 2) {
            if (nq_cnt)
                ret << "The average percentage of filter in bitset: " << filter_percentage_sum * 100/ nq_cnt << "%" <<
                std::endl;
            else
                ret << "The average percentage of filter in bitset: 0%" << std::endl;
            ret << "The frequency distribution of filter: " << std::endl;
            for (auto i = 0; i < 20; ++i) {
                ret << "[" << i * 5 << "%, " << i * 5 + 5 << "%).count = " << filter_cdf[i] << std::endl;
            }
        }
        if (STATISTICS_ENABLE >= 3) {
            std::vector<double> access_lorenz_curve;
            std::vector<int> split_idx(100); // default log 101 idx
            for (auto i = 0; i < 100; ++i)
                split_idx[i] = i + 1;
            GenSplitIdx(split_idx, access_cnt.size());
            CaculateStatistics(access_lorenz_curve, split_idx);
            ret << "Max level: " << max_level << std::endl;
            ret << "Level distribution: " << std::endl;
            int64_t point_cnt = 0;
            for (auto i = max_level; i >= 0; --i) {
                point_cnt += distribution[i];
                ret << "Level " << i << " has " << point_cnt << " points" << std::endl;
            }
            ret << "There are " << access_total << " times point-access at level " << target_level << std::endl;
            ret << "The distribution of probability density at level " << target_level << ":" << std::endl;
            for (auto i = 0; i < access_lorenz_curve.size(); ++i) {
                ret << "(" << i << "," << access_lorenz_curve[i] << ")";
                if (i < access_lorenz_curve.size())
                    ret << " ";
                else
                    ret << std::endl;
            }
        }
        return ret.str();
    }

    const std::vector<int64_t>&
    LevelNodesNum() { return distribution; }

    double AvgSearchEf() { return nq_cnt ? ef_sum / nq_cnt : 0; }

    std::vector<double>
    AccessCDF() {
        std::vector<double> access_lorenz_curve;
        std::vector<int> split_idx(20, 0);
        for (auto i = 0; i < 20; ++i) {
            split_idx[i] = (i + 1) * 5;
        }
        GenSplitIdx(split_idx, access_cnt.size());
        CaculateStatistics(access_lorenz_curve, split_idx);
        return access_lorenz_curve;
    }

    std::vector<double>
    AccessCDF(const std::vector<int64_t> &axis_x) {
        std::vector<int> split_idx(axis_x.size(), 0);
        for (auto i = 0; i < axis_x.size(); ++i)
            split_idx[i] = (int)axis_x[i];
        GenSplitIdx(split_idx, access_cnt.size());
        std::vector<double> access_lorenz_curve;
        CaculateStatistics(access_lorenz_curve, split_idx);
        return access_lorenz_curve;
    }

    int max_level;
    std::vector<int64_t> distribution;
    std::unordered_map<unsigned int, uint64_t> access_cnt;
    int64_t access_total;
    int target_level;
    int64_t ef_sum;
};

class IVFStatistics : public Statistics {
 public:
    std::vector<double> cdf;
    std::vector<std::pair<int64_t, int64_t>> nprobe_count;
    int64_t nprobe_access_count;
    double total_quantizer_search_time;
    double total_data_search_time;

    explicit IVFStatistics(std::string &idx_t):Statistics(idx_t), total_quantizer_search_time(0),
    total_data_search_time(0), nprobe_access_count(0) {}

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
        for (int i=5; i <= 100; i += 5) {
            cdfresult.push_back(cdf[i]);
        }
        return cdfresult;
    }

    void
    CaculateStatistics(std::vector<int> nprobe_statistics) {
        nprobe_count.clear();
        for (int i=0; i < nprobe_statistics.size(); i++) {
            nprobe_count.push_back(std::pair<int64_t, int64_t>(i, nprobe_statistics[i]));
        }
        std::sort(nprobe_statistics.begin(), nprobe_statistics.end());
        std::reverse(nprobe_statistics.begin(), nprobe_statistics.end());

        int sum_id = 0;
        int total = 0;
        cdf.clear();
        for (int i=1; i <= 100; i++) {
            while (sum_id < int(nprobe_statistics.size()*0.01*i)) {
                total += nprobe_statistics[sum_id++];
            }
            cdf.push_back(total/(double)nprobe_access_count);
        }

        return;
    }

    void
    Clear() {
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

        if (STATISTICS_ENABLE == 0) {
            LOG_KNOWHERE_DEBUG_  << "There is nothing because configuration STATISTICS_ENABLE = 0";
        }
        if (STATISTICS_ENABLE >= 1) {
            LOG_KNOWHERE_DEBUG_  << "Total queries: " << nq_cnt;
            LOG_KNOWHERE_DEBUG_  << "The frequency distribution of the num of queries:";
            LOG_KNOWHERE_DEBUG_  << "[1, 1].count = " << nq_fd[0];
            LOG_KNOWHERE_DEBUG_  << "[2, 2].count = " << nq_fd[1];
            for (auto i = 2; i < 12; ++i)
                LOG_KNOWHERE_DEBUG_  << "[" << ((1 << (i - 1)) | 1) << ", " << (1 << i) << "].count = " << nq_fd[i];
            LOG_KNOWHERE_DEBUG_  << "[2048, +00).count = " << nq_fd[12];

            LOG_KNOWHERE_DEBUG_  << "Total batches: " << batch_cnt;

            LOG_KNOWHERE_DEBUG_  << "Total query_time: " << total_query_time << " ms";
            LOG_KNOWHERE_DEBUG_  << "Total quantizer search time: " << total_quantizer_search_time << " ms";
            LOG_KNOWHERE_DEBUG_  << "Total data search time: " << total_data_search_time << " ms";
            LOG_KNOWHERE_DEBUG_  << "The total number of nprobe access: " << nprobe_access_count;
        }
        if (STATISTICS_ENABLE >= 2) {
            if (nq_cnt)
                LOG_KNOWHERE_DEBUG_ << "The percentage of 1 in bitset: " << filter_percentage_sum * 100/ nq_cnt << "%";
            else
                LOG_KNOWHERE_DEBUG_ << "The percentage of 1 in bitset: 0%";
        }
        if (STATISTICS_ENABLE >= 3) {
            LOG_KNOWHERE_DEBUG_  << "Total queries: " << nq_cnt;
            for (int i = 0; i < cdf.size(); i++) {
                LOG_KNOWHERE_DEBUG_  << "Top " << i+1 << "% access count " << cdf[i];
            }
        }
    }

    std::string
    ToString(const std::string &index_name) override {
        std::ostringstream ret;
        ret << "IVFStatistics:";
        if (STATISTICS_ENABLE == 0) {
            ret << "There is nothing because configuration STATISTICS_ENABLE = 0" << std::endl;
        }
        if (STATISTICS_ENABLE >= 1) {
            ret << "Total queries: " << nq_cnt << std::endl;
            ret << "The frequency distribution of the num of queries:" << std::endl;
            ret << "[1, 1].count = " << nq_fd[0] << std::endl;
            ret << "[2, 2].count = " << nq_fd[1] << std::endl;
            for (auto i = 2; i < 12; ++i)
                ret << "[" << ((1 << (i - 1)) | 1) << ", " << (1 << i) << "].count = " << nq_fd[i] << std::endl;
            ret << "[2048, +00).count = " << nq_fd[12] << std::endl;

            ret << "Total batches: " << batch_cnt << std::endl;
            ret << "Total query_time: " << total_query_time << " ms" << std::endl;
            ret << "Total quantizer search time: " <<total_quantizer_search_time << " ms" << std::endl;
            ret << "Total data search time" << total_data_search_time << " ms" << std::endl;
            ret << "The total number of nprobe access: " <<nprobe_access_count;
        }
        if (STATISTICS_ENABLE >= 2) {
            if (nq_cnt)
                ret << "The percentage of 1 in bitset: " << filter_percentage_sum * 100/ nq_cnt << "%" << std::endl;
            else
                ret << "The percentage of 1 in bitset: 0%" << std::endl;
        }
        if (STATISTICS_ENABLE >= 3) {
            ret << "Total queries: " << nq_cnt << std::endl;
            for (int i = 0; i < cdf.size(); i++) {
                ret << "Top " << i+1 << "% access count " << cdf[i];
            }
        }
        return ret.str();
    }
};

class RHNSWStatistics : public Statistics {
 public:
    explicit RHNSWStatistics(std::string &idx_t):Statistics(idx_t), max_level(0), access_total(0),
                                        target_level(1), ef_sum(0) {}

    void
    Clear() override {
        Statistics::Clear();
        max_level = 0;
        distribution.clear();
        access_total = 0;
        target_level = 1;
        access_lorenz_curve.clear();
        ef_sum = 0;
    }

    std::string
    ToString(const std::string &index_name) override {
        std::ostringstream ret;
        ret << index_name << " Statistics:" << std::endl;
        if (STATISTICS_ENABLE == 0) {
            ret << "There is nothing because configuration STATISTICS_ENABLE = 0" << std::endl;
            return ret.str();
        }
        if (STATISTICS_ENABLE >= 1) {
            ret << "Total queries: " << nq_cnt << std::endl;
            ret << "The frequency distribution of the num of queries:" << std::endl;
            ret << "[1, 1].count = " << nq_fd[0] << std::endl;
            ret << "[2, 2].count = " << nq_fd[1] << std::endl;
            for (auto i = 2; i < 12; ++i)
                ret << "[" << ((1 << (i - 1)) | 1) << ", " << (1 << i) << "].count = " << nq_fd[i] << std::endl;
            ret << "[2048, +00).count = " << nq_fd[12] << std::endl;
            ret << "Total batches: " << batch_cnt << std::endl;
            ret << "Total ef: " << ef_sum << std::endl;
            ret << "Total query_time: " << total_query_time << " ms" << std::endl;
        }
        if (STATISTICS_ENABLE >= 2) {
            if (nq_cnt)
                ret << "The percentage of 1 in bitset: " << filter_percentage_sum * 100/ nq_cnt << "%" << std::endl;
            else
                ret << "The percentage of 1 in bitset: 0%" << std::endl;
            ret << "The frequency distribution of filter: " << std::endl;
            for (auto i = 0; i < 20; ++i) {
                ret << "[" << i * 5 << "%, " << i * 5 + 5 << "%).count = " << filter_cdf[i] << std::endl;
            }
        }
        if (STATISTICS_ENABLE >= 3) {
            ret << "Max level: " << max_level << std::endl;
            ret << "Level distribution: " << std::endl;
            int64_t point_cnt = 0;
            for (auto i = max_level; i >= 0; --i) {
                point_cnt += distribution[i];
                ret << "Level " << i << " has " << point_cnt << " points" << std::endl;
            }
            ret << "There are " << access_total << " times point-access at level " << target_level << std::endl;
            ret << "The distribution of probability density at level " << target_level << ":" << std::endl;
            if (access_lorenz_curve.empty())
                access_lorenz_curve.resize(101, 0.0);
            for (auto i = 0; i < access_lorenz_curve.size(); ++i) {
                ret << "(" << i << "," << access_lorenz_curve[i] << ")";
                if (i < access_lorenz_curve.size())
                    ret << " ";
                else
                    ret << std::endl;
            }
        }
        return ret.str();
    }

    const std::vector<int64_t>&
    LevelNodesNum() { return distribution; }

    double AvgSearchEf() { return nq_cnt ? ef_sum / nq_cnt : 0; }

    std::vector<double>
    AccessCDF() {
        std::vector<double> ret(20, 0.0);
        for (auto i = 0; i < 20; ++i) {
            ret[i] = access_lorenz_curve[i * 5 + 4];
        }
        return ret;
    }

    std::vector<double>
    AccessCDF(const std::vector<int64_t> &axis_x) {
        std::vector<double> ret(axis_x.size());
        int j = 0;
        for (auto i = 0; i < axis_x.size(); ++j) {
            if (j == (int)axis_x[i]) {
                ret.push_back(access_lorenz_curve[j]);
                i++;
            }
        }
        return ret;
    }

    int max_level;
    std::vector<int64_t> distribution;
    std::vector<double> access_lorenz_curve;
    int64_t access_total;
    int target_level;
    int64_t ef_sum;
};

}  // namespace knowhere
}  // namespace milvus
