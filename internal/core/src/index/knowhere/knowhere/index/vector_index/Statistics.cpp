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

#include <algorithm>
#include <cstdio>
#include <string>
#include <unordered_map>
#include <vector>

#include "IndexIVF.h"
#include "knowhere/common/Log.h"
#include "knowhere/index/vector_index/Statistics.h"

namespace milvus {
namespace knowhere {

int32_t STATISTICS_LEVEL = 0;

std::string
Statistics::ToString() {
    std::ostringstream ret;

    if (STATISTICS_LEVEL == 0) {
        ret << "There is nothing because configuration STATISTICS_LEVEL = 0" << std::endl;
        return ret.str();
    }
    if (STATISTICS_LEVEL >= 1) {
        ret << "Total batches: " << batch_cnt << std::endl;
        ret << "Total queries: " << nq_cnt << std::endl;
        ret << "Qps: " << Qps() << std::endl;

        ret << "The frequency distribution of the num of queries:" << std::endl;
        size_t left = 1, right = 1;
        for (size_t i = 0; i < NQ_Histogram_Slices - 1; i++) {
            ret << "[" << left << ", " << right << "].count = " << nq_stat[i] << std::endl;
            left = right + 1;
            right <<= 1;
        }
        ret << "[" << left << ", +00).count = " << nq_stat.back() << std::endl;
    }
    if (STATISTICS_LEVEL >= 2) {
        ret << "The frequency distribution of filter: " << std::endl;
        for (auto i = 0; i < 20; ++i) {
            ret << "[" << i * 5 << "%, " << i * 5 + 5 << "%).count = " << filter_stat[i] << std::endl;
        }
    }

    return ret.str();
}

std::string
HNSWStatistics::ToString() {
    std::ostringstream ret;

    if (STATISTICS_LEVEL >= 1) {
        ret << "Avg Ef: " << AvgSearchEf() << std::endl;
    }
    if (STATISTICS_LEVEL >= 3) {
        std::vector<size_t> axis_x = {5, 10, 20, 40};
        std::vector<double> access_cdf = AccessCDF(axis_x);
        ret << "There are " << access_total << " times point-access at level " << target_level << std::endl;
        ret << "The CDF at level " << target_level << ":" << std::endl;
        for (auto i = 0; i < axis_x.size(); ++i) {
            ret << "(" << axis_x[i] << "," << access_cdf[i] << ") ";
        }
        ret << std::endl;
        ret << "Level distribution: " << std::endl;
        size_t point_cnt = 0;
        for (int i = distribution.size() - 1; i >= 0; i--) {
            point_cnt += distribution[i];
            ret << "Level " << i << " has " << point_cnt << " points" << std::endl;
        }
    }

    return Statistics::ToString() + ret.str();
}

std::vector<size_t>
GenSplitIndex(size_t size, const std::vector<size_t>& axis_x) {
    // Gen split index
    std::vector<size_t> split_idx(axis_x.size());
    for (size_t i = 0; i < axis_x.size(); i++) {
        if (axis_x[i] >= 100) {
            // for safe, not to let idx be larger than size
            split_idx[i] = size;
        } else {
            split_idx[i] = (axis_x[i] * size + 50) / 100;
        }
    }
    return split_idx;
}

std::vector<double>
CaculateCDF(size_t access_total, const std::vector<size_t>& access_cnt, const std::vector<size_t>& axis_x) {
    auto split_idx = GenSplitIndex(access_cnt.size(), axis_x);

    // count cdf
    std::vector<double> access_cdf;
    access_cdf.resize(split_idx.size(), 0.0);

    size_t idx = 0;
    size_t tmp_cnt = 0;
    for (size_t i = 0; i < split_idx.size(); ++i) {
        if (i != 0 && split_idx[i] < split_idx[i - 1]) {
            // wrong split_idx
            // Todo: log output
            access_cdf[i] = 0;
        } else {
            while (idx < split_idx[i]) {
                tmp_cnt += access_cnt[idx];
                idx++;
            }
            access_cdf[i] = static_cast<double>(tmp_cnt) / static_cast<double>(access_total);
        }
    }

    return access_cdf;
}

std::vector<double>
LibHNSWStatistics::AccessCDF(const std::vector<size_t>& axis_x) {
    // copy from std::map to std::vector
    std::vector<size_t> access_cnt;
    access_cnt.reserve(access_cnt_map.size());
    access_total = 0;
    for (auto& elem : access_cnt_map) {
        access_cnt.push_back(elem.second);
        access_total += elem.second;
    }
    std::sort(access_cnt.begin(), access_cnt.end(), std::greater<>());

    return CaculateCDF(access_total, access_cnt, axis_x);
}

std::vector<double>
RHNSWStatistics::AccessCDF(const std::vector<size_t>& axis_x) {
    return CaculateCDF(access_total, access_cnt, axis_x);
}

std::string
IVFStatistics::ToString() {
    std::ostringstream ret;

    if (STATISTICS_LEVEL >= 1) {
        ret << "nlist " << Nlist() << std::endl;
        ret << "(nprobe, count): " << std::endl;
        auto nprobe = SearchNprobe();
        for (auto& it : nprobe) {
            ret << "(" << it.first << ", " << it.second << ") ";
        }
        ret << std::endl;
    }
    if (STATISTICS_LEVEL >= 3) {
        std::vector<size_t> axis_x = {5, 10, 20, 40};
        ret << "Bucket CDF " << std::endl;
        auto output = AccessCDF(axis_x);
        for (int i = 0; i < output.size(); i++) {
            ret << "Top " << axis_x[i] << "% access count " << output[i] << std::endl;
        }
        ret << std::endl;
    }
    return Statistics::ToString() + ret.str();
}

void
IVFStatistics::count_nprobe(const int64_t nprobe) {
    // nprobe count
    auto it = nprobe_count.find(nprobe);
    if (it == nprobe_count.end()) {
        nprobe_count[nprobe] = 1;
    } else {
        it->second++;
    }
}

void
IVFStatistics::update_ivf_access_stats(const std::vector<size_t>& nprobe_statistics) {
    nlist = nprobe_statistics.size();
    access_total = 0;
    access_cnt = nprobe_statistics;

    std::sort(access_cnt.begin(), access_cnt.end(), std::greater<>());
    // access total
    for (auto& cnt : access_cnt) {
        access_total += cnt;
    }
}

std::vector<double>
IVFStatistics::AccessCDF(const std::vector<size_t>& axis_x) {
    return CaculateCDF(access_total, access_cnt, axis_x);
}

}  // namespace knowhere
}  // namespace milvus
