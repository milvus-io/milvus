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
#include <unordered_map>
#include "Log.h"

namespace milvus {
namespace knowhere {

class Statistics {
public:
    Statistics():bitset_percentage1_sum(0.0), nq_cnt(0) {}
    virtual ~Statistics() = default;
    double bitset_percentage1_sum; // the percentage of 1 in bitset before search
    int64_t nq_cnt;
    virtual std::string
    ToString(const std::string &index_name) = 0;
    virtual void
    Clear() = 0;
};
using StatisticsPtr = std::shared_ptr<Statistics>;

class HNSWStatistics : public Statistics {
public:
    HNSWStatistics():Statistics(), max_level(0) {}
    int max_level;
    std::vector<int> distribution;
    std::unordered_map<unsigned int, uint64_t> access_cnt;

    void
    Clear() override {
        bitset_percentage1_sum = 0.0;
        max_level = 0;
        distribution.clear();
        access_cnt.clear();
    }
    void
    show() {
        LOG_KNOWHERE_DEBUG_ << "HNSWStatistics:";
        LOG_KNOWHERE_DEBUG_ << "bs_percentage_static = " << bitset_percentage1_sum;
        LOG_KNOWHERE_DEBUG_ << "max level = " << max_level;
        LOG_KNOWHERE_DEBUG_ << "level distribution:";
        for (auto i = 0; i < distribution.size(); ++ i) {
            LOG_KNOWHERE_DEBUG_ << "level" << i << ": " << distribution[i];
        }
        if (access_cnt.size() > 0) {
            LOG_KNOWHERE_DEBUG_ << "point access cnt in level 1:";
            int cnt = 0;
            for (auto &rec : access_cnt) {
                LOG_KNOWHERE_DEBUG_ << "point id:" << rec.first << ", access cnt:" << rec.second;
                cnt ++;
                if (cnt >= 10)
                    break;
            }
        } else {
            LOG_KNOWHERE_DEBUG_ << "there is no access cnt records";
        }
    }

    void
    CaculateStatistics(std::vector<double> &access_lorenz_curve) {
        std::vector<int64_t> cnts;
        int64_t sum = 0;
        for (auto &elem : access_cnt) {
            cnts.push_back(elem.second);
            sum += elem.second;
        }
        std::sort(cnts.begin(), cnts.end(), std::greater<int64_t>());
        size_t len = cnts.size();
        // todo: hard code?
        std::vector<int> stat_len = {5, 10, 15, 20, 50};
        auto gini_len = stat_len.size();
        for (auto i = 0; i < gini_len; ++ i) {
            stat_len[i] = (int)(((double)stat_len[i] / 100.0) * len);
        }
        int64_t tmp_cnt = 0;
        access_lorenz_curve.resize(gini_len << 1);
        int j = 0;
        for (auto i = 0; i < len && j < gini_len; ++ i) {
            if (i > stat_len[j]) {
                access_lorenz_curve[j] = (double)tmp_cnt / sum;
                tmp_cnt = 0;
                j ++;
            }
            if (j >= gini_len)
                break;
            tmp_cnt += cnts[i];
        }
        tmp_cnt = 0;
        for (auto i = len - 1; i >= 0 && j < (gini_len << 1); -- i) {
            if (len - i > stat_len[j - gini_len]) {
                access_lorenz_curve[j] = (double)tmp_cnt / sum;
                tmp_cnt = 0;
                j ++;
            }
            if (j >= (gini_len << 1))
                break;
            tmp_cnt += cnts[i];
        }
    };

    std::string
    ToString(const std::string &index_name) override {
        std::vector<double> access_lorenz_curve;
        CaculateStatistics(access_lorenz_curve);
        std::ostringstream ret;
        ret << index_name << " Statistics:" << std::endl;
        ret << "Total queries: " << nq_cnt << std::endl;
        if (nq_cnt)
            ret << "The percentage of 1 in bitset: " << bitset_percentage1_sum * 100/ nq_cnt << "%" << std::endl;
        else
            ret << "The percentage of 1 in bitset: 0%" << std::endl;
        ret << "Max level: " << max_level << std::endl;
        ret << "Level distribution: " << std::endl;
        for (auto i = 0; i < max_level; ++ i) {
            ret << "Level " << i << " has " << distribution[i] << " points" << std::endl;
        }
        std::vector<int> stat_len = {5, 10, 15, 20, 50};
        ret << "The gini coefficient of access distribution at level 1:" << std::endl;
        for (auto i = 0; i < access_lorenz_curve.size() / 2; ++ i) {
            ret << "The top" << stat_len[i] << " point has " << access_lorenz_curve[i] << "% access counts" << std::endl;
        }
        for (auto i = access_lorenz_curve.size() / 2; i < access_lorenz_curve.size(); ++ i) {
            ret << "The last" << stat_len[i] << " point has " << access_lorenz_curve[i] << "% access counts" << std::endl;
        }
        return ret.str();
    }
};

class RHNSWStatistics : public Statistics {
public:
    RHNSWStatistics():Statistics(), max_level(0) {}
    int max_level;
    std::vector<int> distribution;
    std::unordered_map<unsigned int, uint64_t> access_cnt;
    std::vector<double> access_gini_coefficient;

    void
    Clear() override {
        bitset_percentage1_sum = 0.0;
        max_level = 0;
        distribution.clear();
        access_cnt.clear();
    }

    std::string
    ToString(const std::string &index_name) override {
        std::ostringstream ret;
        ret << index_name << " Statistics:" << std::endl;
        ret << "Total queries: " << nq_cnt << std::endl;
        if (nq_cnt)
            ret << "The percentage of 1 in bitset: " << bitset_percentage1_sum * 100/ nq_cnt << "%" << std::endl;
        else
            ret << "The percentage of 1 in bitset: 0%" << std::endl;
        ret << "Max level: " << max_level << std::endl;
        ret << "Level distribution: " << std::endl;
        for (auto i = 0; i < max_level; ++ i) {
            ret << "Level " << i << " has " << distribution[i] << " points" << std::endl;
        }
        std::vector<int> stat_len = {5, 10, 15, 20, 50};
        ret << "The gini coefficient of access distribution at level 1:" << std::endl;
        for (auto i = 0; i < access_gini_coefficient.size() / 2; ++ i) {
            ret << "The top" << stat_len[i] << " point has " << access_gini_coefficient[i] << "% access counts" << std::endl;
        }
        for (auto i = access_gini_coefficient.size() / 2; i < access_gini_coefficient.size(); ++ i) {
            ret << "The last" << stat_len[i] << " point has " << access_gini_coefficient[i] << "% access counts" << std::endl;
        }
        return ret.str();
    }
};


}  // namespace knowhere
}  // namespace milvus
