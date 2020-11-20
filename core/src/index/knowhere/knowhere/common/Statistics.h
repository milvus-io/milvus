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
    Statistics():bs_percentage_static(-1.0), bs_percentage_dynamic(0.0) {}
    virtual ~Statistics() = default;
    double bs_percentage_static; // the percentage of 1 in bitset before search
    double bs_percentage_dynamic; // the percentage of 1 in search process
};
using StatisticsPtr = std::shared_ptr<Statistics>;

class HNSWStatistics : public Statistics {
public:
    HNSWStatistics():Statistics(), max_level(0) {}
    int max_level;
    std::vector<int> distribution;
    std::unordered_map<unsigned int, uint64_t> access_cnt;

    void
    clear() {
        bs_percentage_dynamic = 0.0;
        bs_percentage_static = 0.0;
        max_level = 0;
        distribution.clear();
        access_cnt.clear();
    }
    void
    show() {
        LOG_KNOWHERE_DEBUG_ << "HNSWStatistics:";
        LOG_KNOWHERE_DEBUG_ << "bs_percentage_static = " << bs_percentage_static;
        LOG_KNOWHERE_DEBUG_ << "bs_percentage_dynamic = " << bs_percentage_dynamic;
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
};

}  // namespace knowhere
}  // namespace milvus
