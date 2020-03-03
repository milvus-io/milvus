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

#include <memory>
#include <sstream>
#include "Log.h"
#include "knowhere/common/Exception.h"

namespace knowhere {

enum class METRICTYPE {
    INVALID = 0,
    L2 = 1,
    IP = 2,
    HAMMING = 20,
    JACCARD = 21,
    TANIMOTO = 22,
};

// General Config
constexpr int64_t INVALID_VALUE = -1;
constexpr int64_t DEFAULT_K = INVALID_VALUE;
constexpr int64_t DEFAULT_DIM = INVALID_VALUE;
constexpr int64_t DEFAULT_GPUID = INVALID_VALUE;
constexpr METRICTYPE DEFAULT_TYPE = METRICTYPE::INVALID;

struct Cfg {
    METRICTYPE metric_type = DEFAULT_TYPE;
    int64_t k = DEFAULT_K;
    int64_t gpu_id = DEFAULT_GPUID;
    int64_t d = DEFAULT_DIM;

    Cfg(const int64_t& dim, const int64_t& k, const int64_t& gpu_id, METRICTYPE type)
        : metric_type(type), k(k), gpu_id(gpu_id), d(dim) {
    }

    Cfg() = default;

    virtual bool
    CheckValid() {
        if (metric_type == METRICTYPE::IP || metric_type == METRICTYPE::L2) {
            return true;
        }
        std::stringstream ss;
        ss << "MetricType: " << int(metric_type) << " not support!";
        KNOWHERE_THROW_MSG(ss.str());
        return false;
    }

    void
    Dump() {
        KNOWHERE_LOG_DEBUG << DumpImpl().str();
    }

    virtual std::stringstream
    DumpImpl() {
        std::stringstream ss;
        ss << "dim: " << d << ", metric: " << int(metric_type) << ", gpuid: " << gpu_id << ", k: " << k;
        return ss;
    }
};
using Config = std::shared_ptr<Cfg>;

}  // namespace knowhere
