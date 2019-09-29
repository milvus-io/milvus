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

#pragma once

#include <memory>

namespace knowhere {

enum class METRICTYPE {
    INVALID = 0,
    L2 = 1,
    IP = 2,
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
        return true;
    }
};
using Config = std::shared_ptr<Cfg>;

}  // namespace knowhere
