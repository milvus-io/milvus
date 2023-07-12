// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

#include "segcore/SingletonConfig.h"

namespace milvus::test {
class SingletonConfigRestorer {
 public:
    SingletonConfigRestorer() {
        auto& cfg = milvus::segcore::SingletonConfig::GetInstance();
        enable_parallel_reduce_ = cfg.is_enable_parallel_reduce();
        nq_threshold_to_enable_parallel_reduce_ =
            cfg.get_nq_threshold_to_enable_parallel_reduce();
        k_threshold_to_enable_parallel_reduce_ =
            cfg.get_k_threshold_to_enable_parallel_reduce();
    }

    ~SingletonConfigRestorer() {
        auto& cfg = milvus::segcore::SingletonConfig::GetInstance();
        cfg.set_enable_parallel_reduce(enable_parallel_reduce_);
        cfg.set_nq_threshold_to_enable_parallel_reduce(
            nq_threshold_to_enable_parallel_reduce_);
        cfg.set_k_threshold_to_enable_parallel_reduce(
            k_threshold_to_enable_parallel_reduce_);
    }

 private:
    bool enable_parallel_reduce_;
    int64_t nq_threshold_to_enable_parallel_reduce_;
    int64_t k_threshold_to_enable_parallel_reduce_;
};
}  // namespace milvus::test
