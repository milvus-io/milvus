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

#include <shared_mutex>

namespace milvus::segcore {
class SingletonConfig {
 public:
    SingletonConfig() = default;
    SingletonConfig(const SingletonConfig&) = delete;
    SingletonConfig
    operator=(const SingletonConfig&) = delete;

 public:
    static SingletonConfig&
    GetInstance() {
        // thread-safe enough after c++ 11
        static SingletonConfig instance;
        return instance;
    }

 public:
    bool
    is_enable_parallel_reduce() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return enable_parallel_reduce_;
    }

    void
    set_enable_parallel_reduce(bool flag) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        enable_parallel_reduce_ = flag;
    }

    int64_t
    get_nq_threshold_to_enable_parallel_reduce() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return nq_threshold_to_enable_parallel_reduce_;
    }

    void
    set_nq_threshold_to_enable_parallel_reduce(int64_t nq) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        nq_threshold_to_enable_parallel_reduce_ = nq;
    }

    int64_t
    get_k_threshold_to_enable_parallel_reduce() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return k_threshold_to_enable_parallel_reduce_;
    }

    void
    set_k_threshold_to_enable_parallel_reduce(int64_t k) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        k_threshold_to_enable_parallel_reduce_ = k;
    }

 private:
    mutable std::shared_mutex mutex_;
    bool enable_parallel_reduce_ = true;
    int64_t nq_threshold_to_enable_parallel_reduce_ = 100;
    int64_t k_threshold_to_enable_parallel_reduce_ = 1000;
};
}  // namespace milvus::segcore
