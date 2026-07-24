// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstddef>
#include <cstdint>
#include <mutex>

#include "cachinglayer/LoadingOverhead.h"

namespace milvus::storage {

class LoadMemoryOverheadGroup {
 public:
    static LoadMemoryOverheadGroup&
    GetInstance();

    cachinglayer::LoadingOverheadGroupHandle
    GetOrCreate(int64_t executor_workers);

    bool
    UpdateBudgetBytes(size_t bytes);

    bool
    UpdateExecutorWorkers(int64_t executor_workers);

 private:
    LoadMemoryOverheadGroup() = default;

    std::mutex mutex_;
    cachinglayer::LoadingOverheadGroupHandle group_;
    size_t budget_bytes_{0};
    int64_t executor_workers_{0};
    bool executor_workers_initialized_{false};
};

class LoadFileOverheadGroup {
 public:
    static LoadFileOverheadGroup&
    GetInstance();

    cachinglayer::LoadingOverheadGroupHandle
    GetOrCreate(int64_t executor_workers);

    bool
    UpdateExecutorWorkers(int64_t executor_workers);

 private:
    LoadFileOverheadGroup() = default;

    std::mutex mutex_;
    cachinglayer::LoadingOverheadGroupHandle group_;
    int64_t executor_workers_{0};
    bool executor_workers_initialized_{false};
};

}  // namespace milvus::storage
