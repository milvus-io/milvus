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

template <cachinglayer::LoadingOverheadDimension Dimension>
class LoadOverheadController {
 public:
    static LoadOverheadController&
    GetInstance();

    // Returns the singleton Group handle, creating it if needed.
    // initial_executor_workers bootstraps the worker count used by the
    // Executor policy only when the controller has not been initialized yet;
    // it is not a lookup key and every call returns the same Group. Use
    // UpdateExecutorWorkers() for subsequent worker-count changes.
    cachinglayer::LoadingOverheadGroupHandle
    GetOrCreate(int64_t initial_executor_workers);

    bool
    UpdateBudgetBytes(size_t bytes)
        requires(Dimension == cachinglayer::LoadingOverheadDimension::kMemory);

    bool
    UpdateExecutorWorkers(int64_t executor_workers);

 private:
    LoadOverheadController() = default;

    cachinglayer::LoadingOverheadPolicy
    CurrentPolicy() const;

    bool
    UsesExecutorPolicy() const;

    std::mutex mutex_;
    cachinglayer::LoadingOverheadGroupHandle group_handle_;
    size_t budget_bytes_{0};
    int64_t executor_workers_{0};
    bool executor_workers_initialized_{false};
};

extern template class LoadOverheadController<
    cachinglayer::LoadingOverheadDimension::kMemory>;
extern template class LoadOverheadController<
    cachinglayer::LoadingOverheadDimension::kFile>;

using LoadMemoryOverheadController =
    LoadOverheadController<cachinglayer::LoadingOverheadDimension::kMemory>;
using LoadFileOverheadController =
    LoadOverheadController<cachinglayer::LoadingOverheadDimension::kFile>;

}  // namespace milvus::storage
