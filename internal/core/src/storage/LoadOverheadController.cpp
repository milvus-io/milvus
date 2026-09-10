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

#include "storage/LoadOverheadController.h"

#include <limits>

#include "cachinglayer/Manager.h"
#include "common/EasyAssert.h"
#include "log/Log.h"

namespace milvus::storage {

namespace {

bool
UpdateGroupPolicy(const cachinglayer::LoadingOverheadGroupHandle& group_handle,
                  const cachinglayer::LoadingOverheadPolicy& policy,
                  const char* resource_name) {
    if (group_handle == nullptr) {
        return true;
    }
    auto result =
        cachinglayer::Manager::UpdateLoadingOverheadGroup(group_handle, policy);
    if (result != cachinglayer::LoadingOverheadUpdateResult::kApplied) {
        LOG_ERROR("Failed to update load {} loading overhead group",
                  resource_name);
        return false;
    }
    return true;
}

template <cachinglayer::LoadingOverheadDimension Dimension>
constexpr const char*
ResourceName() {
    if constexpr (Dimension ==
                  cachinglayer::LoadingOverheadDimension::kMemory) {
        return "memory";
    }
    return "file";
}

}  // namespace

template <cachinglayer::LoadingOverheadDimension Dimension>
LoadOverheadController<Dimension>&
LoadOverheadController<Dimension>::GetInstance() {
    static LoadOverheadController instance;
    return instance;
}

template <cachinglayer::LoadingOverheadDimension Dimension>
cachinglayer::LoadingOverheadPolicy
LoadOverheadController<Dimension>::CurrentPolicy() const {
    if constexpr (Dimension ==
                  cachinglayer::LoadingOverheadDimension::kMemory) {
        if (budget_bytes_ != 0) {
            return cachinglayer::LoadingOverheadPolicy::Budget(
                static_cast<int64_t>(budget_bytes_));
        }
    }
    return cachinglayer::LoadingOverheadPolicy::Executor(executor_workers_);
}

template <cachinglayer::LoadingOverheadDimension Dimension>
bool
LoadOverheadController<Dimension>::UsesExecutorPolicy() const {
    if constexpr (Dimension ==
                  cachinglayer::LoadingOverheadDimension::kMemory) {
        return budget_bytes_ == 0;
    }
    return true;
}

template <cachinglayer::LoadingOverheadDimension Dimension>
cachinglayer::LoadingOverheadGroupHandle
LoadOverheadController<Dimension>::GetOrCreate(
    int64_t initial_executor_workers) {
    std::lock_guard<std::mutex> lock(mutex_);
    AssertInfo(initial_executor_workers >= 0,
               "Load {} executor workers must be non-negative",
               ResourceName<Dimension>());
    if (!executor_workers_initialized_) {
        executor_workers_ = initial_executor_workers;
        executor_workers_initialized_ = true;
    }
    if (group_handle_ == nullptr) {
        group_handle_ = cachinglayer::Manager::CreateLoadingOverheadGroup(
            Dimension, CurrentPolicy());
        AssertInfo(group_handle_ != nullptr,
                   "Failed to create load {} overhead group",
                   ResourceName<Dimension>());
    }
    return group_handle_;
}

template <cachinglayer::LoadingOverheadDimension Dimension>
bool
LoadOverheadController<Dimension>::UpdateBudgetBytes(size_t bytes)
    requires(Dimension == cachinglayer::LoadingOverheadDimension::kMemory)
{
    std::lock_guard<std::mutex> lock(mutex_);
    AssertInfo(
        bytes <= static_cast<size_t>(std::numeric_limits<int64_t>::max()),
        "Load memory budget bytes exceed the loading-overhead policy range");
    if (bytes == budget_bytes_) {
        return true;
    }
    auto policy =
        bytes == 0
            ? cachinglayer::LoadingOverheadPolicy::Executor(executor_workers_)
            : cachinglayer::LoadingOverheadPolicy::Budget(
                  static_cast<int64_t>(bytes));
    if (!UpdateGroupPolicy(group_handle_, policy, ResourceName<Dimension>())) {
        return false;
    }
    budget_bytes_ = bytes;
    return true;
}

template <cachinglayer::LoadingOverheadDimension Dimension>
bool
LoadOverheadController<Dimension>::UpdateExecutorWorkers(
    int64_t executor_workers) {
    std::lock_guard<std::mutex> lock(mutex_);
    AssertInfo(executor_workers >= 0,
               "Load {} executor workers must be non-negative",
               ResourceName<Dimension>());
    if (executor_workers_initialized_ &&
        executor_workers == executor_workers_) {
        return true;
    }
    if (UsesExecutorPolicy() &&
        !UpdateGroupPolicy(
            group_handle_,
            cachinglayer::LoadingOverheadPolicy::Executor(executor_workers),
            ResourceName<Dimension>())) {
        return false;
    }
    executor_workers_ = executor_workers;
    executor_workers_initialized_ = true;
    return true;
}

template class LoadOverheadController<
    cachinglayer::LoadingOverheadDimension::kMemory>;
template class LoadOverheadController<
    cachinglayer::LoadingOverheadDimension::kFile>;

}  // namespace milvus::storage
