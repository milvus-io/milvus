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

#include "storage/LoadOverheadGroup.h"

#include <algorithm>
#include <limits>

#include "cachinglayer/Manager.h"
#include "common/EasyAssert.h"
#include "log/Log.h"

namespace milvus::storage {

namespace {

int64_t
PolicyBytes(size_t bytes) {
    return static_cast<int64_t>(std::min(
        bytes, static_cast<size_t>(std::numeric_limits<int64_t>::max())));
}

bool
UpdateGroup(const cachinglayer::LoadingOverheadGroupHandle& group,
            const cachinglayer::LoadingOverheadPolicy& policy,
            const char* name) {
    if (group == nullptr) {
        return true;
    }
    auto result =
        cachinglayer::Manager::UpdateLoadingOverheadGroup(group, policy);
    if (result != cachinglayer::LoadingOverheadUpdateResult::kApplied) {
        LOG_ERROR("Failed to update {} loading overhead group", name);
        return false;
    }
    return true;
}

}  // namespace

LoadMemoryOverheadGroup&
LoadMemoryOverheadGroup::GetInstance() {
    static LoadMemoryOverheadGroup instance;
    return instance;
}

cachinglayer::LoadingOverheadGroupHandle
LoadMemoryOverheadGroup::GetOrCreate(int64_t executor_workers) {
    std::lock_guard<std::mutex> lock(mutex_);
    AssertInfo(executor_workers >= 0,
               "Load memory executor workers must be non-negative");
    if (!executor_workers_initialized_) {
        executor_workers_ = executor_workers;
        executor_workers_initialized_ = true;
    }
    if (group_ == nullptr) {
        auto policy = budget_bytes_ == 0
                          ? cachinglayer::LoadingOverheadPolicy::Executor(
                                executor_workers_)
                          : cachinglayer::LoadingOverheadPolicy::Budget(
                                PolicyBytes(budget_bytes_));
        group_ = cachinglayer::Manager::CreateLoadingOverheadGroup(
            cachinglayer::LoadingOverheadDimension::kMemory, policy);
        AssertInfo(group_ != nullptr,
                   "Failed to create load memory overhead group");
    }
    return group_;
}

bool
LoadMemoryOverheadGroup::UpdateBudgetBytes(size_t bytes) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (bytes == budget_bytes_) {
        return true;
    }
    auto policy =
        bytes == 0
            ? cachinglayer::LoadingOverheadPolicy::Executor(executor_workers_)
            : cachinglayer::LoadingOverheadPolicy::Budget(PolicyBytes(bytes));
    if (!UpdateGroup(group_, policy, "load memory")) {
        return false;
    }
    budget_bytes_ = bytes;
    return true;
}

bool
LoadMemoryOverheadGroup::UpdateExecutorWorkers(int64_t executor_workers) {
    std::lock_guard<std::mutex> lock(mutex_);
    AssertInfo(executor_workers >= 0,
               "Load memory executor workers must be non-negative");
    if (executor_workers_initialized_ &&
        executor_workers == executor_workers_) {
        return true;
    }
    if (budget_bytes_ == 0 &&
        !UpdateGroup(
            group_,
            cachinglayer::LoadingOverheadPolicy::Executor(executor_workers),
            "load memory")) {
        return false;
    }
    executor_workers_ = executor_workers;
    executor_workers_initialized_ = true;
    return true;
}

LoadFileOverheadGroup&
LoadFileOverheadGroup::GetInstance() {
    static LoadFileOverheadGroup instance;
    return instance;
}

cachinglayer::LoadingOverheadGroupHandle
LoadFileOverheadGroup::GetOrCreate(int64_t executor_workers) {
    std::lock_guard<std::mutex> lock(mutex_);
    AssertInfo(executor_workers >= 0,
               "Load file executor workers must be non-negative");
    if (!executor_workers_initialized_) {
        executor_workers_ = executor_workers;
        executor_workers_initialized_ = true;
    }
    if (group_ == nullptr) {
        group_ = cachinglayer::Manager::CreateLoadingOverheadGroup(
            cachinglayer::LoadingOverheadDimension::kFile,
            cachinglayer::LoadingOverheadPolicy::Executor(executor_workers_));
        AssertInfo(group_ != nullptr,
                   "Failed to create load file overhead group");
    }
    return group_;
}

bool
LoadFileOverheadGroup::UpdateExecutorWorkers(int64_t executor_workers) {
    std::lock_guard<std::mutex> lock(mutex_);
    AssertInfo(executor_workers >= 0,
               "Load file executor workers must be non-negative");
    if (executor_workers_initialized_ &&
        executor_workers == executor_workers_) {
        return true;
    }
    if (!UpdateGroup(
            group_,
            cachinglayer::LoadingOverheadPolicy::Executor(executor_workers),
            "load file")) {
        return false;
    }
    executor_workers_ = executor_workers;
    executor_workers_initialized_ = true;
    return true;
}

}  // namespace milvus::storage
